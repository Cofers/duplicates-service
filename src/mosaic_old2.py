import hashlib
import logging
from typing import Dict, Any, Optional, List
import datetime
from dateutil.relativedelta import relativedelta

from src.core.config import get_settings
from src.storage import Storage # Ahora Storage es asíncrono

logger = logging.getLogger(__name__)

class Mosaic:
    def __init__(self):
        self.storage = Storage()
        self.settings = get_settings()
        self.checksum_prefix = "duplicate_checksum_"
        
        self.exact_duplicate_ttl = getattr(self.settings, 'DUPLICATE_EXACT_TTL', 1296000)
        self.pattern_history_ttl = getattr(self.settings, 'DUPLICATE_PATTERN_TTL', 31536000)
        self.pattern_months_lookback = getattr(self.settings, 'PATTERN_MONTHS_LOOKBACK', [1, 2, 3, 4, 5, 6])

    def _normalize_concept(self, concept: str) -> str:
        concept = ' '.join(concept.split())
        concept = concept.lower()
        concept = ''.join(c for c in concept if c.isalnum() or c.isspace())
        return concept

    def _serialize_metadata(self, metadata: Any) -> str:
        if not metadata:
            return ''
        if isinstance(metadata, dict):
            values = [f"{k}{v}" for k, v in metadata.items() if isinstance(v, str)]
        elif isinstance(metadata, list):
            values = []
            for item in metadata:
                if isinstance(item, dict):
                    for k, v in item.items():
                        if isinstance(v, str):
                            values.append(f"{k}{v}")
                elif isinstance(item, str):
                    values.append(item)
        else:
            return ''
        return '|'.join(sorted(values))

    def generate_checksum(self, transaction: Dict[str, Any]) -> str:
        # Asegurarse de que los campos clave existen, o usar defaults
        concept_str = transaction.get('concept', '')
        amount_val = transaction.get('amount', 0)
        
        concept = self._normalize_concept(concept_str)
        amount = str(amount_val)
        metadata = self._serialize_metadata(transaction.get('metadata'))
        hash_input = f"{concept}{amount}{metadata}"
        return hashlib.md5(hash_input.encode()).hexdigest()

    def _get_redis_key(
        self, company_id: str, bank: str, account_number: str
    ) -> str:
        return f"{self.checksum_prefix}{company_id}:{bank}:{account_number}"

    async def exists_checksum(
        self, checksum: str, company_id: str, bank: str, account_number: str
    ) -> bool:
        try:
            redis_key = self._get_redis_key(company_id, bank, account_number)
            return bool(await self.storage.client.hexists(redis_key, checksum))
        except Exception as e:
            logger.error(f"Error verifying checksum for key {redis_key}, checksum {checksum}: {str(e)}", exc_info=True)
            return False

    async def get_original_checksum(
        self, checksum: str, company_id: str, bank: str, account_number: str
    ) -> Optional[str]:
        try:
            redis_key = self._get_redis_key(company_id, bank, account_number)
            return await self.storage.client.hget(redis_key, checksum)
        except Exception as e:
            logger.error(f"Error getting original checksum for key {redis_key}, checksum {checksum}: {str(e)}", exc_info=True)
            return None

    async def add_checksum(
        self,
        checksum: str,
        value_to_store: str,
        company_id: str,
        bank: str,
        account_number: str
    ) -> bool:
        redis_key = "" # Inicializar para el logging en caso de error temprano
        try:
            redis_key = self._get_redis_key(company_id, bank, account_number)
            hset_result = await self.storage.client.hset(
                redis_key,
                checksum,
                value_to_store
            )
            # Para aioredis, hset devuelve un entero (0 si el campo se actualizó, 1 si se creó).
            # Si no hay excepción, la operación fue exitosa.
            operation_successful = isinstance(hset_result, int) 
            
            if operation_successful:
                await self.storage.client.expire(redis_key, self.exact_duplicate_ttl)
            else:
                # Esto no debería ocurrir si hset no lanza una excepción y devuelve un entero.
                logger.warning(f"HSET para {redis_key} (checksum: {checksum}) devolvió un resultado inesperado: {hset_result}, considerándolo fallido.")
            return operation_successful
        except Exception as e:
            logger.error(f"Error en add_checksum para la clave Redis '{redis_key}', checksum '{checksum}': {str(e)}", exc_info=True)
            return False

    def _get_year_month_str(self, date_str: Optional[str]) -> Optional[str]: # date_str puede ser None
        if not date_str:
            logger.warning("Se recibió date_str vacío o None en _get_year_month_str.")
            return None
        try:
            dt_obj = datetime.datetime.strptime(date_str.split("T")[0], "%Y-%m-%d")
            return dt_obj.strftime("%Y-%m")
        except ValueError:
            try:
                dt_obj = datetime.datetime.strptime(date_str, "%d-%m-%Y")
                return dt_obj.strftime("%Y-%m")
            except Exception: # Evitar capturar una excepción y luego lanzar otra muy genérica
                logger.warning(f"No se pudo parsear año-mes de date_str (formatos intentados YYYY-MM-DD, dd-mm-yyyy): {date_str}")
                return None
        except Exception as e:
             logger.warning(f"Error inesperado parseando año-mes de date_str: '{date_str}'. Error: {e}", exc_info=True)
             return None

    def _get_previous_months_strs(self, current_year_month_str: str, num_months_back: int) -> Optional[str]:
        try:
            year, month = map(int, current_year_month_str.split('-'))
            current_date_obj = datetime.date(year, month, 1)
            target_date = current_date_obj - relativedelta(months=num_months_back)
            return target_date.strftime("%Y-%m")
        except Exception as e:
            logger.warning(f"No se pudo calcular el mes anterior para {current_year_month_str}, {num_months_back} meses atrás. Error: {e}", exc_info=True)
            return None

    def _get_concept_pattern_key(
        self, company_id: str, bank: str, account_number: str, normalized_concept: str
    ) -> str:
        concept_key_part = "".join(normalized_concept.split())
        return f"concept_history_:{self.checksum_prefix}{company_id}:{bank}:{account_number}:{concept_key_part}"

    async def check_concept_recurrence(
        self,
        normalized_concept: str,
        company_id: str,
        bank: str,
        account_number: str,
        current_transaction_date_str: str, # Este argumento es obligatorio
        months_lookback: Optional[List[int]] = None
    ) -> Dict[str, bool]:
        
        active_months_lookback = months_lookback if months_lookback is not None else self.pattern_months_lookback

        results = {}
        # Inicializar resultados a False por si current_year_month_str es None
        if active_months_lookback:
            for i in active_months_lookback:
                results[f"occurred_{i}_month(s)_ago"] = False
        
        redis_key = self._get_concept_pattern_key(company_id, bank, account_number, normalized_concept)
        current_year_month_str = self._get_year_month_str(current_transaction_date_str)

        if not current_year_month_str:
            logger.warning("No se puede verificar recurrencia de concepto sin un año-mes válido para transacción actual. Clave de patrón no consultada.")
            return results # Devuelve los defaults a False

        try:
            for month_offset in active_months_lookback:
                past_month_str = self._get_previous_months_strs(current_year_month_str, month_offset)
                label = f"occurred_{month_offset}_month(s)_ago"
                if past_month_str:
                    is_member = bool(await self.storage.client.sismember(redis_key, past_month_str))
                    results[label] = is_member
                # else: results[label] ya es False por la inicialización
            return results
        except Exception as e:
            logger.error(f"Error comprobando recurrencia de concepto para {normalized_concept} (clave: {redis_key}): {str(e)}", exc_info=True)
            # Al fallar, devolvemos los resultados inicializados a False
            return results


    async def add_concept_occurrence(
        self,
        normalized_concept: str,
        company_id: str,
        bank: str,
        account_number: str,
        transaction_date_str: str,
    ) -> bool:
        redis_key = self._get_concept_pattern_key(company_id, bank, account_number, normalized_concept)
        # Tus prints de depuración:
        print(f"DEBUG: add_concept_occurrence - Clave Redis Patrón: {redis_key}")
        year_month_str = self._get_year_month_str(transaction_date_str)
        print(f"DEBUG: add_concept_occurrence - Año-Mes: {year_month_str}")

        if not year_month_str:
            logger.error(f"No se puede añadir ocurrencia de concepto sin un año-mes válido para el concepto '{normalized_concept}'.")
            return False
        
        try:
            print(f"DEBUG: add_concept_occurrence - Añadiendo a Redis. TTL: {self.pattern_history_ttl}")
            # sadd devuelve el número de elementos añadidos (1 si era nuevo, 0 si ya existía).
            added_count = await self.storage.client.sadd(redis_key, year_month_str)
            print(f"DEBUG: add_concept_occurrence - Resultado de SADD (added_count): {added_count}")
            
            # Consideramos éxito si no hay excepción y added_count es >= 0.
            if isinstance(added_count, int) and added_count >= 0:
                await self.storage.client.expire(redis_key, self.pattern_history_ttl)
                return True
            else:
                logger.warning(f"SADD para {redis_key} devolvió un resultado inesperado: {added_count}, considerándolo fallido.")
                return False
        except Exception as e:
            logger.error(f"Error añadiendo ocurrencia de concepto para '{normalized_concept}' (clave: {redis_key}): {str(e)}", exc_info=True)
            return False

    async def process_transaction(
        self,
        transaction: Dict[str, Any]
    ) -> Dict[str, Any]:
        try:
            company_id = transaction["company_id"]
            bank = transaction["bank"]
            account_number = transaction["account_number"]
            transaction_date_str = transaction.get("transaction_date")

            if not transaction_date_str:
                logger.error(f"Campo 'transaction_date' es requerido y no se encontró en la transacción para la compañía {company_id}.")
                return {
                    "is_duplicate": False, "duplicate_type": None, "generated_checksum": None,
                    "conflicting_checksum": None, "is_recurring_pattern": False,
                    "pattern_details": {}, "error": "Campo 'transaction_date' es requerido."
                }

            exact_checksum = self.generate_checksum(transaction)
            result = {
                "is_duplicate": False, "duplicate_type": None, "generated_checksum": exact_checksum,
                "conflicting_checksum": None, "is_recurring_pattern": False, "pattern_details": {}, "error": None
            }

            is_exact_duplicate = await self.exists_checksum(exact_checksum, company_id, bank, account_number)

            if is_exact_duplicate:
                result["is_duplicate"] = True
                result["duplicate_type"] = "exact_match_recent"
                result["conflicting_checksum"] = await self.get_original_checksum(exact_checksum, company_id, bank, account_number)
            else:
                value_to_store_as_original = transaction.get("checksum")
                if value_to_store_as_original is None or value_to_store_as_original == "": 
                    value_to_store_as_original = exact_checksum
                
                success_add_exact = await self.add_checksum(exact_checksum, value_to_store_as_original, company_id, bank, account_number)
                
                if not success_add_exact:
                    result["error"] = result["error"] or "Error adding exact_checksum to Redis" # Preserva error anterior si existe
                    logger.error(f"Fallo al llamar a add_checksum para {exact_checksum} (compañía {company_id}). El error ya debería estar logueado por add_checksum.")
                
                if success_add_exact:
                    logger.info(f"Checksum exacto {exact_checksum} añadido con éxito. Procediendo a añadir patrón de concepto.")
                    normalized_concept_for_adding = self._normalize_concept(transaction.get('concept', ''))
                    pattern_added_successfully = await self.add_concept_occurrence(
                        normalized_concept_for_adding,
                        company_id, bank, account_number, transaction_date_str
                    )
                    if not pattern_added_successfully:
                        logger.warning(f"No se pudo añadir/actualizar el patrón de concepto para '{normalized_concept_for_adding}' (compañía {company_id}) después de añadir checksum exacto.")
                        # No marcamos un error principal por esto, pero se loguea.
                else:
                    logger.warning(f"No se intentará añadir patrón de concepto para '{transaction.get('concept', '')}' porque add_checksum falló.")


            # Verificación de Patrón de Recurrencia (siempre se hace si hay fecha)
            normalized_concept_for_checking = self._normalize_concept(transaction.get('concept', ''))
            pattern_info = await self.check_concept_recurrence(
                normalized_concept_for_checking,
                company_id, bank, account_number, transaction_date_str,
                self.pattern_months_lookback
            )
            
            has_recurring_pattern = any(pattern_info.values())
            result["is_recurring_pattern"] = has_recurring_pattern
            if has_recurring_pattern:
                result["pattern_details"] = pattern_info
            else:
                num_months_checked_str = f"{len(self.pattern_months_lookback)} periodos definidos" if isinstance(self.pattern_months_lookback, list) else "periodos configurados"
                result["pattern_details"] = f"No recurring pattern found for concept '{normalized_concept_for_checking}' in the {num_months_checked_str} checked."
            
            return result

        except KeyError as e:
            logger.error(f"Campo mandatorio faltante en la transacción: {str(e)}. Datos de la transacción: {transaction}", exc_info=True)
            return {
                "is_duplicate": False, "duplicate_type": None, "generated_checksum": None,
                "conflicting_checksum": None, "is_recurring_pattern": False,
                "pattern_details": {}, "error": f"Campo mandatorio faltante: {str(e)}"
            }
        except Exception as e:
            logger.error(f"Error general procesando la transacción: {str(e)}", exc_info=True)
            generated_checksum_on_error = None
            try:
                generated_checksum_on_error = self.generate_checksum(transaction)
            except Exception: pass # Evitar error en cadena si la transacción es muy mala
            return {
                "is_duplicate": False, "duplicate_type": None, "generated_checksum": generated_checksum_on_error,
                "conflicting_checksum": None, "is_recurring_pattern": False,
                "pattern_details": {}, "error": f"Error general procesando la transacción: {str(e)}"
            }