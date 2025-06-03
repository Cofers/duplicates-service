# mosaic.py
import hashlib
import logging
from typing import Dict, Any, Optional, List
import datetime
from dateutil.relativedelta import relativedelta

# Asumimos que get_settings y Storage están definidos como en tu código original
# y que Storage ahora usa un cliente Redis asíncrono (ej. aioredis)
from src.core.config import get_settings
from src.storage import Storage

logger = logging.getLogger(__name__)

class Mosaic:
    def __init__(self):
        self.storage = Storage()
        self.settings = get_settings()
        self.checksum_prefix = "duplicate_checksum_"
        
        # TTL para el hash de checksums exactos (ej. 7 o 15 días en segundos)
        self.exact_duplicate_ttl = getattr(self.settings, 'DUPLICATE_EXACT_TTL', 604800) # Default a 7 días (7*24*60*60)
        
        # TTL para el hash de conteos de patrones de concepto (ej. 1 año en segundos)
        self.pattern_history_ttl = getattr(self.settings, 'DUPLICATE_PATTERN_TTL', 31536000) # Default a 1 año
        
        # Lista de meses hacia atrás a consultar para los conteos de patrones
        self.pattern_months_lookback = getattr(self.settings, 'PATTERN_MONTHS_LOOKBACK', [1, 2, 3, 4, 5, 6])

    def _normalize_concept(self, concept: str) -> str:
        concept_str = str(concept) # Asegurar que es string
        concept_str = ' '.join(concept_str.split())
        concept_str = concept_str.lower()
        concept_str = ''.join(c for c in concept_str if c.isalnum() or c.isspace())
        return concept_str

    def _serialize_metadata(self, metadata: Any) -> str:
        if not metadata:
            return ''
        if isinstance(metadata, dict):
            values = [f"{k}{v}" for k, v in sorted(metadata.items()) if isinstance(v, str)] # Ordenar por clave para consistencia
        elif isinstance(metadata, list):
            values = []
            # Para listas de dicts, asegurar un orden consistente si es posible,
            # o serializar cada dict y luego ordenar las cadenas resultantes.
            # Aquí asumimos que el orden en la lista es significativo o que los dicts son simples.
            # Si los dicts tienen una clave primaria, se podría ordenar por ella.
            # Por simplicidad, mantenemos la serialización previa pero para dicts internos, sería bueno ordenar.
            temp_serialized_items = []
            for item in metadata:
                if isinstance(item, dict):
                    # Serializar cada dict interno de forma determinista
                    item_values = [f"{k}{v}" for k, v in sorted(item.items()) if isinstance(v, str)]
                    temp_serialized_items.append('|'.join(item_values))
                elif isinstance(item, str):
                    temp_serialized_items.append(item)
            values = sorted(temp_serialized_items) # Ordenar las representaciones de string de los items
        else:
            return ''
        return '|'.join(values) # Unir los valores finales


    def generate_checksum(self, transaction: Dict[str, Any]) -> str:
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
        """Genera la clave de Redis para el HASH de checksums exactos."""
        return f"{self.checksum_prefix}{company_id}:{bank}:{account_number}"

    def _get_concept_count_key(
        self, company_id: str, bank: str, account_number: str, normalized_concept: str
    ) -> str:
        """Genera la clave de Redis para el HASH de conteos mensuales de conceptos."""
        concept_key_part = "".join(normalized_concept.split()) # Sin espacios
        # Prefijo distinto para estas claves de conteo
        return f"concept_counts_:{self.checksum_prefix}{company_id}:{bank}:{account_number}:{concept_key_part}"

    async def exists_checksum(
        self, checksum: str, company_id: str, bank: str, account_number: str
    ) -> bool:
        redis_key = self._get_redis_key(company_id, bank, account_number)
        try:
            return bool(await self.storage.client.hexists(redis_key, checksum))
        except Exception as e:
            logger.error(f"Error verificando checksum para clave {redis_key}, checksum {checksum}: {str(e)}", exc_info=True)
            return False

    async def get_original_checksum(
        self, checksum: str, company_id: str, bank: str, account_number: str
    ) -> Optional[str]:
        redis_key = self._get_redis_key(company_id, bank, account_number)
        try:
            return await self.storage.client.hget(redis_key, checksum)
        except Exception as e:
            logger.error(f"Error obteniendo checksum original para clave {redis_key}, checksum {checksum}: {str(e)}", exc_info=True)
            return None

    async def add_checksum(
        self,
        checksum: str,
        value_to_store: str,
        company_id: str,
        bank: str,
        account_number: str
    ) -> bool:
        redis_key = self._get_redis_key(company_id, bank, account_number)
        try:
            hset_result = await self.storage.client.hset(
                redis_key,
                checksum,
                value_to_store
            )
            operation_successful = isinstance(hset_result, int) # aioredis hset devuelve int (0 o 1)
            
            if operation_successful:
                await self.storage.client.expire(redis_key, self.exact_duplicate_ttl)
            else:
                logger.warning(f"HSET para {redis_key} (checksum: {checksum}) devolvió un resultado inesperado: {hset_result}, considerándolo fallido.")
            return operation_successful
        except Exception as e:
            logger.error(f"Error en add_checksum para la clave Redis '{redis_key}', checksum '{checksum}': {str(e)}", exc_info=True)
            return False

    def _get_year_month_str(self, date_str: Optional[str]) -> Optional[str]:
        if not date_str:
            logger.debug("Se recibió date_str vacío o None en _get_year_month_str.") # Cambiado a debug
            return None
        try:
            # Intentar parsear YYYY-MM-DD primero (común en APIs y BQ)
            dt_obj = datetime.datetime.strptime(str(date_str).split("T")[0], "%Y-%m-%d")
            return dt_obj.strftime("%Y-%m")
        except ValueError:
            try:
                # Intentar parsear DD-MM-YYYY como alternativa
                dt_obj = datetime.datetime.strptime(str(date_str), "%d-%m-%Y")
                return dt_obj.strftime("%Y-%m")
            except Exception:
                logger.warning(f"No se pudo parsear año-mes de date_str (formatos intentados YYYY-MM-DD, DD-MM-YYYY): '{date_str}'")
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

    async def increment_concept_monthly_count(
        self,
        normalized_concept: str,
        company_id: str,
        bank: str,
        account_number: str,
        transaction_date_str: str,
    ) -> bool:
        redis_key = self._get_concept_count_key(company_id, bank, account_number, normalized_concept)
        year_month_str = self._get_year_month_str(transaction_date_str)

        if not year_month_str:
            logger.error(f"No se puede incrementar conteo de concepto sin un año-mes válido para '{normalized_concept}' (fecha provista: '{transaction_date_str}').")
            return False
        
        try:
            await self.storage.client.hincrby(redis_key, year_month_str, 1)
            await self.storage.client.expire(redis_key, self.pattern_history_ttl)
            logger.debug(f"Conteo incrementado para concepto '{normalized_concept}', mes '{year_month_str}' en clave {redis_key}")
            return True
        except Exception as e:
            logger.error(f"Error incrementando conteo para concepto '{normalized_concept}' (clave: {redis_key}, mes: {year_month_str}): {str(e)}", exc_info=True)
            return False

    async def get_past_concept_monthly_counts(
        self,
        normalized_concept: str,
        company_id: str,
        bank: str,
        account_number: str,
        current_transaction_date_str: str,
        months_lookback: Optional[List[int]] = None
    ) -> Dict[str, int]:
        
        active_months_lookback = months_lookback if months_lookback is not None else self.pattern_months_lookback
        results: Dict[str, int] = {}
        
        if active_months_lookback:
            for i in active_months_lookback:
                results[f"count_{i}_month_ago"] = 0 # Inicializar con 0
        else: # Si no hay meses para revisar (lista vacía)
            return results 
            
        redis_key = self._get_concept_count_key(company_id, bank, account_number, normalized_concept)
        current_year_month_str = self._get_year_month_str(current_transaction_date_str)

        if not current_year_month_str:
            logger.warning(f"No se pueden obtener conteos de concepto sin un año-mes válido para transacción actual ('{current_transaction_date_str}'). Concepto: '{normalized_concept}'.")
            return results

        try:
            fields_to_get_map: Dict[str, str] = {} # Mapea "YYYY-MM" a la etiqueta "count_X_months_ago"
            for month_offset in active_months_lookback:
                past_month_str = self._get_previous_months_strs(current_year_month_str, month_offset)
                if past_month_str:
                    fields_to_get_map[past_month_str] = f"count_{month_offset}_month_ago"
            
            if not fields_to_get_map:
                return results

            # Obtener todos los conteos de los meses pasados relevantes con HMGET
            # HMGET devuelve una lista de valores (o None si el campo no existe) en el orden de los campos solicitados.
            field_list = list(fields_to_get_map.keys())
            counts_from_redis = await self.storage.client.hmget(redis_key, field_list)
            
            if counts_from_redis:
                for i, count_str in enumerate(counts_from_redis):
                    yyyymm_field = field_list[i]
                    label = fields_to_get_map[yyyymm_field]
                    if count_str is not None:
                        try:
                            results[label] = int(count_str)
                        except ValueError:
                            logger.warning(f"Valor no entero encontrado para el conteo de {label} (campo {yyyymm_field}) en {redis_key}: '{count_str}'")
                            results[label] = 0 # O manejar como error/default
                    # else: results[label] ya está en 0 por la inicialización
            return results
        except Exception as e:
            logger.error(f"Error obteniendo conteos de concepto para '{normalized_concept}' (clave: {redis_key}): {str(e)}", exc_info=True)
            return results


    async def process_transaction(
        self,
        transaction: Dict[str, Any]
    ) -> Dict[str, Any]:
        try:
            # Validar campos mandatorios al inicio
            company_id = transaction["company_id"]
            bank = transaction["bank"]
            account_number = transaction["account_number"]
            transaction_concept = transaction.get('concept', '') # Usar default si falta
            transaction_date_str = transaction.get("transaction_date")

            if not transaction_date_str:
                logger.error(f"Campo 'transaction_date' es requerido. Compañía: {company_id}, Cuenta: {account_number}.")
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
                value_to_store_as_original = transaction.get("checksum") # Identificador de la TX entrante
                if value_to_store_as_original is None or value_to_store_as_original == "": 
                    value_to_store_as_original = exact_checksum # Default al MD5 generado
                
                success_add_exact = await self.add_checksum(exact_checksum, value_to_store_as_original, company_id, bank, account_number)
                
                if not success_add_exact:
                    result["error"] = result["error"] or "Error adding exact_checksum to Redis"
                    logger.error(f"Fallo al llamar a add_checksum para {exact_checksum} (compañía {company_id}). El error específico debería estar logueado por add_checksum.")
                
                if success_add_exact:
                    logger.info(f"Checksum exacto {exact_checksum} añadido. Incrementando conteo de patrón de concepto.")
                    normalized_concept_for_adding = self._normalize_concept(transaction_concept)
                    count_incremented = await self.increment_concept_monthly_count(
                        normalized_concept_for_adding,
                        company_id, bank, account_number, transaction_date_str
                    )
                    if not count_incremented:
                        logger.warning(f"No se pudo incrementar el conteo del patrón de concepto para '{normalized_concept_for_adding}' (compañía {company_id}).")
                else:
                    logger.warning(f"No se intentará incrementar conteo de patrón para '{transaction_concept}' porque add_checksum falló.")

            # Obtener y adjuntar información de patrones de recurrencia (conteos)
            normalized_concept_for_checking = self._normalize_concept(transaction_concept)
            pattern_counts_info = await self.get_past_concept_monthly_counts(
                normalized_concept_for_checking,
                company_id, bank, account_number, transaction_date_str,
                self.pattern_months_lookback # Usa la lista de la instancia
            )
            
            has_recurring_pattern = any(count > 0 for count in pattern_counts_info.values())
            result["is_recurring_pattern"] = has_recurring_pattern
            result["pattern_details"] = pattern_counts_info # Ahora son conteos
            
            # Actualizar el mensaje en pattern_details si no hay patrón
            if not has_recurring_pattern and not result["error"]: # No sobrescribir si ya hay un error
                num_months_checked_str = f"{len(self.pattern_months_lookback)} periodos definidos" if isinstance(self.pattern_months_lookback, list) else "periodos configurados"
                result["pattern_details"] = f"No recurring pattern found for concept '{normalized_concept_for_checking}' in the {num_months_checked_str} checked."
            
            return result

        except KeyError as e:
            # Este error debería ser menos frecuente si usamos .get() para campos opcionales de la transacción
            logger.error(f"Campo mandatorio faltante en la transacción: {str(e)}. Datos de la transacción: {transaction}", exc_info=True)
            # Intenta generar un checksum si es posible para el resultado, aunque falten campos clave.
            partial_checksum = None
            try: partial_checksum = self.generate_checksum(transaction) # Podría fallar si 'concept' o 'amount' faltan
            except: pass
            return {
                "is_duplicate": False, "duplicate_type": None, "generated_checksum": partial_checksum,
                "conflicting_checksum": None, "is_recurring_pattern": False,
                "pattern_details": {}, "error": f"Campo mandatorio faltante en la transacción: {str(e)}"
            }
        except Exception as e:
            logger.error(f"Error general procesando la transacción: {str(e)}", exc_info=True)
            generated_checksum_on_error = None
            try: generated_checksum_on_error = self.generate_checksum(transaction)
            except Exception: pass
            return {
                "is_duplicate": False, "duplicate_type": None, "generated_checksum": generated_checksum_on_error,
                "conflicting_checksum": None, "is_recurring_pattern": False,
                "pattern_details": {}, "error": f"Error general procesando la transacción: {str(e)}"
            }