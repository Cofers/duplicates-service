# mosaic.py
import hashlib
import logging
from typing import Dict, Any, Optional # List ya no es necesaria para la funcionalidad principal

# Asumimos que get_settings y Storage están definidos como en tu código original
# y que Storage ahora usa un cliente Redis asíncrono (ej. aioredis)
from src.core.config import get_settings # Asegúrate que esta ruta sea correcta
from src.storage import Storage # Asegúrate que esta ruta sea correcta

logger = logging.getLogger(__name__)

class Mosaic:
    def __init__(self):
        self.storage = Storage()
        self.settings = get_settings()
        self.checksum_prefix = "duplicate_checksum_"
        
        # TTL para el hash de checksums exactos: 15 días en segundos
        # 15 días * 24 horas/día * 60 minutos/hora * 60 segundos/minuto = 1,296,000 segundos
        self.exact_duplicate_ttl = getattr(self.settings, 'DUPLICATE_EXACT_TTL', 1296000) # Default a 15 días

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
            # Ordenar por clave para consistencia
            values = [f"{k}{v}" for k, v in sorted(metadata.items()) if isinstance(v, (str, int, float, bool))]
        elif isinstance(metadata, list):
            values = []
            temp_serialized_items = []
            for item in metadata:
                if isinstance(item, dict):
                    item_values = [f"{k}{v}" for k, v in sorted(item.items()) if isinstance(v, (str, int, float, bool))]
                    temp_serialized_items.append('|'.join(item_values))
                elif isinstance(item, (str, int, float, bool)): # Aceptar también tipos primitivos en la lista
                    temp_serialized_items.append(str(item))
            values = sorted(temp_serialized_items) # Ordenar las representaciones de string de los items
        else:
            # Si no es ni dict ni list, intentar convertir a string directamente
            # Podrías añadir más lógica aquí si esperas otros tipos complejos
            try:
                return str(metadata)
            except:
                logger.warning(f"No se pudo serializar metadata de tipo {type(metadata)}")
                return ''
        return '|'.join(values) # Unir los valores finales


    def generate_checksum(self, transaction: Dict[str, Any]) -> str:
        concept_str = transaction.get('concept', '')
        amount_val = transaction.get('amount', 0) # Mantener como número para hashing consistente si es float
        
        concept = self._normalize_concept(concept_str)
        # Convertir el monto a una representación string canónica, ej. con 2 decimales para floats
        if isinstance(amount_val, float):
            amount = f"{amount_val:.2f}" # Asegurar formato consistente para floats
        else:
            amount = str(amount_val)
            
        metadata = self._serialize_metadata(transaction.get('metadata'))
        hash_input = f"{concept}{amount}{metadata}"
        return hashlib.md5(hash_input.encode()).hexdigest()

    def _get_redis_key(
        self, company_id: str, bank: str, account_number: str
    ) -> str:
        """Genera la clave de Redis para el HASH de checksums exactos."""
        return f"{self.checksum_prefix}{company_id}:{bank}:{account_number}"

    async def exists_checksum(
        self, checksum: str, company_id: str, bank: str, account_number: str
    ) -> bool:
        redis_key = self._get_redis_key(company_id, bank, account_number)
        try:
            return bool(await self.storage.client.hexists(redis_key, checksum))
        except Exception as e:
            logger.error(f"Error verificando checksum para clave {redis_key}, checksum {checksum}: {str(e)}", exc_info=True)
            return False

    async def get_original_checksum_value( # Renombrado para claridad, obtiene el valor asociado al checksum
        self, checksum_key_in_hash: str, company_id: str, bank: str, account_number: str
    ) -> Optional[str]:
        redis_key = self._get_redis_key(company_id, bank, account_number)
        try:
            # Este es el checksum de la transacción original (o su ID) que se guardó.
            return await self.storage.client.hget(redis_key, checksum_key_in_hash)
        except Exception as e:
            logger.error(f"Error obteniendo valor de checksum original para clave {redis_key}, checksum_key_in_hash {checksum_key_in_hash}: {str(e)}", exc_info=True)
            return None

    async def add_checksum(
        self,
        checksum_key_in_hash: str, # El checksum MD5 generado para la transacción actual
        value_to_store: str,      # El identificador de la transacción actual (puede ser su propio checksum o un ID externo)
        company_id: str,
        bank: str,
        account_number: str
    ) -> bool:
        redis_key = self._get_redis_key(company_id, bank, account_number)
        try:
            # Guardamos el checksum_key_in_hash como campo y value_to_store como su valor.
            # Si otra transacción genera el mismo checksum_key_in_hash, hexists lo detectará.
            # El value_to_store nos permite saber qué transacción original generó este checksum.
            hset_result = await self.storage.client.hset(
                redis_key,
                checksum_key_in_hash, # El MD5 de la transacción actual es la "clave" dentro del hash
                value_to_store        # El "valor" es el ID de la transacción actual (o su MD5 si no hay ID)
            )
            operation_successful = isinstance(hset_result, int) 
            
            if operation_successful:
                # Solo actualizamos el TTL si la operación HSET fue realmente una inserción o actualización.
                # Si HSET devuelve 0 (campo ya existía y valor fue actualizado), o 1 (nuevo campo), está bien.
                await self.storage.client.expire(redis_key, self.exact_duplicate_ttl)
            else:
                logger.warning(f"HSET para {redis_key} (checksum_key_in_hash: {checksum_key_in_hash}) devolvió un resultado inesperado: {hset_result}, considerándolo fallido.")
            return operation_successful
        except Exception as e:
            logger.error(f"Error en add_checksum para la clave Redis '{redis_key}', checksum_key_in_hash '{checksum_key_in_hash}': {str(e)}", exc_info=True)
            return False

    async def process_transaction(
        self,
        transaction: Dict[str, Any]
    ) -> Dict[str, Any]:
        try:
            company_id = transaction["company_id"]
            bank = transaction["bank"]
            account_number = transaction["account_number"]
            # transaction_date ya no es necesario para la lógica simplificada,
            # pero puede ser útil para logging o si se reintroduce alguna lógica temporal.
            # Lo mantenemos por si la transacción en sí lo requiere para el checksum.
            # transaction_concept = transaction.get('concept', '') # Usado en generate_checksum

            # Validar si 'transaction_date' es relevante para el checksum o metadata.
            # Si no, se puede omitir su verificación aquí.
            # Por ahora, la quitamos de la validación estricta a menos que sea parte del checksum implícitamente.

            # Este es el checksum MD5 generado para la transacción actual.
            current_transaction_checksum_md5 = self.generate_checksum(transaction)
            
            result = {
                "is_duplicate": False,
                "generated_checksum": current_transaction_checksum_md5, # Checksum de la TX actual
                "conflicting_checksum": None, # Checksum/ID de la TX original con la que choca
                "error": None
            }

            is_exact_duplicate = await self.exists_checksum(current_transaction_checksum_md5, company_id, bank, account_number)

            if is_exact_duplicate:
                result["is_duplicate"] = True
                # Obtenemos el valor que se almacenó originalmente para este MD5.
                # Este valor es el checksum (o ID) de la transacción que *ya está* en Redis.
                stored_original_tx_identifier = await self.get_original_checksum_value(current_transaction_checksum_md5, company_id, bank, account_number)
                result["conflicting_checksum"] = stored_original_tx_identifier
                logger.info(f"Transacción duplicada detectada. Checksum actual: {current_transaction_checksum_md5}, Checksum en Redis (original): {stored_original_tx_identifier}")

            else:
                # La transacción no es un duplicado (según el MD5). La añadimos.
                # El 'value_to_store' es un identificador de la transacción actual.
                # Puede ser un ID único de la transacción si lo tiene, o su propio checksum MD5.
                value_to_store_for_this_tx = transaction.get("checksum") # Asumimos que la TX puede tener un ID/checksum propio
                if not value_to_store_for_this_tx: 
                    value_to_store_for_this_tx = current_transaction_checksum_md5
                
                success_add_exact = await self.add_checksum(
                    current_transaction_checksum_md5, # El MD5 de esta transacción es la clave en el HASH
                    value_to_store_for_this_tx,       # El ID (o MD5) de esta transacción es el valor
                    company_id,
                    bank,
                    account_number
                )
                
                if not success_add_exact:
                    error_msg = "Error añadiendo checksum a Redis."
                    result["error"] = error_msg
                    logger.error(f"Fallo al llamar a add_checksum para {current_transaction_checksum_md5} (compañía {company_id}).")
                else:
                    logger.info(f"Nuevo checksum {current_transaction_checksum_md5} (valor: {value_to_store_for_this_tx}) añadido para {company_id}:{bank}:{account_number}.")
            
            return result

        except KeyError as e:
            logger.error(f"Campo mandatorio faltante en la transacción: {str(e)}. Datos de la transacción: {transaction}", exc_info=True)
            partial_checksum = None
            try: partial_checksum = self.generate_checksum(transaction)
            except: pass
            return {
                "is_duplicate": False, "generated_checksum": partial_checksum,
                "conflicting_checksum": None, "error": f"Campo mandatorio faltante en la transacción: {str(e)}"
            }
        except Exception as e:
            logger.error(f"Error general procesando la transacción: {str(e)}. Datos de la transacción: {transaction}", exc_info=True)
            generated_checksum_on_error = None
            try: generated_checksum_on_error = self.generate_checksum(transaction)
            except Exception: pass # Evitar error en cadena si la generación de checksum falla
            return {
                "is_duplicate": False, "generated_checksum": generated_checksum_on_error,
                "conflicting_checksum": None, "error": f"Error general procesando la transacción: {str(e)}"
            }