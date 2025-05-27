# storage.py

from redis import asyncio
import os
import logging
from typing import List, Dict, Any, Tuple, Optional # Añadido Optional
import time
import uuid
import hashlib
from google.cloud import bigquery
from src.core.config import get_settings

# Configurar el logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class Storage:
    """
    Clase gestora para operaciones con Redis (asíncrono) y BigQuery 
    relacionadas con checksums de transacciones.
    """
    def __init__(self) -> None:
        """
        Inicializa las configuraciones para Redis y BigQuery.
        La conexión real a Redis con aioredis se establece al primer comando.
        """
        try:
            self.settings = get_settings() # Movido aquí para que esté disponible para todos los métodos
            
            # Configuración de Redis (usando aioredis)
            # La conexión se establecerá cuando se ejecute el primer comando Redis.
            redis_url = f"redis://{self.settings.REDIS_HOST}:{self.settings.REDIS_PORT}/{self.settings.REDIS_DB}"
            self.client = asyncio.from_url(redis_url, decode_responses=True)
            
            self.key_prefix = "duplicate_checksum_"
            self.lock_prefix = "duplicate_lock_"
            self._current_lock: Optional[Tuple[str, str]] = None # Para tipado explícito
            
            # Configuración de BigQuery (sigue siendo síncrono, se gestionará en la capa que lo llame)
            self.bq_client = bigquery.Client(project=self.settings.GCP_PROJECT)
            self.dataset = self.settings.BIGQUERY_DATASET
            self.table = self.settings.BIGQUERY_TABLE
            
            logger.info(f"Storage inicializado. Cliente Redis (aioredis) configurado para: {redis_url}")
            logger.info(f"Cliente BigQuery configurado para proyecto: {self.settings.GCP_PROJECT}")
            
        except Exception as e:
            logger.error(f"Fallo al inicializar Storage: {str(e)}", exc_info=True)
            raise

    async def check_redis_connection(self) -> bool:
        """
        Verifica la conexión a Redis ejecutando un PING.
        Este es un método async que debe ser llamado con await.
        """
        try:
            await self.client.ping()
            logger.info(f"Conectado exitosamente a Redis: {self.settings.REDIS_HOST}:{self.settings.REDIS_PORT} DB:{self.settings.REDIS_DB}")
            return True
        except Exception as e:
            logger.error(f"Fallo al hacer ping a Redis: {str(e)}", exc_info=True)
            return False

    # --- Métodos de utilidad (síncronos, no cambian) ---
    def _normalize_concept(self, concept: str) -> str:
        concept = ' '.join(concept.split())
        concept = concept.lower()
        concept = ''.join(c for c in concept if c.isalnum() or c.isspace())
        return concept

    def _serialize_metadata(self, metadata: Optional[List[Dict[str, str]]]) -> str: # Ajustado tipo para metadata
        if not metadata:
            return ''
        items = sorted(
            (m['key'], m['value'])
            for m in metadata if isinstance(m, dict) and m.get('key') is not None and m.get('value') is not None
        )
        return '|'.join(
            f"{k}:{v}" for k, v in items
        )

    def _generate_checksum(self, transaction: Dict[str, Any]) -> str:
        concept = self._normalize_concept(transaction.get('concept', '')) # Default a string vacía si falta
        amount = str(transaction.get('amount', 0)) # Default a 0 si falta
        metadata_list = transaction.get('metadata') # metadata puede ser None
        metadata_str = self._serialize_metadata(metadata_list if isinstance(metadata_list, list) else None)
        hash_input = f"{concept}{amount}{metadata_str}"
        return hashlib.md5(hash_input.encode()).hexdigest()

    def generate_redis_key(self, company_id: str, bank: str, account_number: str) -> str:
        return f"{self.key_prefix}{company_id}:{bank}:{account_number}"

    def get_lock_key(self, company_id: str, bank: str, account_number: str) -> str:
        return f"{self.lock_prefix}{company_id}:{bank}:{account_number}"

    def calculate_timeout(self, num_checksums: int) -> int:
        return max(5, 5 + (num_checksums // 100))

    # --- Métodos de Redis (ahora asíncronos) ---
    async def acquire_lock(self, company_id: str, bank: str, account_number: str, num_checksums: int) -> bool:
        lock_key = self.get_lock_key(company_id, bank, account_number)
        lock_value = str(uuid.uuid4())
        timeout = self.calculate_timeout(num_checksums)
        
        acquired = await self.client.set(
            lock_key, 
            lock_value,
            nx=True, # set if not exist
            ex=timeout # expire in seconds
        )
        
        if acquired:
            self._current_lock = (lock_key, lock_value)
            logger.info(f"Lock adquirido para {company_id}:{bank}:{account_number} con timeout {timeout}s")
            return True
        logger.debug(f"No se pudo adquirir lock para {company_id}:{bank}:{account_number}")
        return False

    async def release_lock(self) -> None:
        if hasattr(self, '_current_lock') and self._current_lock is not None:
            lock_key, lock_value = self._current_lock
            # Script LUA para borrado atómico condicional
            script = """
            if redis.call('get', KEYS[1]) == ARGV[1] then
                return redis.call('del', KEYS[1])
            else
                return 0
            end
            """
            try:
                # Para aioredis, eval puede necesitar registrar el script primero o pasarlo directamente.
                # La forma más directa es client.eval(script_body, numkeys, key1, key2, arg1, arg2)
                await self.client.eval(script, 1, lock_key, lock_value)
                logger.info(f"Lock liberado para {lock_key}")
            except Exception as e:
                logger.error(f"Error liberando lock {lock_key}: {str(e)}", exc_info=True)
            finally:
                self._current_lock = None # Asegurarse que se limpia incluso si eval falla
                # No usar delattr(self, '_current_lock') ya que puede causar AttributeError si se llama de nuevo
        else:
            logger.debug("No hay lock que liberar o _current_lock no está definido.")


    async def process_checksum(self, company_id: str, bank: str, account_number: str,
                               transaction: Dict[str, Any], max_retries: int = 3) -> Tuple[bool, bool, str]:
        logger.info(f"Procesando checksum para {company_id}:{bank}:{account_number}")
        logger.debug(f"  Concepto: {transaction.get('concept')}, Monto: {transaction.get('amount')}")

        duplicate_checksum = self._generate_checksum(transaction)
        logger.info(f"Checksum generado: {duplicate_checksum}")
        
        retries = 0
        while retries < max_retries:
            try:
                if not await self.acquire_lock(company_id, bank, account_number, 1):
                    logger.warning(f"No se pudo adquirir lock para {company_id}:{bank}:{account_number}, saltando process_checksum")
                    return False, False, "" # Fallo en adquirir lock

                redis_key = self.generate_redis_key(company_id, bank, account_number)
                logger.info(f"Buscando en Redis (clave: {redis_key}) por checksum: {duplicate_checksum}")
                
                is_member = await self.client.hexists(redis_key, duplicate_checksum)
                logger.info(f"Checksum existe en Redis: {is_member}")
                
                original_value_in_redis = ""
                if is_member:
                    original_value_in_redis = await self.client.hget(redis_key, duplicate_checksum) or ""
                    logger.info(f"Duplicado encontrado. Valor original en Redis: {original_value_in_redis}")
                else:
                    # Si no es duplicado, el "original" desde la perspectiva de esta función es el que se añadiría.
                    # Sin embargo, esta función solo *consulta*, no añade.
                    # Devolvemos el checksum que se buscó si no se encontró, o string vacío si la semántica es "valor del original encontrado".
                    # Para ser consistentes, si no es miembro, no hay "original_checksum" almacenado.
                    logger.info(f"No es duplicado (checksum no encontrado en Redis).")
                
                if logger.isEnabledFor(logging.DEBUG): # Evitar hgetall si no es necesario
                    all_checksums = await self.client.hgetall(redis_key)
                    logger.debug(f"Todos los checksums en la clave {redis_key}: {all_checksums}")
                
                return True, is_member, original_value_in_redis # Éxito, es_duplicado, valor_original_en_redis

            except aioredis.exceptions.WatchError: # Específico de aioredis si se usa WATCH
                retries += 1
                logger.warning(
                    f"Modificación concurrente detectada para {company_id}:{bank}:{account_number} (WatchError), "
                    f"reintento {retries}/{max_retries}"
                )
                if retries < max_retries:
                    await asyncio.sleep(0.1 * retries) # Necesitas 'import asyncio'
                else:
                    logger.error(f"Máximo de reintentos alcanzado para {company_id}:{bank}:{account_number} debido a WatchError")
            except Exception as e: # Capturar otras excepciones de Redis o lógicas
                logger.error(f"Excepción en process_checksum para {company_id}:{bank}:{account_number}: {str(e)}", exc_info=True)
                break # Salir del bucle de reintentos en caso de error no relacionado con WatchError
            finally:
                await self.release_lock()
        
        return False, False, "" # Fallo después de reintentos o por otra excepción

    async def process_checksum_group(self, company_id: str, bank: str, account_number: str,
                                     checksum_pairs: List[Tuple[str, str]], max_retries: int = 3) -> Tuple[bool, int]:
        logger.info(f"Procesando grupo de {len(checksum_pairs)} checksums para {company_id}:{bank}:{account_number}")
        retries = 0
        while retries < max_retries:
            # Iniciar pipeline aquí para que WATCH esté al principio de la transacción lógica
            async with self.client.pipeline(transaction=True) as pipe: # transaction=True para MULTI/EXEC
                try:
                    if not await self.acquire_lock(company_id, bank, account_number, len(checksum_pairs)):
                        logger.warning(f"No se pudo adquirir lock para {company_id}:{bank}:{account_number}, saltando process_checksum_group")
                        return False, 0

                    redis_key = self.generate_redis_key(company_id, bank, account_number)
                    
                    # WATCH es implícito con transaction=True en aioredis para las claves usadas
                    # pero si necesitas WATCH explícito sobre una clave que se lee antes del MULTI:
                    await pipe.watch(redis_key) 
                    
                    # Obtener checksums existentes DENTRO de la transacción (antes del MULTI)
                    # hkeys devuelve una lista de campos.
                    existing_checksum_fields_list = await self.client.hkeys(redis_key) # Lectura fuera de MULTI/EXEC pero después de WATCH
                    existing_checksums = set(existing_checksum_fields_list)

                    missing_checksums_map: Dict[str, str] = {}
                    for dup_checksum, orig_checksum_val in checksum_pairs:
                        if dup_checksum not in existing_checksums:
                            missing_checksums_map[dup_checksum] = orig_checksum_val
                    
                    if missing_checksums_map:
                        # pipe.multi() es implícito al usar transaction=True y luego comandos
                        await pipe.hmset(redis_key, missing_checksums_map) # hmset está deprecado, pero aioredis puede manejarlo o usar hset con mapping
                        # Nota: Si hmset no está disponible o da problemas con tu versión de aioredis/Redis,
                        # podrías iterar y hacer múltiples pipe.hset(redis_key, campo, valor)
                        await pipe.expire(redis_key, 90 * 24 * 60 * 60)  # 90 días TTL
                        
                        # execute() se llama al salir del bloque 'async with' si transaction=True
                        # o puedes llamarlo explícitamente: await pipe.execute()
                        # Si WatchError ocurre, el bloque 'async with' debería desechar la transacción.
                        # El resultado de execute contendrá los resultados de los comandos encolados.
                        # No es necesario aquí si solo nos importa si la transacción se completó.
                        results = await pipe.execute() # Ejecutar la transacción
                        logger.info(f"Pipeline ejecutado para {redis_key}. Resultados: {results}")

                    else:
                        # Si no hay checksums faltantes, no es necesario ejecutar el pipeline,
                        # pero sí liberar el WATCH si se hizo explícito y no es manejado por `transaction=False`.
                        # Con `transaction=True`, el `EXEC` o `DISCARD` es automático.
                        # Simplemente no encolamos nada si `missing_checksums_map` está vacío.
                        # Si no hay nada que hacer, podemos ejecutar un comando no-op o nada.
                        # El pipeline se ejecutará vacío o se descartará.
                        # Para evitar una transacción vacía si `transaction=True` y no hay comandos:
                        if not missing_checksums_map:
                           await pipe.discard() # O simplemente no ejecutar, depende del cliente.
                                               # Con `async with` y `transaction=True`, no hacer nada en el `if`
                                               # resultará en una transacción vacía que se descarta o ejecuta sin comandos.

                    logger.info(f"{len(missing_checksums_map)} nuevos checksums añadidos a {redis_key}")
                    return True, len(missing_checksums_map)

                except aioredis.exceptions.WatchError: # Usar la excepción correcta de aioredis
                    retries += 1
                    logger.warning(
                        f"Modificación concurrente (WatchError) para {company_id}:{bank}:{account_number}, "
                        f"reintento {retries}/{max_retries}"
                    )
                    if retries >= max_retries:
                        logger.error(f"Máximo de reintentos por WatchError para {company_id}:{bank}:{account_number}")
                        # No es necesario liberar el lock aquí ya que se hace en finally
                    else:
                        await asyncio.sleep(0.1 * retries) # Necesitas 'import asyncio'
                except Exception as e:
                    logger.error(f"Excepción en process_checksum_group para {company_id}:{bank}:{account_number}: {str(e)}", exc_info=True)
                    break # Salir del bucle en caso de error no relacionado con WatchError
                finally:
                    # La liberación del lock debe ocurrir independientemente del éxito de la transacción
                    # pero solo si se adquirió. El acquire_lock está al principio del try.
                    await self.release_lock() 
            
        return False, 0 # Fallo después de reintentos o por otra excepción