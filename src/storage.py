import redis
import os
import logging
from typing import List, Dict, Any, Tuple
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
    Manager class for Redis and BigQuery operations related to transaction checksums.
    """
    def __init__(self) -> None:
        """
        Initialize Redis and BigQuery connections.
        """
        try:
            settings = get_settings()
            
            # Redis setup
            self.client = redis.Redis(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                db=settings.REDIS_DB,
                decode_responses=True
            )
            self.key_prefix = "duplicate_checksum_"
            self.lock_prefix = "duplicate_lock_"
            
            # BigQuery setup
            self.bq_client = bigquery.Client(project=settings.GCP_PROJECT)
            self.dataset = settings.BIGQUERY_DATASET
            self.table = settings.BIGQUERY_TABLE
            
            # Test Redis connection
            self.client.ping()
            print(f"Connected to Redis: {settings.REDIS_HOST}:{settings.REDIS_PORT} DB:{settings.REDIS_DB}")
            
        except Exception as e:
            logging.error(f"Failed to initialize storage: {str(e)}")
            raise

    def _normalize_concept(self, concept: str) -> str:
        """
        Normaliza el texto del concepto para eliminar caracteres especiales
        y espacios extra.
        """
        # Eliminar espacios múltiples
        concept = ' '.join(concept.split())
        # Convertir a minúsculas
        concept = concept.lower()
        # Eliminar caracteres especiales
        concept = ''.join(c for c in concept if c.isalnum() or c.isspace())
        return concept

    def _serialize_metadata(self, metadata) -> str:
        """
        Serializa el campo metadata (array de dicts) de forma determinista
        para el checksum.
        """
        if not metadata:
            return ''
        # Ordenar por clave y concatenar key:value
        items = sorted(
            (m['key'], m['value'])
            for m in metadata if m.get('key') is not None
        )
        return '|'.join(
            f"{k}:{v}" for k, v in items
        )

    def _generate_checksum(self, transaction: Dict[str, Any]) -> str:
        """
        Generate a checksum from transaction fields: concept, amount, metadata.
        """
        concept = self._normalize_concept(transaction['concept'])
        amount = transaction['amount']
        metadata = self._serialize_metadata(transaction.get('metadata'))
        hash_input = f"{concept}{amount}{metadata}"
        return hashlib.md5(hash_input.encode()).hexdigest()

    def generate_redis_key(
        self, company_id: str, bank: str, account_number: str
    ) -> str:
        """
        Generate a Redis key for checksums.
        """
        return f"{self.key_prefix}{company_id}:{bank}:{account_number}"

    def get_lock_key(
        self, company_id: str, bank: str, account_number: str
    ) -> str:
        """
        Generate a lock key for a specific group of checksums.
        """
        return f"{self.lock_prefix}{company_id}:{bank}:{account_number}"

    def calculate_timeout(self, num_checksums: int) -> int:
        """
        Calculate appropriate timeout based on number of checksums.
        """
        return max(5, 5 + (num_checksums // 100))

    def acquire_lock(
        self, 
        company_id: str, 
        bank: str, 
        account_number: str, 
        num_checksums: int
    ) -> bool:
        """
        Try to acquire a distributed lock for processing checksums.
        """
        lock_key = self.get_lock_key(company_id, bank, account_number)
        lock_value = str(uuid.uuid4())
        timeout = self.calculate_timeout(num_checksums)
        
        acquired = self.client.set(
            lock_key, 
            lock_value,
            nx=True,
            ex=timeout
        )
        
        if acquired:
            self._current_lock = (lock_key, lock_value)
            logging.info(
                f"Acquired lock for {company_id}:{bank}:{account_number} "
                f"with timeout {timeout}s"
            )
            return True
        return False

    def release_lock(self) -> None:
        """
        Release the current lock if it exists and belongs to this instance.
        """
        if hasattr(self, '_current_lock'):
            lock_key, lock_value = self._current_lock
            script = """
            if redis.call('get', KEYS[1]) == ARGV[1] then
                return redis.call('del', KEYS[1])
            else
                return 0
            end
            """
            self.client.eval(script, 1, lock_key, lock_value)
            delattr(self, '_current_lock')

    async def process_checksum(
        self,
        company_id: str,
        bank: str,
        account_number: str,
        transaction: Dict[str, Any],
        max_retries: int = 3
    ) -> Tuple[bool, bool, str]:
        """
        Process a single checksum with distributed locking.

        Args:
            company_id: Company ID
            bank: Bank identifier
            account_number: Account number
            transaction: Transaction data to check for duplicates
            max_retries: Maximum number of retries

        Returns:
            Tuple of (success, is_duplicate, original_checksum)
        """
        # Log transaction data
        print("Transaction data received:")
        print(f"  concept: {transaction.get('concept')}")
        print(f"  amount: {transaction.get('amount')}")
        print(f"  metadata: {transaction.get('metadata')}")

        # Generate checksum from transaction data
        duplicate_checksum = self._generate_checksum(transaction)
        print(f"Generated checksum: {duplicate_checksum}")
        
        retries = 0
        while retries < max_retries:
            try:
                if not self.acquire_lock(
                    company_id, bank, account_number, 1
                ):
                    print(f"Could not acquire lock for {company_id}:{bank}:{account_number}, skipping")
                    return False, False, ""

                redis_key = self.generate_redis_key(
                    company_id, bank, account_number
                )
                
                # Log Redis key and search details
                print(f"Searching in Redis key: {redis_key}")
                print(f"Looking for checksum: {duplicate_checksum}")
                
                # Check if checksum exists and get its original checksum
                is_member = self.client.hexists(redis_key, duplicate_checksum)
                print(f"Checksum exists in Redis: {is_member}")
                
                if is_member:
                    # Si es duplicado, devolvemos el checksum original de la transacción duplicada
                    original = self.client.hget(redis_key, duplicate_checksum)
                    print(f"Found duplicate with original_checksum={original}")
                else:
                    # Si no es duplicado, usamos el checksum original de la transacción actual
                    original = duplicate_checksum
                    print(f"No duplicate found, using original_checksum={original}")
                
                # Log all checksums in the key for debugging
                all_checksums = self.client.hgetall(redis_key)
                print(f"All checksums in key {redis_key}:")
                for dup, orig in all_checksums.items():
                    print(f"  {dup} -> {orig}")
                
                return True, is_member, original

            except redis.WatchError:
                retries += 1
                if retries < max_retries:
                    logging.warning(
                        f"Concurrent modification detected for "
                        f"{company_id}:{bank}:{account_number}, "
                        f"retry {retries}/{max_retries}"
                    )
                    time.sleep(0.1 * retries)
                else:
                    logging.error(
                        f"Max retries reached for {company_id}:{bank}:"
                        f"{account_number}, skipping"
                    )
            finally:
                self.release_lock()
        
        return False, False, ""

    async def process_checksum_group(
        self,
        company_id: str,
        bank: str,
        account_number: str,
        checksum_pairs: List[Tuple[str, str]],
        max_retries: int = 3
    ) -> Tuple[bool, int]:
        """
        Process a group of checksums with distributed locking.

        Args:
            company_id: Company ID
            bank: Bank identifier
            account_number: Account number
            checksum_pairs: List of tuples (duplicate_checksum, original_checksum)
            max_retries: Maximum number of retries

        Returns:
            Tuple of (success, number_of_new_checksums)
        """
        retries = 0
        while retries < max_retries:
            try:
                if not self.acquire_lock(
                    company_id, bank, account_number, len(checksum_pairs)
                ):
                    logging.warning(
                        f"Could not acquire lock for {company_id}:{bank}:"
                        f"{account_number}, skipping"
                    )
                    return False, 0

                redis_key = self.generate_redis_key(
                    company_id, bank, account_number
                )
                
                # Use transaction to check and add checksums atomically
                pipe = self.client.pipeline()
                pipe.watch(redis_key)
                
                # Get existing checksums
                existing_checksums = self.client.hkeys(redis_key)
                missing_checksums = {
                    dup: orig for dup, orig in checksum_pairs 
                    if dup not in existing_checksums
                }
                
                if missing_checksums:
                    pipe.multi()
                    pipe.hmset(redis_key, missing_checksums)
                    pipe.expire(redis_key, 90 * 24 * 60 * 60)  # 90 days
                    pipe.execute()
                
                return True, len(missing_checksums)

            except redis.WatchError:
                retries += 1
                if retries < max_retries:
                    logging.warning(
                        f"Concurrent modification detected for "
                        f"{company_id}:{bank}:{account_number}, "
                        f"retry {retries}/{max_retries}"
                    )
                    time.sleep(0.1 * retries)
                else:
                    logging.error(
                        f"Max retries reached for {company_id}:{bank}:"
                        f"{account_number}, skipping"
                    )
            finally:
                self.release_lock()
        
        return False, 0

    

    