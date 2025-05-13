import hashlib
import logging
from typing import Dict, Any, Optional
from src.core.config import get_settings
from src.storage import Storage

logger = logging.getLogger(__name__)

class Mosaic:
    """
    Class that represents the checksum storage in Redis.
    Provides methods to generate, verify and store checksums.
    """
    def __init__(self):
        self.storage = Storage()
        self.settings = get_settings()
        self.checksum_prefix = "duplicate_checksum_"

    def _normalize_concept(self, concept: str) -> str:
        """
        Normalizes the concept text to remove special characters
        and extra spaces.
        """
        # Remove multiple spaces
        concept = ' '.join(concept.split())
        # Convert to lowercase
        concept = concept.lower()
        # Remove special characters
        concept = ''.join(c for c in concept if c.isalnum() or c.isspace())
        return concept

    def _serialize_metadata(self, metadata: Any) -> str:
        """
        Serializes the metadata field deterministically for the checksum.
        Concatenates all keys and string values found in the metadata.
        """
        if not metadata:
            return ''
            
        # If it's a dictionary, extract all keys and string values
        if isinstance(metadata, dict):
            values = []
            for k, v in metadata.items():
                if isinstance(v, str):
                    values.append(f"{k}{v}")
        # If it's a list, extract all keys and string values
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
            
        # Sort and concatenate all values
        return '|'.join(sorted(values))

    def generate_checksum(self, transaction: Dict[str, Any]) -> str:
        """
        Generates a checksum from the transaction fields.
        
        Args:
            transaction: Transaction to process
            
        Returns:
            str: Generated checksum
        """
        concept = self._normalize_concept(transaction['concept'])
        amount = str(transaction['amount'])
        metadata = self._serialize_metadata(transaction.get('metadata'))
        

        hash_input = f"{concept}{amount}{metadata}"
        
        return hashlib.md5(hash_input.encode()).hexdigest()

    def _get_redis_key(
        self, company_id: str, bank: str, account_number: str
    ) -> str:
        """
        Generates the Redis key for a set of checksums.
        """
        return f"{self.checksum_prefix}{company_id}:{bank}:{account_number}"

    async def exists_checksum(
        self,
        checksum: str,
        company_id: str,
        bank: str,
        account_number: str
    ) -> bool:
        """
        Verifies if a checksum exists in Redis.
        
        Args:
            checksum: Checksum to verify
            company_id: Company ID
            bank: Bank
            account_number: Account number
            
        Returns:
            bool: True if exists, False otherwise
        """
        try:
            redis_key = self._get_redis_key(company_id, bank, account_number)
            return bool(self.storage.client.hexists(redis_key, checksum))
        except Exception as e:
            logger.error(f"Error verifying checksum: {str(e)}")
            return False

    async def get_original_checksum(
        self,
        checksum: str,
        company_id: str,
        bank: str,
        account_number: str
    ) -> Optional[str]:
        """
        Gets the original checksum associated with a checksum.
        
        Args:
            checksum: Checksum to search
            company_id: Company ID
            bank: Bank
            account_number: Account number
            
        Returns:
            Optional[str]: Original checksum if exists, None otherwise
        """
        try:
            redis_key = self._get_redis_key(company_id, bank, account_number)
            return self.storage.client.hget(redis_key, checksum)
        except Exception as e:
            logger.error(f"Error getting original checksum: {str(e)}")
            return None

    async def add_checksum(
        self,
        checksum: str,
        original_checksum: str,
        company_id: str,
        bank: str,
        account_number: str,
        ttl: int = 1296000  # Default TTL of 15 days in seconds
    ) -> bool:
        """
        Adds a new checksum to Redis.
        
        Args:
            checksum: Checksum to add
            original_checksum: Original checksum
            company_id: Company ID
            bank: Bank
            account_number: Account number
            ttl: Time to live in seconds (default: 15 days)
            
        Returns:
            bool: True if added successfully, False otherwise
        """
        try:
            redis_key = self._get_redis_key(company_id, bank, account_number)
            # First add the checksum
            result = bool(
                self.storage.client.hset(
                    redis_key,
                    checksum,
                    original_checksum
                )
            )
            # Then set the TTL for the key
            if result:
                self.storage.client.expire(redis_key, ttl)
            return result
        except Exception as e:
            logger.error(f"Error adding checksum: {str(e)}")
            return False

    async def process_transaction(
        self,
        transaction: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Processes a transaction verifying if it's a duplicate.
        
        Args:
            transaction: Transaction to process
            
        Returns:
            Dict with the processing result
        """
        try:
            # Generate checksum
            checksum = self.generate_checksum(transaction)
            
            # Verify if exists
            exists = await self.exists_checksum(
                checksum,
                transaction["company_id"],
                transaction["bank"],
                transaction["account_number"]
            )
            
            if exists:
                # Get original checksum
                original_checksum = await self.get_original_checksum(
                    checksum,
                    transaction["company_id"],
                    transaction["bank"],
                    transaction["account_number"]
                )
                
                return {
                    "is_duplicate": True,
                    "checksum": transaction.get("checksum", ""),
                    "conflicting_checksum": original_checksum
                }
            
            # If doesn't exist, add it
            success = await self.add_checksum(
                checksum,
                transaction.get("checksum", ""),
                transaction["company_id"],
                transaction["bank"],
                transaction["account_number"]
            )
            
            if not success:
                return {
                    "is_duplicate": False,
                    "error": "Error adding checksum"
                }
            
            return {
                "is_duplicate": False,
                "checksum": checksum
            }
            
        except Exception as e:
            logger.error(f"Error processing transaction: {str(e)}")
            return {
                "is_duplicate": False,
                "error": str(e)
            } 