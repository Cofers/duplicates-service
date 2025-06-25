# mosaic.py
import logging
from typing import Dict, Any, Optional, List
import math
import re
from redis import asyncio as redis_async_pkg
import json

# Asumimos que get_settings y Storage están definidos como en tu código original
# y que Storage ahora usa un cliente Redis asíncrono (ej. aioredis)
from src.core.config import get_settings  # Asegúrate que esta ruta sea correcta
from src.storage import Storage  # Asegúrate que esta ruta sea correcta

logger = logging.getLogger(__name__)


class Mosaic:
    def __init__(self, redis_client: redis_async_pkg.Redis = None):
        self.settings = get_settings()
        self.checksum_prefix = "duplicate_checksum_"
        
        # TTLs in seconds
        self.EXACT_DUPLICATE_TTL = 432000  # 5 days
        self.SIMILAR_DUPLICATE_TTL = 432000  # 5 days
        self.UPDATE_DUPLICATE_TTL = 432000  # 5 days
        
        # Use provided Redis client or create new one
        if redis_client:
            self.redis_client = redis_client
            # Create storage with the provided client
            self.storage = Storage()
            self.storage.client = redis_client
        else:
            # Fallback to creating new storage (for backward compatibility)
            self.storage = Storage()
            self.redis_client = self.storage.client

    def _format_field_for_key(self, value: Any) -> str:
        """Formats a field for use in a Redis key."""
        if value is None:
            return "null"
        if isinstance(value, (int, float)):
            # For numbers, we use a fixed precision
            return f"{value:.2f}"
        if isinstance(value, str):
            # For strings, we remove spaces and convert to lowercase
            return re.sub(r'\s+', '', value.lower())
        if isinstance(value, list):
            # For lists, we concatenate their formatted items
            return "_".join(self._format_field_for_key(item) for item in value)
        if isinstance(value, dict):
            # For dictionaries, we concatenate their formatted values
            return "_".join(self._format_field_for_key(v) for v in value.values())
        return str(value)

    def _serialize_metadata_for_key(
        self, metadata: List[Dict[str, str]]
    ) -> str:
        """Serializes metadata for use in a Redis key."""
        if not metadata or not isinstance(metadata, list):
            return "N/A"

        serialized_parts = []
        try:
            for item in metadata:
                if isinstance(item, dict) and 'key' in item and 'value' in item:
                    key_str = str(item['key'])
                    value_str = str(item['value'])
                    serialized_parts.append(key_str + value_str)
                else:
                    logger.warning(
                        f"Metadata item does not have the expected format "
                        f"{{'key':k, 'value':v}}: {item}. Using 'invalid_metadata_item'."
                    )
                    return "invalid_metadata_structure"

            if not serialized_parts:
                return "N/A"

            return "".join(serialized_parts)
        except Exception as e:
            logger.error(
                f"Error during simple metadata serialization "
                f"(type: {type(metadata)}): {e}",
                exc_info=True
            )
            return "serialization_error"

    def custom_is_na(self, value: Any) -> bool:
        """Checks if a value is null or empty."""
        if value is None:
            return True
        if isinstance(value, (str, list, dict)) and not value:
            return True
        if isinstance(value, (int, float)) and math.isnan(value):
            return True
        return False

    def _normalize_concept(self, concept: str) -> str:
        concept_str = str(concept)
        concept_str = concept_str.lower()
        concept_str = ''.join(
            c for c in concept_str if c.isalnum() or c.isspace()
        )
        concept_str = concept_str.replace(' ', '')
        return concept_str

    def _serialize_metadata(self, metadata: Any) -> str:
        if not metadata:
            return ''
        if isinstance(metadata, dict):
            # Sort by key for consistency
            items = sorted(metadata.items())
            values = [
                f"{k}{v}" for k, v in items
                if isinstance(v, (str, int, float, bool))
            ]
        elif isinstance(metadata, list):
            values = []
            temp_serialized_items = []
            for item in metadata:
                if isinstance(item, dict):
                    item_values = [
                        f"{k}{v}" for k, v in sorted(item.items())
                        if isinstance(v, (str, int, float, bool))
                    ]
                    temp_serialized_items.append('|'.join(item_values))
                elif isinstance(item, (str, int, float, bool)):
                    temp_serialized_items.append(str(item))
            values = sorted(temp_serialized_items)
        else:
            try:
                return str(metadata)
            except Exception:
                logger.warning(
                    f"Could not serialize metadata of type {type(metadata)}"
                )
                return ''
        return '|'.join(values)

    def generate_checksum(self, transaction: Dict[str, Any]) -> str:
        """Generates a checksum for the transaction."""
        # Validate that transaction is a dictionary
        if not isinstance(transaction, dict):
            raise ValueError(
                f"Transaction must be a dictionary, got {type(transaction)}"
            )
        
        try:
            # We extract the relevant fields using direct access like in 
            # transaction_update_detector
            logger.debug(
                f"Extracting fields from transaction: {transaction}"
            )
            company_id = transaction['company_id']
            logger.debug(
                f"company_id: {company_id} "
                f"(type: {type(company_id)})"
            )
            
            bank = transaction['bank']
            logger.debug(
                f"bank: {bank} "
                f"(type: {type(bank)})"
            )
            
            account_number = transaction['account_number']
            logger.debug(
                f"account_number: {account_number} "
                f"(type: {type(account_number)})"
            )
            
            concept = transaction.get('concept', '')
            logger.debug(
                f"concept: {concept} "
                f"(type: {type(concept)})"
            )
            
            amount = transaction['amount']
            logger.debug(
                f"amount: {amount} "
                f"(type: {type(amount)})"
            )
            
            metadata = transaction.get('metadata', [])
            logger.debug(
                f"metadata: {metadata} "
                f"(type: {type(metadata)})"
            )
            
            # Convert metadata to list if it's a dict (like in transaction_update_detector)
            if isinstance(metadata, dict):
                metadata = [metadata]
                logger.debug(f"Converted metadata to list: {metadata}")
            
            # We generate the Redis key
            redis_key = self._get_redis_key(
                company_id, bank, account_number, concept, amount, metadata
            )
            logger.debug(f"Generated Redis key: {redis_key}")
            
            # The checksum is the Redis key
            return redis_key
        except Exception as e:
            logger.error(f"Error in generate_checksum: {str(e)}")
            logger.error(f"Transaction data: {transaction}")
            raise

    def _get_redis_key(
        self, company_id: str, bank: str, account_number: str,
        concept: str, amount: float, metadata: List[Dict[str, str]]
    ) -> str:
        """Generates a unique Redis key based on the transaction fields."""
        # We format each field for the key
        formatted_company = self._format_field_for_key(company_id)
        formatted_bank = self._format_field_for_key(bank)
        formatted_account = self._format_field_for_key(account_number)
        formatted_concept = self._normalize_concept(concept)
        formatted_amount = self._format_field_for_key(amount)
        formatted_metadata = self._serialize_metadata_for_key(metadata)
        
        # We build the key
        return (
            f"duplicate_checksum_{formatted_company}:{formatted_bank}:"
            f"{formatted_account}:{formatted_concept}:{formatted_amount}:"
            f"{formatted_metadata}"
        )

    async def exists_checksum(
        self, redis_client: redis_async_pkg.Redis, transaction: Dict[str, Any]
    ) -> bool:
        """Checks if a checksum exists for the transaction."""
        checksum = self.generate_checksum(transaction)
        return await redis_client.exists(checksum) == 1

    async def get_original_checksum_value(
        self, redis_client: redis_async_pkg.Redis, transaction: Dict[str, Any]
    ) -> Optional[list]:
        """
        Gets the array of values stored in Redis for the transaction's key.
        """
        checksum = self.generate_checksum(transaction)
        value = await redis_client.get(checksum)
        if value is not None:
            try:
                return json.loads(value.decode('utf-8'))
            except Exception:
                logger.warning(
                    "Could not deserialize Redis value for key "
                    f"{checksum}"
                )
                return None
        return None

    async def add_checksum(
        self, redis_client: redis_async_pkg.Redis, transaction: Dict[str, Any]
    ) -> list:
        """
        Adds an object (checksum, extraction_date, transaction_date)
        to the array in Redis.
        """
        checksum_key = self.generate_checksum(transaction)
        new_entry = {
            "checksum": transaction.get("checksum"),
            "extraction_date": transaction.get("extraction_timestamp"),
            "transaction_date": transaction.get("transaction_date")
        }
        # Try to get the current array
        value = await redis_client.get(checksum_key)
        if value is not None:
            try:
                arr = json.loads(value.decode('utf-8'))
            except Exception:
                arr = []
        else:
            arr = []
        # Add only if the checksum is not repeated
        if not any(e['checksum'] == new_entry['checksum'] for e in arr):
            arr.append(new_entry)
        # Save the updated array
        await redis_client.set(
            checksum_key, json.dumps(arr), ex=self.EXACT_DUPLICATE_TTL
        )
        return arr

    async def delete_checksum(self, checksum_key: str) -> bool:
        """Deletes the checksum key from Redis for a given transaction."""
        try:
            result = await self.redis_client.delete(checksum_key)
            logger.info(
                f"Deletion attempt for key: {checksum_key}. "
                f"Keys deleted: {result}"
            )
            return result > 0
        except Exception as e:
            logger.error(
                f"Error deleting checksum key {checksum_key}: "
                f"{str(e)}"
            )
            return False

    async def process_transaction(
        self, transaction: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Processes a transaction and checks if it is a duplicate.
        Returns the array of collisions if any.
        """
        try:
            # Validate that transaction is a dictionary
            if not isinstance(transaction, dict):
                logger.error(
                    f"Transaction must be a dictionary, "
                    f"got {type(transaction)}"
                )
                return {
                    "is_duplicate": False,
                    "generated_checksum": None,
                    "conflicting_checksums": None,
                    "error": (
                        f"Transaction must be a dictionary, "
                        f"got {type(transaction)}"
                    )
                }
            
            generated_checksum = self.generate_checksum(transaction)
            existing_arr = await self.get_original_checksum_value(
                self.redis_client, transaction
            )
            if existing_arr:
                # If it exists, we add the new one and return the updated array
                arr = await self.add_checksum(self.redis_client, transaction)
                logger.info(
                    "Duplicate transaction detected. Current checksum: "
                    f"{generated_checksum}. Array in Redis: {arr}"
                )
                return {
                    "is_duplicate": True,
                    "generated_checksum": generated_checksum,
                    "conflicting_checksums": arr,
                    "error": None
                }
            # If it does not exist, we create the array with the first object
            arr = await self.add_checksum(self.redis_client, transaction)
            return {
                "is_duplicate": False,
                "generated_checksum": generated_checksum,
                "conflicting_checksums": None,
                "error": None
            }
        except Exception as e:
            logger.error(f"Error processing transaction: {str(e)}")
            return {
                "is_duplicate": False,
                "generated_checksum": None,
                "conflicting_checksums": None,
                "error": str(e)
            }