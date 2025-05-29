# transaction_update_detector.py

import re
from collections import Counter
import math
import time
from functools import wraps
import logging

from Levenshtein import distance as levenshtein_distance
import jellyfish
import json
import asyncio
import datetime


from redis import asyncio as redis_async_pkg

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- measure_time decorator ---
def measure_time(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start = time.time()
        if asyncio.iscoroutinefunction(func):
            result = await func(*args, **kwargs)
        else:
            result = func(*args, **kwargs) 
        total_time = time.time() - start
        logger.info(f"Execution time {func.__name__}: {total_time:.2f} seconds")
        return result
    return wrapper

class TransactionUpdateDetectorRedis:
    def __init__(self, redis_client: redis_async_pkg.Redis, redis_ttl_seconds: int = 7 * 24 * 60 * 60):
        self.redis_client = redis_client
        self.redis_data_ttl_seconds = redis_ttl_seconds
        self.levenshtein_threshold = 3
        self.cosine_threshold = 0.8
        self.jaro_winkler_threshold = 0.9

    def _format_amount_for_key(self, amount: any) -> str:
        return str(amount).strip()

    def _serialize_metadata_for_key(self, metadata: any) -> str:
        """
        Serializes metadata (expected as list of dicts [{'key':k, 'value':v},...])
        concatenating key+value of each item, and then all items together.
        Returns "N/A" if metadata is None, empty, or not a list.
        """
        if not metadata or not isinstance(metadata, list):
            return "N/A" 

        serialized_parts = []
        try:
            for item in metadata:
                if isinstance(item, dict) and 'key' in item and 'value' in item:
                    # Ensure key and value are strings before concatenating
                    key_str = str(item['key'])
                    value_str = str(item['value'])
                    serialized_parts.append(key_str + value_str)
                else:
                    
                    logger.warning(f"Metadata item does not have the expected format {{'key':k, 'value':v}}: {item}. Using 'invalid_metadata_item'.")
                    
                    return "invalid_metadata_structure" 
            
            if not serialized_parts: # If the list was empty or all items were invalid
                return "N/A"

            return "".join(serialized_parts)
        except Exception as e:
            logger.error(f"Error during simple metadata serialization (type: {type(metadata)}): {e}", exc_info=True)
            return "serialization_error"

    def _generate_redis_key_for_candidates(
        self, 
        company_id: str, 
        bank: str, 
        account_number: str, 
        transaction_date_str: str, 
        amount_str: str,
        metadata_serialized_str: str  # New parameter with serialized string
    ) -> str:
        """Generates Redis key including the serialized metadata string."""
        # Colons (:) are Redis separators. Ensure metadata_serialized_str doesn't contain them
        # or if it does, it doesn't cause problems with your key scheme
        # This simple serialization (e.g. "originextension...") shouldn't have them
        return f"tx_cand:{company_id}:{bank}:{account_number}:{transaction_date_str}:{amount_str}:{metadata_serialized_str}"
    
    def custom_is_na(self, value: any) -> bool: 
        return value is None or (isinstance(value, float) and math.isnan(value))

    async def _get_candidates_from_redis(self, redis_key: str) -> list[dict[str, str]] | None:
        # ... (no changes from your last version)
        try:
            if not await self.redis_client.exists(redis_key):
                logger.debug(f"Redis key {redis_key} does not exist.")
                return None
            json_cand_list_bytes = await self.redis_client.lrange(redis_key, 0, -1)
            if not json_cand_list_bytes:
                 logger.debug(f"Redis key {redis_key} exists but stores an empty list.")
                 return []
            candidates = [json.loads(cand_str_bytes.decode('utf-8')) for cand_str_bytes in json_cand_list_bytes]
            return candidates
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error for Redis key {redis_key}. Data might be corrupted: {e}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Error getting candidates from Redis key {redis_key}: {e}", exc_info=True)
            return None

    async def _store_list_to_redis(self, redis_key: str, list_to_store: list[dict[str, str]]):
        # ... (no changes from your last version)
        try:
            pipe = self.redis_client.pipeline()
            pipe.delete(redis_key) 
            if list_to_store: 
                for item_dict in list_to_store:
                    pipe.rpush(redis_key, json.dumps(item_dict))
            pipe.expire(redis_key, self.redis_data_ttl_seconds)
            await pipe.execute()
            action = "Stored" if list_to_store else "Stored empty list (marker)"
            logger.info(f"{action} with {len(list_to_store)} items in Redis for key: {redis_key}")
        except Exception as e:
            logger.error(f"Error storing list to Redis key {redis_key}: {e}", exc_info=True)

    def _normalize_text(self, text: any) -> str:
        if self.custom_is_na(text): 
            return ""
        text_str = str(text).lower()
        text_str = re.sub(r"[^\w\s]", "", text_str)
        text_str = ' '.join(text_str.split()) 
        return text_str

    def _cosine_similarity(self, text1_norm: str, text2_norm: str) -> float:
        words1 = text1_norm.split()
        words2 = text2_norm.split()
        if not words1 or not words2: return 0.0
        vector1 = Counter(words1)
        vector2 = Counter(words2)
        intersection = set(vector1.keys()) & set(vector2.keys())
        numerator = sum([vector1[x] * vector2[x] for x in intersection])
        sum1 = sum([vector1[x] ** 2 for x in vector1.keys()])
        sum2 = sum([vector2[x] ** 2 for x in vector2.keys()])
        denominator = math.sqrt(sum1) * math.sqrt(sum2)
        if not denominator: return 0.0
        return float(numerator) / denominator

    def _jaro_winkler_similarity(self, text1_norm: str, text2_norm: str) -> float:
        return jellyfish.jaro_winkler_similarity(text1_norm, text2_norm)

    @measure_time
    async def detect_updates(self, new_transaction: dict) -> list[dict]:
        try:
            company_id = new_transaction['company_id']
            bank = new_transaction['bank']
            account_number = new_transaction['account_number']
            
            date_input = new_transaction['transaction_date']
            
            if isinstance(date_input, datetime.datetime):
                transaction_date_obj = date_input.date()
            elif isinstance(date_input, datetime.date):
                transaction_date_obj = date_input
            elif isinstance(date_input, str):
                date_str_part = date_input.split(' ')[0]
                transaction_date_obj = datetime.datetime.strptime(date_str_part, '%Y-%m-%d').date()
            else:
                raise ValueError(f"Unsupported date format or unexpected type: {type(date_input)} for value {date_input}")
            transaction_date_str = transaction_date_obj.strftime('%Y-%m-%d')
            
            amount_str = self._format_amount_for_key(new_transaction['amount'])
            concept_new_raw = str(new_transaction.get('concept', ''))
            checksum_new = new_transaction['checksum']
            metadata_new = new_transaction.get('metadata') 
            
        except KeyError as e:
            logger.error(f"Required field missing in new_transaction: {e}", exc_info=True); return []
        except ValueError as e: 
            logger.error(f"Error processing date '{new_transaction.get('transaction_date')}': {e}", exc_info=True); return []
        except Exception as e: 
            logger.error(f"Error processing new_transaction fields: {e}", exc_info=True); return []

        
        metadata_serialized_str_new = self._serialize_metadata_for_key(metadata_new)

        redis_key = self._generate_redis_key_for_candidates(
            company_id, bank, account_number, transaction_date_str, amount_str, metadata_serialized_str_new  # Pass serialized metadata string
        )
        
        current_redis_candidates = await self._get_candidates_from_redis(redis_key)
        updated_transactions_details = [] 
        final_list_for_redis = [] 
        
        new_transaction_data_to_add = {"cs": checksum_new, "ctx": concept_new_raw}
        new_transaction_is_in_redis_list = False

        if current_redis_candidates is None: 
            logger.info(f"No data or invalid data in Redis for key {redis_key}. Will store new transaction.")
            final_list_for_redis = [new_transaction_data_to_add]
        else: 
            logger.info(f"Found {len(current_redis_candidates)} candidates in Redis for key {redis_key}.")
            normalized_concept_new = self._normalize_text(concept_new_raw)

            for cand in current_redis_candidates:
                checksum_candidate = cand['cs']
                if checksum_new == checksum_candidate:
                    new_transaction_is_in_redis_list = True
                    continue 
                concept_candidate_raw = cand['ctx']
                normalized_concept_candidate = self._normalize_text(concept_candidate_raw)
                lev_dist = levenshtein_distance(normalized_concept_new, normalized_concept_candidate)
                cos_sim = self._cosine_similarity(normalized_concept_new, normalized_concept_candidate) 
                jaro_sim = self._jaro_winkler_similarity(normalized_concept_new, normalized_concept_candidate)
                
                # Add logs to see the values
                logger.debug(f"Comparing concepts:")
                logger.debug(f"Original: {concept_candidate_raw}")
                logger.debug(f"New: {concept_new_raw}")
                logger.debug(f"Metrics: L={lev_dist}, C={cos_sim:.4f}, J={jaro_sim:.4f}")
                logger.debug(f"Thresholds: L<={self.levenshtein_threshold}, C>={self.cosine_threshold}, J>={self.jaro_winkler_threshold}")
                
                is_update = False
                if lev_dist <= self.levenshtein_threshold: 
                    is_update = True
                    logger.info("Update detected by Levenshtein")
                elif cos_sim >= self.cosine_threshold: 
                    is_update = True
                    logger.info("Update detected by Cosine")
                elif jaro_sim >= self.jaro_winkler_threshold: 
                    is_update = True
                    logger.info("Update detected by Jaro-Winkler")
                
                if is_update:
                    logger.debug(f"Update found: new_cs={checksum_new} updates orig_cs={checksum_candidate} (L:{lev_dist},C:{cos_sim:.2f},J:{jaro_sim:.2f})")
                    updated_transactions_details.append({
                        "original_checksum": checksum_candidate,
                        "new_checksum": checksum_new,
                        "metrics": {
                            "levenshtein_distance": lev_dist,
                            "cosine_similarity": round(cos_sim, 4),
                            "jaro_winkler_similarity": round(jaro_sim, 4)
                        },
                        "thresholds": {
                            "levenshtein": self.levenshtein_threshold,
                            "cosine": self.cosine_threshold,
                            "jaro_winkler": self.jaro_winkler_threshold
                        }
                    })
            
            if not new_transaction_is_in_redis_list:
                final_list_for_redis = current_redis_candidates + [new_transaction_data_to_add]
            else:
                final_list_for_redis = current_redis_candidates

        await self._store_list_to_redis(redis_key, final_list_for_redis)
            
        if updated_transactions_details:
            logger.info(f"Found {len(updated_transactions_details)} original transaction(s) updated by new transaction {checksum_new}")
            
        return updated_transactions_details