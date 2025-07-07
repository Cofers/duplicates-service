import logging
from typing import Dict, Any, Optional, List
import re
from redis import asyncio as redis_async_pkg
import json
from datetime import datetime, timedelta
import jellyfish

from src.core.config import get_settings
from src.storage import Storage

logger = logging.getLogger("mosaic")


class Mosaic:
    def __init__(self, redis_client: redis_async_pkg.Redis = None):
        self.settings = get_settings()
        self.daily_transactions_prefix = "daily_tx_list_"
        self.DAILY_TX_LIST_TTL = 7 * 24 * 3600
        # Lazy initialization - don't load the model until needed
        self._embedding_model = None

        if redis_client:
            self.redis_client = redis_client
            self.storage = Storage()
            self.storage.client = redis_client
        else:
            self.storage = Storage()
            self.redis_client = self.storage.client

    @property
    def embedding_model(self):
        """Lazy load the embedding model only when needed"""
        if self._embedding_model is None:
            try:
                from sentence_transformers import SentenceTransformer
                self._embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
                logger.info("SentenceTransformer model loaded successfully")
            except Exception as e:
                logger.error(f"Failed to load SentenceTransformer model: {e}")
                raise
        return self._embedding_model

    def _format_field_for_key(self, value: Any) -> str:
        if value is None:
            return "null"
        if isinstance(value, (int, float)):
            return f"{value:.2f}"
        if isinstance(value, str):
            return re.sub(r'\s+', '', value.lower())
        if isinstance(value, list):
            return "_".join(self._format_field_for_key(item) for item in value)
        if isinstance(value, dict):
            return "_".join(self._format_field_for_key(v) for v in value.values())
        return str(value)

    def _normalize_concept(self, concept: str) -> str:
        concept_str = str(concept).lower()
        concept_str = ''.join(c for c in concept_str if c.isalnum() or c.isspace())
        return concept_str.strip()

    def _get_daily_redis_key(self, company_id: str, bank: str, account_number: str, transaction_date: str) -> str:
        return f"{self.daily_transactions_prefix}{self._format_field_for_key(company_id)}:{self._format_field_for_key(bank)}:{self._format_field_for_key(account_number)}:{self._format_field_for_key(transaction_date)}"

    async def get_transactions_for_day(self, redis_client: redis_async_pkg.Redis, company_id: str, bank: str, account_number: str, transaction_date: str) -> List[Dict[str, Any]]:
        daily_key = self._get_daily_redis_key(company_id, bank, account_number, transaction_date)
        value = await redis_client.get(daily_key)
        if value is not None:
            try:
                return json.loads(value.decode('utf-8'))
            except Exception as e:
                logger.warning(f"Error decoding Redis value for key {daily_key}: {e}")
                return []
        return []

    def compute_concept_embedding(self, concept: str) -> List[float]:
        try:
            return self.embedding_model.encode(concept).tolist()
        except Exception as e:
            logger.error(f"Failed to compute embedding for concept '{concept}': {e}", exc_info=True)
            return []

    def _is_similar_concept(self, concept1: str, concept2: str, threshold: float = 0.75, emb1: Optional[List[float]] = None, emb2: Optional[List[float]] = None) -> bool:
        if not concept1 or not concept2:
            return False

        concept1_norm = self._normalize_concept(concept1)
        concept2_norm = self._normalize_concept(concept2)

        if concept1_norm in concept2_norm or concept2_norm in concept1_norm:
            logger.info(f"[SUBSTRING MATCH] '{concept1_norm}' ⊂ '{concept2_norm}' or vice versa → match aceptado")
            return True

        jw_score = jellyfish.jaro_winkler_similarity(concept1_norm, concept2_norm)
        if jw_score > 0.93:
            logger.info(f"[JARO-WINKLER] Score {jw_score:.4f} between '{concept1_norm}' and '{concept2_norm}' → match")
            return True

        try:
            if emb1 is None:
                emb1 = self.compute_concept_embedding(concept1)
            if emb2 is None or not isinstance(emb2, list):
                emb2 = self.compute_concept_embedding(concept2)
            
            # Lazy import of torch and util
            import torch
            from sentence_transformers import util
            
            similarity = util.cos_sim(torch.tensor(emb1).unsqueeze(0), torch.tensor(emb2).unsqueeze(0)).item()
            logger.info(f"[SIMILARITY] '{concept1}' vs '{concept2}' => score: {similarity:.4f}, threshold: {threshold}")
            return similarity >= threshold
        except Exception as e:
            logger.error(f"Error computing embedding similarity: {e}", exc_info=True)
            return False

    def _is_similar_amount_decimal(self, amount1: float, amount2: float, threshold: float = 0.5) -> bool:
        if amount1 is None or amount2 is None:
            return False
        int1 = int(abs(amount1))
        int2 = int(abs(amount2))
        match = int1 == int2
        delta = abs(amount1 - amount2)
        logger.info(f"[DECIMAL MATCH] amount1={amount1}, amount2={amount2}, int_match={match}, delta={delta}")
        return match and delta <= threshold

    async def add_transaction_to_daily_list(self, redis_client: redis_async_pkg.Redis, transaction: Dict[str, Any]) -> None:
        key = self._get_daily_redis_key(transaction['company_id'], transaction['bank'], transaction['account_number'], transaction['transaction_date'])
        tx_list = await self.get_transactions_for_day(redis_client, transaction['company_id'], transaction['bank'], transaction['account_number'], transaction['transaction_date'])
        tx_list.append({
            'checksum': transaction['checksum'],
            'concept': transaction['concept'],
            'amount': transaction['amount'],
            'transaction_date': transaction['transaction_date'],
            'extraction_date': transaction['extraction_date'],
        })
        await redis_client.set(key, json.dumps(tx_list), ex=self.DAILY_TX_LIST_TTL)
        logger.info(f"Added new transaction to daily list for key: {key}. New list size: {len(tx_list)}")

    async def get_transactions_last_days(self, company_id: str, bank: str, account_number: str, current_date: str, days_back: int = 3) -> List[Dict[str, Any]]:
        all_txs = []
        current_date_obj = datetime.strptime(current_date, "%Y-%m-%d")
        for i in range(1, days_back + 1):
            prev_date = (current_date_obj - timedelta(days=i)).strftime("%Y-%m-%d")
            txs = await self.get_transactions_for_day(self.redis_client, company_id, bank, account_number, prev_date)
            logger.info(f"Found {len(txs)} transactions on day {prev_date} (within lookback for date change rule)")
            all_txs.extend(txs)
        return all_txs

    async def process_transaction(self, transaction: Dict[str, Any]) -> Dict[str, Any]:
        try:
            company_id = transaction['company_id']
            bank = transaction['bank']
            account_number = transaction['account_number']
            transaction_date = transaction['transaction_date']
            extraction_date = transaction['extraction_date']
            checksum = transaction['checksum']
            amount = transaction['amount']
            concept = transaction['concept']

            logger.info(f"Processing transaction {checksum} on {transaction_date} (extraction: {extraction_date})")

            txs_same_day = await self.get_transactions_for_day(self.redis_client, company_id, bank, account_number, transaction_date)
            logger.info(f"Found {len(txs_same_day)} transactions on same day")

            for existing in txs_same_day:
                if existing['checksum'] == checksum:
                    logger.info(f"Same checksum {checksum} already stored, skipping")
                    return {
                        "status": "no_conflict",
                        "reason": "same_checksum_ignore",
                        "provided_checksum": checksum,
                        "conflicts": [],
                        "error": None
                    }

            for existing in txs_same_day:
                if existing['extraction_date'] >= extraction_date:
                    continue

                amount_match = amount == existing['amount']
                logger.info(f"Comparando amount={amount} con {existing['amount']}")
                logger.info(f"Resultado comparación: amount_match={amount_match}")

                if amount_match:
                    if self._is_similar_concept(concept, existing['concept']):
                        logger.info(f"[REGLA 1] Detected enriched_concept vs tx {existing['checksum']}")
                        return {
                            "status": "conflict",
                            "reason": "enriched_concept",
                            "provided_checksum": checksum,
                            "conflicts": [existing['checksum']],
                            "error": None
                        }

                if self._is_similar_amount_decimal(amount, existing['amount']):
                    if self._is_similar_concept(concept, existing['concept']):
                        logger.info(f"[REGLA 2] Detected concept+amount (decimals) update vs tx {existing['checksum']}")
                        return {
                            "status": "conflict",
                            "reason": "concept_amount_update",
                            "provided_checksum": checksum,
                            "conflicts": [existing['checksum']],
                            "error": None
                        }

            # REGLA 3: Buscar en días anteriores
            txs_past_days = await self.get_transactions_last_days(company_id, bank, account_number, transaction_date, days_back=3)
            for existing in txs_past_days:
                if existing['extraction_date'] >= extraction_date:
                    continue
                if amount == existing['amount'] and self._normalize_concept(concept) == self._normalize_concept(existing['concept']):
                    logger.info(f"[REGLA 3] Detected date_correction vs tx {existing['checksum']}")
                    return {
                        "status": "conflict",
                        "reason": "date_change_same_content",
                        "provided_checksum": checksum,
                        "conflicts": [existing['checksum']],
                        "error": None
                    }

            await self.add_transaction_to_daily_list(self.redis_client, transaction)
            logger.info(f"No conflicts found for {checksum}, storing as new.")

            return {
                "status": "no_conflict",
                "reason": "new_transaction",
                "provided_checksum": checksum,
                "conflicts": [],
                "error": None
            }

        except Exception as e:
            logger.error(f"Error in process_transaction: {e}", exc_info=True)
            return {
                "status": "error",
                "reason": "exception",
                "provided_checksum": transaction.get('checksum'),
                "conflicts": [],
                "error": str(e)
            }
