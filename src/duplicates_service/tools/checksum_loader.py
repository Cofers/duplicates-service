import logging
import hashlib
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple
from google.cloud import bigquery
from ..lib.storage import Storage
from ..core.config import get_settings


logger = logging.getLogger(__name__)


class ChecksumLoader:
    """
    Tool for loading checksums into Redis and maintaining date ranges.
    """
    def __init__(self):
        self.storage = Storage()
        self.settings = get_settings()
        self.date_range_prefix = "duplicate_date_range_"

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

    def _generate_checksum(self, transaction: Dict[str, Any]) -> str:
        """
        Generate a checksum from transaction fields.
        """
        # Normalizar el concepto
        concept = self._normalize_concept(transaction['concept'])
        
        hash_input = (
            f"{transaction['bank']}"
            f"{transaction['account_number']}"
            f"{concept}"
            f"{transaction['amount']}"
        )
        return hashlib.md5(hash_input.encode()).hexdigest()

    def _get_date_range_key(
        self, company_id: str, bank: str, account_number: str
    ) -> str:
        """
        Generate Redis key for date range.
        """
        return f"{self.date_range_prefix}{company_id}:{bank}:{account_number}"

    async def get_date_range(
        self, company_id: str, bank: str, account_number: str
    ) -> Tuple[datetime, datetime]:
        """
        Get date range stored in Redis.
        """
        try:
            key = self._get_date_range_key(company_id, bank, account_number)
            data = self.storage.client.get(key)
            if data:
                start_date, end_date = data.split("|")
                return (
                    datetime.fromtimestamp(float(start_date)),
                    datetime.fromtimestamp(float(end_date))
                )
            return None, None
        except Exception as e:
            logger.error(f"Error getting date range: {e}")
            return None, None

    async def update_date_range(
        self,
        company_id: str,
        bank: str,
        account_number: str,
        start_date: datetime,
        end_date: datetime
    ) -> bool:
        """
        Update date range in Redis.
        """
        try:
            key = self._get_date_range_key(company_id, bank, account_number)
            value = f"{start_date.timestamp()}|{end_date.timestamp()}"
            return bool(self.storage.client.set(
                key,
                value,
                ex=90 * 24 * 60 * 60  # 90 days
            ))
        except Exception as e:
            logger.error(f"Error updating date range: {e}")
            return False

    async def generate_and_store_checksums(
        self,
        company_id: str,
        bank: str,
        account_number: str
    ) -> Tuple[bool, int]:
        """
        Generate checksums from transaction data and store them in Redis.
        
        Args:
            company_id: Company ID
            bank: Bank identifier
            account_number: Account number
            
        Returns:
            Tuple[bool, int]: Success status and number of new checksums generated
        """
        try:
            # Get transactions from BigQuery
            query = f"""
            SELECT DISTINCT
                bank,
                account_number,
                concept,
                amount,
                checksum as original_checksum
            FROM `{self.settings.BIGQUERY_DATASET}.{self.settings.BIGQUERY_TABLE}`
            WHERE company_id = @company_id
            AND bank = @bank
            AND account_number = @account_number
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "company_id", "STRING", company_id
                    ),
                    bigquery.ScalarQueryParameter(
                        "bank", "STRING", bank
                    ),
                    bigquery.ScalarQueryParameter(
                        "account_number", "STRING", account_number
                    ),
                ]
            )
            
            query_job = self.storage.bq_client.query(query, job_config=job_config)
            transactions = [dict(row) for row in query_job]
            
            if not transactions:
                logger.info(
                    f"No transactions found for "
                    f"{company_id}:{bank}:{account_number}"
                )
                return True, 0
            
            # Generate checksums for each transaction
            checksums = [
                (self._generate_checksum(t), t['original_checksum']) 
                for t in transactions
            ]
            
            # Log generated checksums
            for dup_checksum, orig_checksum in checksums:
                logger.info(
                    f"Generated checksum pair for {bank}:{account_number}: "
                    f"duplicate={dup_checksum}, original={orig_checksum}"
                )
            
            # Process checksums in groups
            success, new_checksums = await self.storage.process_checksum_group(
                company_id,
                bank,
                account_number,
                checksums
            )
            
            return success, new_checksums
            
        except Exception as e:
            logger.error(f"Error loading checksums: {e}")
            return False, 0

    async def load_company_checksums(
        self,
        company_id: str
    ) -> Dict[str, Any]:
        """
        Load checksums for all accounts of a company.
        """
        try:
            # Get all company accounts
            query = f"""
            SELECT DISTINCT
                bank,
                account_number
            FROM `{self.settings.BIGQUERY_DATASET}.{self.settings.BIGQUERY_TABLE}`
            WHERE company_id = @company_id
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "company_id", "STRING", company_id
                    ),
                ]
            )
            
            query_job = self.storage.bq_client.query(query, job_config=job_config)
            accounts = [dict(row) for row in query_job]
            
            if not accounts:
                logger.info(f"No accounts found for {company_id}")
                return {
                    "success": True,
                    "total_accounts": 0,
                    "total_new_checksums": 0
                }
            
            # Process each account
            total_new_checksums = 0
            for account in accounts:
                success, new_checksums = await self.generate_and_store_checksums(
                    company_id,
                    account["bank"],
                    account["account_number"]
                )
                if success:
                    total_new_checksums += new_checksums
            
            return {
                "success": True,
                "total_accounts": len(accounts),
                "total_new_checksums": total_new_checksums
            }
            
        except Exception as e:
            logger.error(f"Error loading company checksums: {e}")
            return {
                "success": False,
                "error": str(e)
            } 