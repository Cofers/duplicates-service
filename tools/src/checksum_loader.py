import logging
from typing import Dict, Any, Tuple, List
from google.cloud import bigquery
from src.lib.mosaic import Mosaic
from src.core.config import get_settings

logger = logging.getLogger(__name__)


class ChecksumLoader:
    """
    Tool for loading checksums from BigQuery into Redis.
    """
    def __init__(self):
        self.mosaic = Mosaic()
        self.settings = get_settings()

    def _transform_metadata(self, metadata: List[Dict[str, str]]) -> Dict[str, str]:
        """
        Transform metadata from BigQuery format to PubSub format.
        
        Args:
            metadata: List of dicts with 'key' and 'value' pairs
            
        Returns:
            Dict with key-value pairs
        """
        if not metadata:
            return {}
            
        return {item['key']: item['value'] for item in metadata}

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
            Tuple[bool, int]: Success status and number of new checksums 
                              generated
        """
        try:
            # Get transactions from BigQuery
            query = f"""
            SELECT DISTINCT
                bank,
                account_number,
                concept,
                amount,
                metadata,
                checksum as original_checksum
            FROM 
                `{self.settings.BIGQUERY_DATASET}`
                .`{self.settings.BIGQUERY_TABLE}`
            WHERE company_id = @company_id
            AND bank = @bank
            AND account_number = @account_number
            AND transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
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
            
            query_job = self.mosaic.storage.bq_client.query(
                query, job_config=job_config
            )
            transactions = [dict(row) for row in query_job]
            
            
            if not transactions:
                logger.info(
                    f"No transactions found for "
                    f"{company_id}:{bank}:"
                    f"{account_number}"
                )
                return True, 0
            
            # Process each transaction
            new_checksums = 0
            for transaction in transactions:
                # Transform metadata to match PubSub format
                transaction['metadata'] = self._transform_metadata(
                    transaction.get('metadata', [])
                )
                
                # Generar checksum usando Mosaic
                checksum = self.mosaic.generate_checksum(transaction)
                
                # Añadir checksum usando Mosaic
                success = await self.mosaic.add_checksum(
                    checksum,
                    transaction.get("original_checksum", ""),
                    company_id,
                    bank,
                    account_number
                )
                
                if success:
                    new_checksums += 1
            
            return True, new_checksums
            
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
            FROM `{self.settings.BIGQUERY_DATASET}`
            .`{self.settings.BIGQUERY_TABLE}`
            WHERE company_id = @company_id
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "company_id", "STRING", company_id
                    ),
                ]
            )
            
            query_job = self.mosaic.storage.bq_client.query(
                query, job_config=job_config
            )
            accounts = [dict(row) for row in query_job]
            
            if not accounts:
                logger.info(
                    f"No accounts found for "
                    f"{company_id}"
                )
                return {
                    "success": True,
                    "total_accounts": 0,
                    "total_new_checksums": 0
                }
            
            # Process each account
            total_new_checksums = 0
            for account in accounts:
                success, new_checksums = await (
                    self.generate_and_store_checksums(
                        company_id,
                        account["bank"],
                        account["account_number"]
                    )
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