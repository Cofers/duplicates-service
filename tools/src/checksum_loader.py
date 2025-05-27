import logging
from typing import Dict, Any, Tuple, List
from google.cloud import bigquery
import datetime # Importante para manejar fechas de BigQuery
import asyncio # Necesario para asyncio.to_thread

# Asegúrate de que Mosaic es la nueva clase con la lógica de patrones y conteos
from mosaic import Mosaic 
from src.core.config import get_settings # Asumo que esto funciona como antes

logger = logging.getLogger(__name__)


class ChecksumLoader:
    """
    Herramienta para cargar checksums exactos e históricos de patrones de concepto (con conteos)
    de BigQuery a Redis.
    """
    def __init__(self):
        self.mosaic = Mosaic() # Instancia de la NUEVA clase Mosaic (con conteos)
        self.settings = get_settings()
        self.load_history_months = getattr(self.settings, 'CHECKSUM_LOADER_HISTORY_MONTHS', 12)
        logger.info(f"ChecksumLoader inicializado para cargar datos de los últimos {self.load_history_months} meses.")

    def _transform_metadata(self, metadata: List[Dict[str, str]]) -> Dict[str, str]:
        if not metadata:
            return {}
        return {item['key']: item['value'] for item in metadata}

    async def _execute_bq_query(self, query: str, job_config: bigquery.QueryJobConfig) -> List[Dict[str, Any]]:
        """
        Helper para ejecutar consultas de BigQuery de forma no bloqueante.
        """
        # self.mosaic.storage.bq_client es síncrono
        # Usamos asyncio.to_thread para ejecutar la llamada bloqueante en un hilo separado
        query_job = await asyncio.to_thread(
            self.mosaic.storage.bq_client.query, query, job_config=job_config
        )
        # .result() también puede ser bloqueante si el job no ha terminado,
        # pero query() usualmente espera a que el job termine por defecto para queries pequeñas.
        # Para jobs largos, se podría manejar de forma más asíncrona con job.done() y reintentos.
        # Aquí asumimos que query() es suficientemente rápido o que el bloqueo en to_thread es aceptable.
        return [dict(row) for row in await asyncio.to_thread(query_job.result)]


    async def generate_and_store_data_for_account(
        self,
        company_id: str,
        bank: str,
        account_number: str
    ) -> Tuple[bool, int, int]: # Éxito, ops_checksums_exactos, ops_conteos_patron
        try:
            query = f"""
            SELECT DISTINCT
                bank,
                account_number,
                concept,
                amount,
                metadata,
                transaction_date,
                checksum AS original_checksum_bq
            FROM 
                `{self.settings.BIGQUERY_DATASET}`.`{self.settings.BIGQUERY_TABLE}`
            WHERE company_id = @company_id
            AND bank = @bank
            AND account_number = @account_number
            AND transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @history_months MONTH) 
            ORDER BY transaction_date ASC -- Procesar en orden cronológico puede ser bueno para los conteos
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("company_id", "STRING", company_id),
                    bigquery.ScalarQueryParameter("bank", "STRING", bank),
                    bigquery.ScalarQueryParameter("account_number", "STRING", account_number),
                    bigquery.ScalarQueryParameter("history_months", "INTEGER", self.load_history_months),
                ]
            )
            
            transactions = await self._execute_bq_query(query, job_config) # Ejecución no bloqueante
            
            if not transactions:
                logger.info(
                    f"No se encontraron transacciones para la cuenta "
                    f"{company_id}:{bank}:{account_number} en los últimos {self.load_history_months} meses."
                )
                return True, 0, 0
            
            logger.info(f"Procesando {len(transactions)} transacciones para {company_id}:{bank}:{account_number}...")

            ops_exact_checksums = 0
            ops_concept_counts = 0 # Renombrado para claridad

            for i, transaction in enumerate(transactions):
                if (i + 1) % 200 == 0:
                    logger.info(f"Procesadas {i+1}/{len(transactions)} transacciones para {company_id}:{bank}:{account_number}")

                transaction['metadata'] = self._transform_metadata(
                    transaction.get('metadata', [])
                )
                
                exact_checksum = self.mosaic.generate_checksum(transaction)
                value_for_exact_checksum = transaction.get("original_checksum_bq") or exact_checksum
                
                success_add_exact = await self.mosaic.add_checksum(
                    exact_checksum,
                    value_for_exact_checksum, 
                    company_id,
                    bank,
                    account_number
                )
                if success_add_exact:
                    ops_exact_checksums += 1
            
                tx_date_for_pattern = transaction.get('transaction_date')
                if not tx_date_for_pattern:
                    logger.warning(f"Transacción (concepto: {transaction.get('concept', 'N/A')}) sin 'transaction_date', no se puede generar dato de patrón.")
                    continue

                if isinstance(tx_date_for_pattern, datetime.date):
                    transaction_date_str = tx_date_for_pattern.strftime("%Y-%m-%d")
                else:
                    transaction_date_str = str(tx_date_for_pattern).split(" ")[0]

                normalized_concept = self.mosaic._normalize_concept(transaction.get('concept', ''))
                
                # ---- CAMBIO IMPORTANTE AQUÍ ----
                # Llamar al nuevo método para incrementar conteos
                success_increment_count = await self.mosaic.increment_concept_monthly_count(
                    normalized_concept,
                    company_id,
                    bank,
                    account_number,
                    transaction_date_str
                )
                # ---- FIN DEL CAMBIO ----
                if success_increment_count:
                    ops_concept_counts += 1 # Contamos operaciones de incremento exitosas
            
            logger.info(
                f"Finalizado para {company_id}:{bank}:{account_number}. "
                f"Operaciones de checksums exactos: {ops_exact_checksums}. "
                f"Operaciones de incremento de conteo de conceptos: {ops_concept_counts}."
            )
            return True, ops_exact_checksums, ops_concept_counts
            
        except Exception as e:
            logger.error(f"Error cargando datos para la cuenta {company_id}:{bank}:{account_number}: {e}", exc_info=True)
            return False, 0, 0

    async def load_company_checksums(
        self,
        company_id: str
    ) -> Dict[str, Any]:
        try:
            query = f"""
            SELECT DISTINCT
                bank,
                account_number
            FROM `{self.settings.BIGQUERY_DATASET}`.`{self.settings.BIGQUERY_TABLE}`
            WHERE company_id = @company_id
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("company_id", "STRING", company_id),
                ]
            )

            accounts = await self._execute_bq_query(query, job_config) # Ejecución no bloqueante
            
            if not accounts:
                logger.info(f"No se encontraron cuentas para la empresa {company_id}")
                return {
                    "success": True, "total_accounts": 0,
                    "total_exact_checksum_ops": 0, "total_concept_count_ops": 0 # Renombrado
                }
            
            logger.info(f"Iniciando carga para {len(accounts)} cuentas de la empresa {company_id}.")
            
            total_exact_ops_company = 0
            total_concept_count_ops_company = 0 # Renombrado
            processed_accounts = 0

            for account in accounts:
                logger.info(f"Procesando cuenta: {account['bank']} / {account['account_number']} para empresa {company_id}")
                success, exact_ops, pattern_ops = await self.generate_and_store_data_for_account(
                    company_id,
                    account["bank"],
                    account["account_number"]
                )
                if success:
                    total_exact_ops_company += exact_ops
                    total_concept_count_ops_company += pattern_ops
                processed_accounts +=1
                logger.info(f"Progreso carga empresa: {processed_accounts}/{len(accounts)} cuentas procesadas para {company_id}.")
            
            logger.info(f"Carga completada para la empresa {company_id}.")
            return {
                "success": True,
                "total_accounts": len(accounts),
                "total_exact_checksum_ops": total_exact_ops_company,
                "total_concept_count_ops": total_concept_count_ops_company, # Renombrado
            }
            
        except Exception as e:
            logger.error(f"Error cargando datos para la empresa {company_id}: {e}", exc_info=True)
            return {"success": False, "error": str(e)}

    async def get_all_companies(self) -> List[str]:
        """
        Obtiene todas las empresas únicas de BigQuery.
        
        Returns:
            List[str]: Lista de IDs de empresas
        """
        try:
            query = f"""
            SELECT DISTINCT company_id
            FROM `{self.settings.BIGQUERY_DATASET}`.`{self.settings.BIGQUERY_TABLE}`
            ORDER BY company_id
            """
            
            job_config = bigquery.QueryJobConfig()
            results = await self._execute_bq_query(query, job_config)
            
            return [row['company_id'] for row in results]
            
        except Exception as e:
            logger.error(f"Error obteniendo lista de empresas: {e}", exc_info=True)
            return []