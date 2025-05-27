#!/usr/bin/env python3
import asyncio
import logging
import datetime 
import os
import sys
import argparse
from typing import List, Dict, Any, Tuple # Optional ya no es necesario aquí si Python >= 3.10
from collections import defaultdict

# Importaciones de Redis y BigQuery
from redis import asyncio as redis_async_pkg
from redis.exceptions import ConnectionError as RedisConnectionError
from google.cloud import bigquery
from google.oauth2 import service_account # Para autenticación con service account

# Importar tu clase (asegúrate que la ruta es correcta)
try:
    from src.transaction_update_detector import TransactionUpdateDetectorRedis
except ImportError as e:
    print(f"Error: No se pudo importar TransactionUpdateDetectorRedis desde src.transaction_update_detector.")
    print(f"Asegúrate de que el archivo transaction_update_detector.py está en la carpeta src y que ejecutas este script desde el directorio correcto o que src está en tu PYTHONPATH.")
    print(f"Detalle del error: {e}")
    sys.exit(1)

# Configurar el logger (como lo tenías)
logger = logging.getLogger("load_checksum_updates") # Nombre específico para este logger
logger.setLevel(logging.INFO)
if not logger.handlers: # Evitar añadir múltiples handlers si el script se re-importa o se llama varias veces
    handler = logging.StreamHandler(sys.stdout) # Usar sys.stdout
    handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - [%(module)s.%(funcName)s:%(lineno)d] - %(message)s'))
    logger.addHandler(handler)

class LoaderSettings:
    def __init__(self, gcp_project_id: str, bq_dataset: str, bq_table: str, 
                 history_days: int): # google_credentials_path eliminado de aquí
        self.GCP_PROJECT_ID = gcp_project_id
        self.BIGQUERY_DATASET = bq_dataset
        self.BIGQUERY_TABLE_NAME = bq_table
        self.BIGQUERY_FULL_TABLE_ID = f"`{gcp_project_id}.{bq_dataset}.{bq_table}`"
        self.HISTORY_DAYS = history_days
# --- FIN Clase Settings ---

class UpdateCandidatesLoader:
    def __init__(self, 
                 bq_client: bigquery.Client, 
                 settings: LoaderSettings, 
                 detector_instance: TransactionUpdateDetectorRedis):
        self.bq_client = bq_client
        self.settings = settings
        self.detector = detector_instance # Instancia del detector para usar sus helpers

    # _transform_bq_metadata ya no es necesario aquí, la lógica de serialización
    # está en TransactionUpdateDetectorRedis._serialize_metadata_for_key
    # y espera un formato específico (lista de dicts con "key" y "value")

    async def _execute_bq_query(self, query: str, job_config: bigquery.QueryJobConfig | None = None) -> List[Dict[str, Any]]:
        try:
            query_job = await asyncio.to_thread(self.bq_client.query, query, job_config=job_config)
            return [dict(row) for row in await asyncio.to_thread(query_job.result)]
        except Exception as e:
            logger.error(f"Error ejecutando query en BigQuery: {query[:200]}... Error: {e}", exc_info=True)
            raise

    async def load_and_store_for_account(self, company_id: str, bank: str, account_number: str) -> Tuple[bool, int]:
        try:
            query = f"""
            SELECT DISTINCT
                checksum, 
                concept,
                transaction_date,
                amount,
                metadata, -- Este campo es clave
                company_id, 
                bank,       
                account_number 
            FROM {self.settings.BIGQUERY_FULL_TABLE_ID}
            WHERE company_id = @company_id
              AND bank = @bank
              AND account_number = @account_number
              AND transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @history_days DAY) 
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("company_id", "STRING", company_id),
                    bigquery.ScalarQueryParameter("bank", "STRING", bank),
                    bigquery.ScalarQueryParameter("account_number", "STRING", account_number),
                    bigquery.ScalarQueryParameter("history_days", "INTEGER", self.settings.HISTORY_DAYS),
                ]
            )
            
            transactions_from_bq = await self._execute_bq_query(query, job_config)
            
            if not transactions_from_bq:
                logger.info(f"No transactions found for account {company_id}:{bank}:{account_number} in the last {self.settings.HISTORY_DAYS} days.")
                return True, 0

            logger.info(f"Processing {len(transactions_from_bq)} BQ transactions for account {company_id}:{bank}:{account_number}...")
            grouped_for_redis = defaultdict(list)

            for i, tx_row in enumerate(transactions_from_bq):
                if (i + 1) % 500 == 0:
                    logger.info(f"Formatted {i+1}/{len(transactions_from_bq)} BQ transactions for grouping...")

                bq_company_id = tx_row['company_id']
                bq_bank = tx_row['bank']
                bq_account_number = tx_row['account_number']
                
                # --- MANEJO DE METADATA PARA EL LOADER ---
                raw_metadata_from_bq = tx_row.get('metadata')
                metadata_for_detector_serializer: List[Dict[str, str]] | None = None

                if isinstance(raw_metadata_from_bq, list):
                    metadata_for_detector_serializer = []
                    for item_bq in raw_metadata_from_bq:
                        if isinstance(item_bq, dict):
                            # IMPORTANTE: Asumimos que los structs de BQ tienen campos llamados 'key' y 'value'.
                            # Si BQ usa otros nombres (ej. 'name', 'val'), necesitas mapearlos aquí:
                            # k = item_bq.get('nombre_real_de_la_clave_en_bq')
                            # v = item_bq.get('nombre_real_del_valor_en_bq')
                            k = item_bq.get('key') 
                            v = item_bq.get('value')
                            if k is not None and v is not None:
                                metadata_for_detector_serializer.append({"key": str(k), "value": str(v)})
                            else:
                                logger.warning(f"Metadata item de BQ no tiene 'key' o 'value', o son None: {item_bq} en tx {tx_row.get('checksum')}")
                        else:
                            logger.warning(f"Item en lista de metadata de BQ no es un dict: {item_bq} en tx {tx_row.get('checksum')}")
                elif raw_metadata_from_bq is not None: # Si no es una lista y no es None
                    logger.warning(f"Metadata de BQ para tx {tx_row.get('checksum')} no es una lista ({type(raw_metadata_from_bq)}). "
                                   "El serializador espera una lista de dicts {{'key':k, 'value':v}} o None.")
                    # _serialize_metadata_for_key devolverá "N/A" o similar si no es lista.
                    # Se pasa tal cual para que el serializador lo maneje.
                    metadata_for_detector_serializer = raw_metadata_from_bq # type: ignore
                # Si raw_metadata_from_bq es None, metadata_for_detector_serializer permanecerá None.

                amount_str = self.detector._format_amount_for_key(str(tx_row['amount']))
                
                transaction_date_value = tx_row['transaction_date']
                if isinstance(transaction_date_value, datetime.date):
                    tx_date_obj = transaction_date_value
                elif isinstance(transaction_date_value, datetime.datetime):
                    tx_date_obj = transaction_date_value.date()
                elif isinstance(transaction_date_value, str):
                     tx_date_obj = datetime.datetime.strptime(transaction_date_value.split("T")[0], '%Y-%m-%d').date()
                else:
                    logger.warning(f"Tipo de fecha no esperado de BQ: {type(transaction_date_value)} para transacción {tx_row.get('checksum')}. Saltando.")
                    continue
                tx_date_str = tx_date_obj.strftime('%Y-%m-%d')
                
                # Usar el método _serialize_metadata_for_key del detector
                metadata_serialized_str = self.detector._serialize_metadata_for_key(metadata_for_detector_serializer)

                redis_key = self.detector._generate_redis_key_for_candidates(
                    bq_company_id, bq_bank, bq_account_number,
                    tx_date_str, amount_str, metadata_serialized_str
                )
                
                grouped_for_redis[redis_key].append({
                    "cs": str(tx_row['checksum']),
                    "ctx": str(tx_row.get('concept', ''))
                })
            
            logger.info(f"Finished grouping. {len(grouped_for_redis)} unique Redis keys to process for account {company_id}:{bank}:{account_number}.")
            keys_written_count = 0
            for r_key, items_list in grouped_for_redis.items():
                try:
                    await self.detector._store_list_to_redis(r_key, items_list)
                    keys_written_count += 1
                    if keys_written_count > 0 and keys_written_count % 100 == 0:
                        logger.info(f"Stored {keys_written_count}/{len(grouped_for_redis)} Redis keys for account...")
                except Exception as e:
                    logger.error(f"Failed to store data for Redis key {r_key}: {e}", exc_info=True)
            
            logger.info(f"Successfully processed account {company_id}:{bank}:{account_number}. Wrote {keys_written_count} Redis keys.")
            return True, keys_written_count
        except Exception as e:
            logger.error(f"Error loading data for account {company_id}:{bank}:{account_number}: {e}", exc_info=True)
            return False, 0
    
    async def load_for_company(self, company_id: str) -> Dict[str, Any]:
        try:
            query = f"""
            SELECT DISTINCT bank, account_number
            FROM {self.settings.BIGQUERY_FULL_TABLE_ID}
            WHERE company_id = @company_id
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("company_id", "STRING", company_id)]
            )
            accounts = await self._execute_bq_query(query, job_config)
            
            if not accounts:
                logger.info(f"No accounts found for company {company_id}")
                return {"success": True, "total_accounts_processed": 0, "total_redis_keys_written": 0}
            
            logger.info(f"Starting load for {len(accounts)} accounts of company {company_id}.")
            total_redis_keys_company = 0
            processed_accounts_count = 0

            for i, account in enumerate(accounts):
                logger.info(f"Processing account {i+1}/{len(accounts)}: {account['bank']}/{account['account_number']} for company {company_id}")
                success, keys_written = await self.load_and_store_for_account(
                    company_id, account["bank"], account["account_number"]
                )
                if success:
                    total_redis_keys_company += keys_written
                processed_accounts_count += 1 # Contar incluso si algunos almacenamientos fallan pero la cuenta se procesó
            
            logger.info(f"Load completed for company {company_id}.")
            return {
                "success": True, # El éxito general de la compañía podría redefinirse si alguna cuenta falla críticamente
                "total_accounts_processed": processed_accounts_count,
                "total_redis_keys_written": total_redis_keys_company,
            }
        except Exception as e:
            logger.error(f"Error loading data for company {company_id}: {e}", exc_info=True)
            return {"success": False, "error": str(e)}

    async def get_all_companies(self) -> List[str]:
        try:
            query = f"""SELECT DISTINCT company_id FROM {self.settings.BIGQUERY_FULL_TABLE_ID} ORDER BY company_id"""
            results = await self._execute_bq_query(query) 
            return [row['company_id'] for row in results]
        except Exception as e:
            logger.error(f"Error fetching company list: {e}", exc_info=True)
            return []

async def process_single_company(company_id: str, loader: UpdateCandidatesLoader) -> None:
    logger.info(f"Starting process for company: {company_id}")
    result = await loader.load_for_company(company_id)
    if result["success"]:
        logger.info(
            f"Process completed for company {company_id}. "
            f"Accounts processed: {result['total_accounts_processed']}. "
            f"Redis keys written: {result['total_redis_keys_written']}."
        )
    else:
        logger.error(f"Error loading data for company {company_id}: {result.get('error', 'Unknown error')}")

async def process_all_companies(loader: UpdateCandidatesLoader) -> None:
    companies = await loader.get_all_companies()
    if not companies:
        logger.error("No companies found to process.")
        return
    
    logger.info(f"Starting processing for {len(companies)} companies.")
    for company_id in companies:
        await process_single_company(company_id, loader)
    logger.info("Processing for all companies completed.")

async def main_loader_script():
    parser = argparse.ArgumentParser(description="Load transaction update candidates from BigQuery to Redis.")
    parser.add_argument("--company-id", help="Specific Company ID to process.")
    parser.add_argument("--all-companies", action="store_true", help="Process all companies found in BigQuery.")
    
    parser.add_argument("--redis-host", default=os.getenv("REDIS_HOST", "localhost"), help="Redis host.")
    parser.add_argument("--redis-port", type=int, default=int(os.getenv("REDIS_PORT", 6379)), help="Redis port.")
    parser.add_argument("--redis-db", type=int, default=int(os.getenv("REDIS_DB", 0)), help="Redis DB number.")
    parser.add_argument("--redis-password", default=os.getenv("REDIS_PASSWORD"), help="Redis password (optional).")

    default_gcp_project_id = os.getenv("GCP_PROJECT_ID")
    default_bq_dataset = os.getenv("BQ_DATASET")
    default_bq_table = os.getenv("BQ_TABLE_TRANSACTIONS") 
    default_history_days = int(os.getenv("LOADER_HISTORY_DAYS", 7)) 

    parser.add_argument("--gcp-project-id", default=default_gcp_project_id, 
                        required=default_gcp_project_id is None, help="Google Cloud Project ID.")
    parser.add_argument("--bq-dataset", default=default_bq_dataset, 
                        required=default_bq_dataset is None, help="BigQuery Dataset ID.")
    parser.add_argument("--bq-table", default=default_bq_table, 
                        required=default_bq_table is None, help="BigQuery Table name (transactions table).")
    parser.add_argument("--history-days", type=int, default=default_history_days, 
                        help=f"Number of past days of transactions to load (default: {default_history_days}).")
    parser.add_argument("--google-credentials-path", default=os.getenv("GOOGLE_APPLICATION_CREDENTIALS"), 
                        help="Path to Google Cloud service account JSON key file (optional).")
    
    args = parser.parse_args()

    if not args.company_id and not args.all_companies:
        parser.error("Either --company-id or --all-companies must be specified.")
    if args.company_id and args.all_companies: # Evitar especificar ambos
        parser.error("Cannot use --company-id and --all-companies simultaneously.")
    
    redis_client = None
    try:
        redis_url = f"redis://{args.redis_host}:{args.redis_port}/{args.redis_db}"
        if args.redis_password: # <--- Uso de contraseña de Redis
            redis_url = f"redis://:{args.redis_password}@{args.redis_host}:{args.redis_port}/{args.redis_db}"
        
        logger.info(f"Connecting to Redis: {args.redis_host}:{args.redis_port}, DB: {args.redis_db}")
        redis_client = redis_async_pkg.from_url(redis_url, decode_responses=False)
        await redis_client.ping()
        logger.info("Successfully connected to Redis.")

        logger.info(f"Initializing BigQuery client for project: {args.gcp_project_id}")
        if args.google_credentials_path:
            credentials = service_account.Credentials.from_service_account_file(args.google_credentials_path)
            bq_client_instance = bigquery.Client(project=args.gcp_project_id, credentials=credentials)
        else:
            bq_client_instance = bigquery.Client(project=args.gcp_project_id)
        logger.info("BigQuery client initialized.")

        detector = TransactionUpdateDetectorRedis(redis_client=redis_client) 
        
        loader_settings = LoaderSettings(
            gcp_project_id=args.gcp_project_id,
            bq_dataset=args.bq_dataset,
            bq_table=args.bq_table,
            history_days=args.history_days
            # google_credentials_path ya no está en LoaderSettings
        )
        
        loader = UpdateCandidatesLoader(
            bq_client=bq_client_instance, 
            settings=loader_settings, 
            detector_instance=detector
        )

        if args.all_companies:
            await process_all_companies(loader)
        elif args.company_id:
            await process_single_company(args.company_id, loader)

    except RedisConnectionError as e:
        logger.error(f"Could not connect to Redis: {e}", exc_info=True)
        sys.exit(1)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if redis_client:
            await redis_client.aclose() # Usar aclose() para el cliente async de redis-py
            logger.info("Redis connection closed.")

if __name__ == "__main__":
    # Asegurar que 'src' (o el directorio donde está transaction_update_detector.py) es importable
    # Esto depende de cómo ejecutes el script. Si el script está en la raíz del proyecto
    # y src es una subcarpeta, `from src...` debería funcionar si ejecutas desde la raíz.
    # Si este script está en, por ejemplo, `tools/load_checksum_updates.py`
    # y `src` está en `../src`, entonces sys.path necesita ajuste.
    
    # Ejemplo de ajuste de sys.path (simplificado, puedes hacerlo más robusto)
    # Asume que 'src' está al mismo nivel que la carpeta que contiene este script,
    # o que el script se ejecuta desde un directorio que tiene 'src' como subdirectorio.
    # sys.path.append(os.path.join(os.path.dirname(__file__), '..')) # Si src está un nivel arriba
    # sys.path.append(os.path.join(os.path.dirname(__file__), 'src')) # Si src es una subcarpeta
    # La forma más simple es asegurar que ejecutas el script desde la raíz del proyecto donde 'src' es visible.

    logger.info("Starting Checksum Update Candidates Loader script...")
    asyncio.run(main_loader_script())
    logger.info("Checksum Update Candidates Loader script finished.")