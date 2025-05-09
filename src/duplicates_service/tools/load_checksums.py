#!/usr/bin/env python3
import asyncio
import logging
import sys
import os
import argparse
from google.cloud import bigquery
from .checksum_loader import ChecksumLoader


logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s [%(module)s.%(funcName)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


async def generate_checksums_for_company(
    company_id: str,
    redis_host: str,
    redis_port: int,
    redis_db: int
) -> None:
    """
    Generate and store checksums in Redis for all accounts of a specific company.
    
    Args:
        company_id: Company ID to generate checksums for
        redis_host: Redis host
        redis_port: Redis port
        redis_db: Redis database number
    """
    # Set Redis environment variables
    os.environ["REDIS_HOST"] = redis_host
    os.environ["REDIS_PORT"] = str(redis_port)
    os.environ["REDIS_DB"] = str(redis_db)
    
    # Initialize loader
    loader = ChecksumLoader()
    
    # Initialize BigQuery client
    bq_client = bigquery.Client()
    
    # Query to get all company accounts
    query = f"""
    SELECT DISTINCT
        bank,
        account_number
    FROM `{os.environ.get("BIGQUERY_DATASET")}.{os.environ.get("BIGQUERY_TABLE")}`
    WHERE company_id = @company_id
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "company_id", "STRING", company_id
            ),
        ]
    )
    
    logger.info(f"Querying BigQuery for company_id: {company_id}")
    query_job = bq_client.query(query, job_config=job_config)
    accounts = [dict(row) for row in query_job]
    
    if not accounts:
        logger.info(f"No accounts found for {company_id}")
        return
    
    # Process each account
    total_processed = 0
    total_new_checksums = 0
    
    for account in accounts:
        try:
            success, new_checksums = await loader.generate_and_store_checksums(
                company_id,
                account["bank"],
                account["account_number"]
            )
            
            if success:
                total_new_checksums += new_checksums
            total_processed += 1
            
            # Log every 100 accounts
            if total_processed % 100 == 0:
                logger.info(
                    f"Processed {total_processed} accounts, "
                    f"{total_new_checksums} new checksums"
                )
                
        except Exception as e:
            logger.error(
                f"Error processing account {account['bank']}:"
                f"{account['account_number']}: {str(e)}"
            )
    
    logger.info(
        f"Finished processing {total_processed} accounts for "
        f"company_id: {company_id}, {total_new_checksums} new checksums"
    )


async def main():
    parser = argparse.ArgumentParser(
        description="Load checksums from BigQuery to Redis"
    )
    parser.add_argument(
        "--company-id",
        required=True,
        help="Company ID"
    )
    parser.add_argument(
        "--redis-host",
        default="localhost",
        help="Redis host (default: localhost)"
    )
    parser.add_argument(
        "--redis-port",
        type=int,
        default=6379,
        help="Redis port (default: 6379)"
    )
    parser.add_argument(
        "--redis-db",
        type=int,
        default=0,
        help="Redis database number (default: 0)"
    )
    
    args = parser.parse_args()
    
    try:
        await generate_checksums_for_company(
            args.company_id,
            args.redis_host,
            args.redis_port,
            args.redis_db
        )
    except Exception as e:
        logger.error(f"Error loading checksums: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main()) 