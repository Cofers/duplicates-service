#!/usr/bin/env python3
import asyncio
import logging
import sys
import os
import argparse
from tools.src.checksum_loader import ChecksumLoader


logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format=(
        "[%(asctime)s] %(levelname)s "
        "[%(module)s.%(funcName)s:%(lineno)d] %(message)s"
    ),
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
    Generate and store checksums in Redis for all accounts of a specific 
    company.
    
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
    
    # Load checksums for company
    result = await loader.load_company_checksums(company_id)
    
    if result["success"]:
        logger.info(
            f"Successfully processed {result['total_accounts']} accounts, "
            f"generated {result['total_new_checksums']} new checksums"
        )
    else:
        logger.error(
            f"Error loading checksums: {result.get('error', 'Unknown error')}"
        )
        sys.exit(1)


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