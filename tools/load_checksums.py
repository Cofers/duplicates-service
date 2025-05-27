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
            f"Proceso completado para la empresa {company_id}. "
            f"Cuentas procesadas: {result['total_accounts']}. "
            f"Operaciones de checksums exactos: {result.get('total_exact_checksum_ops', 'N/A')}. "
            f"Operaciones de patrones de concepto: {result.get('total_pattern_ops', 'N/A')}."
        )
    else:
        logger.error(
            f"Error cargando datos para la empresa {company_id}: "
            f"{result.get('error', 'Error desconocido')}"
        )
        sys.exit(1)  # Salir con error


async def generate_checksums_for_all_companies(
    redis_host: str,
    redis_port: int,
    redis_db: int
) -> None:
    """
    Generate and store checksums in Redis for all companies.
    
    Args:
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
    
    # Get all companies
    companies = await loader.get_all_companies()
    
    if not companies:
        logger.error("No se encontraron empresas para procesar")
        sys.exit(1)
    
    logger.info(f"Iniciando procesamiento de {len(companies)} empresas")
    
    for company_id in companies:
        logger.info(f"Procesando empresa: {company_id}")
        await generate_checksums_for_company(
            company_id,
            redis_host,
            redis_port,
            redis_db
        )
    
    logger.info("Procesamiento de todas las empresas completado")


async def main():
    parser = argparse.ArgumentParser(
        description="Load checksums from BigQuery to Redis"
    )
    parser.add_argument(
        "--company-id",
        help="Company ID (no requerido si se usa --all-companies)"
    )
    parser.add_argument(
        "--all-companies",
        action="store_true",
        help="Procesar todas las empresas"
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
    
    if not args.company_id and not args.all_companies:
        parser.error("Se debe especificar --company-id o --all-companies")
    
    try:
        if args.all_companies:
            await generate_checksums_for_all_companies(
                args.redis_host,
                args.redis_port,
                args.redis_db
            )
        else:
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