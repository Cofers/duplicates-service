import base64
import json
import logging
from fastapi import APIRouter, HTTPException, Request
from src.mosaic import Mosaic
from src.llm_client import LLMClient
from src.pubsub import publish_response
from datetime import datetime # Import datetime for timestamp if needed, though not used in the final pubsub_data for now

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BANKS_WHITELIST = ["bbva", "bbvaempresas", 'santander']

router = APIRouter()
# Mosaic will be initialized with Redis client from request state
# No es necesario inicializar mosaic aquí, se hace dentro del endpoint
# mosaic = None 

duplicates_llm_client = LLMClient(
    app_name="duplicates",
    default_host=(
        "https://transactions-duplicate-agent-g3mwhumdcq-ue.a.run.app"
    )
)

# Mapeo de razones de Mosaic a tipos de conflicto para Pub/Sub
CONFLICT_TYPE_MAP = {
    "exact_checksum_match_same_day": "EXACT",
    "similar_match_same_day": "CONCEPT_IMPORT",
    "cross_day_rectification": "DATE",
    "processing_error": "ERROR",
    "missing_fields": "ERROR" # O "MISSING_DATA" si prefieres ser más específico
}


@router.post("/analyze", tags=["Duplicates Service"])
async def process_transaction_endpoint(request: Request):
    """
    Process a transaction message:
    1. Decode and parse the transaction data.
    2. Use Mosaic to check for duplicates.
    3. If it's a duplicate, publish results to Pub/Sub with conflict type.
    4. (LLM analysis and deletion logic is commented out as per previous request)
    5. Return the final status.
    """
    logger.info("Received request for /analyze endpoint.")
    try:
        # Get Redis client from app state
        redis_client = request.app.state.redis_client
        if not redis_client:
            raise HTTPException(
                status_code=500,
                detail="Redis client not available"
            )
        
        # Initialize Mosaic with Redis client for each request
        # This ensures Mosaic uses the correct async Redis client from app state
        mosaic = Mosaic(redis_client=redis_client)
        
        body = await request.json()
        
        if "message" in body and "data" in body["message"]:
            encoded_data_field = body["message"]["data"]
        elif "data" in body:
            encoded_data_field = body["data"]
        else:
            logger.warning("Invalid message format: 'data' field missing.")
            raise HTTPException(
                status_code=400,
                detail="Invalid format: 'data' or 'message.data' is required."
            )
        
        try:
            decoded_bytes = base64.b64decode(encoded_data_field)
            decoded_str = decoded_bytes.decode("utf-8").strip()
            transaction_dict = json.loads(decoded_str)
            logger.debug(f"Decoded transaction: {transaction_dict}") # Use debug for potentially large output
        except (base64.binascii.Error, json.JSONDecodeError) as e:
            logger.error(f"Decoding error: {e}")
            raise HTTPException(status_code=400, detail=f"Invalid data: {e}")
        
        # Debug log to check transaction type and content
        keys_info = (
            list(transaction_dict.keys())
            if isinstance(transaction_dict, dict)
            else 'Not a dict'
        )
        logger.info(
            f"Transaction type: {type(transaction_dict)}, "
            f"Transaction keys: {keys_info}"
        )
        
        # Validate bank is in whitelist
        if transaction_dict.get("bank") not in BANKS_WHITELIST:
            logger.info(
                f"Bank {transaction_dict.get('bank', 'N/A')} not in whitelist. "
                f"Skipping processing."
            )
            return {
                "status": "skipped_bank_not_whitelisted",
                "details": (
                    f"Bank {transaction_dict.get('bank', 'N/A')} not supported"
                )
            }
        
        # Get the provided checksum from the incoming transaction
        provided_checksum = transaction_dict.get("checksum", "N/A")
        logger.info(
            f"Processing tx with provided checksum: {provided_checksum}"
        )
        
        mosaic_result = await mosaic.process_transaction(transaction_dict)
        logger.info(f"Mosaic processing result: {mosaic_result}")

        if mosaic_result.get("is_duplicate"):
            logger.info("Duplicate detected by Mosaic. Sending to Pub/Sub.")
            
            # The new checksum is the one provided in the incoming transaction
            checksum_new = provided_checksum
            
            # conflicting_transactions now holds the list of full transaction objects
            conflicting_transactions = mosaic_result.get("conflicting_transactions", [])
            
            # For simplicity, we take the checksum of the first conflicting transaction as "checksum_old"
            checksum_old = "N/A"
            if conflicting_transactions and isinstance(conflicting_transactions, list):
                if conflicting_transactions[0] and isinstance(conflicting_transactions[0], dict):
                    checksum_old = conflicting_transactions[0].get("checksum", "N/A")

            # Determine the type of conflict based on Mosaic's reason
            mosaic_reason = mosaic_result.get("reason", "unknown_error")
            type_of_conflict = CONFLICT_TYPE_MAP.get(mosaic_reason, "UNKNOWN")

            # Prepare data for Pub/Sub
            pubsub_data = {
                "checksum_old": checksum_old, # Checksum of the first conflicting transaction
                "checksum_new": checksum_new, # Checksum of the newly processed transaction
                "account_number": transaction_dict.get("account_number", "N/A"),
                "bank": transaction_dict.get("bank", "N/A"),
                "company_id": transaction_dict.get("company_id", "N/A"),
                "date": transaction_dict.get("transaction_date", "N/A"), # Transaction date of the new transaction
                "type_of_conflict": type_of_conflict, # The specific type of duplicate
                "mosaic_reason": mosaic_reason, # Original reason from Mosaic for debugging/analysis
                "conflicting_transactions_details": conflicting_transactions # Full details of conflicting transactions
            }
            
            logger.info(
                f"Sending duplicate conflict to Pub/Sub: {pubsub_data}"
            )
            publish_response(pubsub_data, "analyze-transactions")

            # Return duplicate detected status
            return {
                "status": "duplicate_detected",
                "details": mosaic_result,
                "pubsub_sent": True
            }
            
        elif mosaic_result.get("error"):
            logger.warning(f"Mosaic error: {mosaic_result.get('error')}")
            # Determine error type for Pub/Sub if needed, or just log
            error_type = CONFLICT_TYPE_MAP.get(mosaic_result.get("reason", "processing_error"), "ERROR")
            
            # Optionally, send error details to Pub/Sub as well
            pubsub_data_error = {
                "checksum_new": provided_checksum,
                "account_number": transaction_dict.get("account_number", "N/A"),
                "bank": transaction_dict.get("bank", "N/A"),
                "company_id": transaction_dict.get("company_id", "N/A"),
                "date": transaction_dict.get("transaction_date", "N/A"),
                "type_of_conflict": error_type,
                "error_message": mosaic_result.get("error"),
                "mosaic_reason": mosaic_result.get("reason", "processing_error")
            }
            publish_response(pubsub_data_error, "duplicate-transactions-errors") # Consider a separate topic for errors
            
            return {
                "status": "processing_error",
                "details": mosaic_result,
                "pubsub_sent": True # Indicate that error details were sent
            }
            
        else:  # Not a duplicate
            logger.info("Transaction is new. Stored in Redis by Mosaic.")
            return {
                "status": "processed_as_new",
                "details": mosaic_result
            }
            
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.error(f"Unexpected error in endpoint: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))