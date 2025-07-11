import base64
import json
import logging
from fastapi import APIRouter, HTTPException, Request
from src.mosaic import Mosaic
from src.llm_client import LLMClient
from src.pubsub import publish_response

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BANKS_WHITELIST = ["bbva", "bbvaempresas", 'santander']

router = APIRouter()
# Mosaic will be initialized with Redis client from request state
mosaic = None

duplicates_llm_client = LLMClient(
    app_name="duplicates",
    default_host=(
        "https://transactions-duplicate-agent-g3mwhumdcq-ue.a.run.app"
    )
)


@router.post("/duplicates", tags=["Duplicates Service"])
async def process_transaction_endpoint(request: Request):
    """
    Process a transaction message:
    1. Decode and parse the transaction data.
    2. Use Mosaic to check for duplicates.
    3. If it's a duplicate, use an LLM to classify it.
    4. If it's an update, delete the original checksum.
    5. Publish results to Pub/Sub and return the final status.
    """
    logger.info("Received request for /duplicates endpoint.")
    try:
        # Get Redis client from app state
        redis_client = request.app.state.redis_client
        if not redis_client:
            raise HTTPException(
                status_code=500,
                detail="Redis client not available"
            )
        
        # Initialize Mosaic with Redis client
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
            print(transaction_dict)
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
        if transaction_dict["bank"] not in BANKS_WHITELIST:
            logger.info(
                f"Bank {transaction_dict['bank']} not in whitelist. "
                f"Skipping processing."
            )
            return {
                "status": "skipped_bank_not_whitelisted",
                "details": (
                    f"Bank {transaction_dict['bank']} not supported"
                )
            }
        
        logger.info(
            f"Processing tx: {transaction_dict.get('checksum', 'N/A')}"
        )
        
        mosaic_result = await mosaic.process_transaction(transaction_dict)
        logger.info(f"Mosaic processing result: {mosaic_result}")

        if mosaic_result.get("is_duplicate"):
            logger.info("Duplicate detected by Mosaic. Sending to Pub/Sub.")
            
            checksum_new = transaction_dict.get("checksum", "")
            # The key that caused the collision is the one we generated
            colliding_key = mosaic_result.get("generated_checksum")
            conflicting_checksums = mosaic_result.get("conflicting_checksums", [])
            
            # For simplicity, we take the first checksum from the conflicting array
            # as the "old" one for logging and analysis.
            checksum_old = "N/A"
            if conflicting_checksums and isinstance(conflicting_checksums, list):
                if (conflicting_checksums[0] and
                        isinstance(conflicting_checksums[0], dict)):
                    checksum_old = conflicting_checksums[0].get("checksum", "N/A")

            # Send conflict to Pub/Sub for evaluation
            pubsub_data = {
                "checksum_old": checksum_old,
                "checksum_new": checksum_new,
                "account_number": transaction_dict["account_number"],
                "bank": transaction_dict["bank"],
                "company_id": transaction_dict["company_id"],
                "date": transaction_dict.get("transaction_date", "")
            }
            
            logger.info(
                f"Sending duplicate conflict to Pub/Sub: {pubsub_data}"
            )
            publish_response(pubsub_data, "duplicate-transactions")

            # Return duplicate detected without LLM analysis for now
            return {
                "status": "duplicate_detected",
                "details": mosaic_result,
                "pubsub_sent": True
            }
            
            '''
            # LLM analysis commented out for now
            message_for_llm = (
                f"Analyze potential duplicate: new_tx_id='{checksum_new}', "
                f"conflicts_with_tx_id='{checksum_old}'. "
                f"Bank: {transaction_dict['bank']}, "
                f"Company: {transaction_dict['company_id']}. "
                "Is this a transaction update/rectification?"
            )
            print(message_for_llm)
            
            llm_result = await duplicates_llm_client.analyze_message(
                message=message_for_llm
            )
            llm_result = {
                "classification": "update"
            }
            logger.info(f"LLM analysis result: {llm_result}")
            
            
            # --- New Logic: Delete if LLM confirms it's an update ---
            # We assume the LLM classifies updates with "update"
            if llm_result.get("classification") == "update":
                logger.info(
                    "LLM classified as 'update'. "
                    "Deleting original checksum key."
                )
                delete_success = await mosaic.delete_checksum(colliding_key)
                logger.info(
                    f"Deletion of key {colliding_key} was "
                    f"{'successful' if delete_success else 'unsuccessful'}."
                )
                
                # Publish detailed result to Pub/Sub
                pubsub_data = {
                    "status": "update_processed",
                    "company_id": transaction_dict['company_id'],
                    "checksum_new": checksum_new,
                    "checksum_old_deleted": checksum_old,
                    "llm_classification": llm_result,
                    "detection_timestamp_utc": (
                        datetime.datetime.utcnow().isoformat()
                    )
                }
                #publish_response(pubsub_data, "duplicate-transactions")

                return {
                    "status": "update_processed",
                    "details": "LLM identified as update, "
                               "original checksum deleted.",
                    "llm_result": llm_result,
                    "deleted_key": colliding_key
                }

            # If not an update, treat as a regular duplicate
            pubsub_data = {
                "status": "duplicate_detected",
                "company_id": transaction_dict['company_id'],
                "checksum_new": checksum_new,
                "checksum_old": checksum_old,
                "llm_classification": llm_result,
                "detection_timestamp_utc": (
                    datetime.datetime.utcnow().isoformat()
                )
            }
            publish_response(pubsub_data, "duplicate-transactions")

            return {
                "status": "duplicate_detected",
                "details": mosaic_result,
                "llm_result": llm_result
            }
            '''
        
        elif mosaic_result.get("error"):
            logger.warning(f"Mosaic error: {mosaic_result.get('error')}")
            return {
                "status": "processing_error",
                "details": mosaic_result
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