import base64
import json
import time
import logging # Added for more detailed logging
from fastapi import APIRouter, HTTPException, Request
from src.mosaic import Mosaic
from src.llm_client import LLMClient # Assuming this is used or will be used later
from src.pubsub import publish_response # Assuming this is your Pub/Sub publishing function

# Configure logger for this module if not configured globally
logger = logging.getLogger(__name__)
# Example of basic logging config if not set elsewhere, adjust as needed:
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

router = APIRouter()
mosaic = Mosaic() # Initialize Mosaic instance

# Initialize LLM client (as in your original code)
# This client is not used in the refactored duplicate handling part below,
# but kept if you intend to use it later.
duplicates_llm_client = LLMClient(
    app_name="duplicates",
    default_host=(
        "https://transactions-duplicate-agent-g3mwhumdcq-ue.a.run.app"
    )
)

@router.post("/duplicates", tags=["Duplicates Service"]) # Added a tag for OpenAPI docs
async def process_transaction_endpoint(request: Request): # Renamed for clarity
    """
    Process a transaction message:
    1. Decode and parse the transaction data.
    2. Use Mosaic service to check for exact duplicates and recurring patterns.
    3. If an exact duplicate is found, prepare and publish a message to Pub/Sub.
    4. Return the processing result.
    """
    logger.info("Received request for /duplicates endpoint.")
    try:
        body = await request.json()
        
        # Handle different possible message structures for 'data' field
        if "message" in body and "data" in body["message"]:
            encoded_data_field = body["message"]["data"]
        elif "data" in body:
            encoded_data_field = body["data"]
        else:
            logger.warning("Invalid message format received. 'data' or 'message.data' field missing.")
            raise HTTPException(
                status_code=400,
                detail="Invalid message format. Expected 'data' or 'message.data' field containing Base64 encoded JSON."
            )
        
        # Decode base64 data
        try:
            decoded_message_bytes = base64.b64decode(encoded_data_field)
        except base64.binascii.Error as b64_err:
            logger.error(f"Base64 decoding error: {str(b64_err)}")
            raise HTTPException(status_code=400, detail=f"Invalid Base64 encoding in data field: {str(b64_err)}")
        
        decoded_message_str = decoded_message_bytes.decode("utf-8").strip()

        # Parse JSON message to get the transaction dictionary
        try:
            # This is the actual transaction from the incoming request
            current_transaction = json.loads(decoded_message_str)
        except json.JSONDecodeError as json_err:
            logger.error(f"JSON decoding error for message content: {str(json_err)}")
            raise HTTPException(status_code=400, detail=f"Invalid JSON format in decoded message data: {str(json_err)}")
        

        current_transaction["transaction_date"] = "2025-05-20"
        current_transaction["checksum"] = "681fea7810dc61776c21fa6c"
        current_transaction["concept"] = "15 FACTURA ART"
        current_transaction["amount"] = -10000000
        current_transaction["account_number"] = "653180003810259331"
        current_transaction["bank"] = "unalanapay"
        current_transaction["company_id"] = "ccee6737-6e3b-40ce-b7a0-016ec8e5d3c3"
        current_transaction["metadata"] = {"origin": "syncfy"}

        logger.info(f"Processing transaction with ID/Checksum: {current_transaction.get('checksum', 'N/A')}")

        # --- Core Logic: Process the transaction using Mosaic ---
        # This single call handles checksum generation, Redis checks (exact and pattern),
        # and adding new entries to Redis if it's not an exact duplicate.
        mosaic_processing_result = await mosaic.process_transaction(current_transaction)
        
        logger.info(f"Mosaic processing result: {mosaic_processing_result}")

        # Check if Mosaic identified it as an exact duplicate
        if mosaic_processing_result.get("is_duplicate"):
            logger.info("Exact duplicate detected by Mosaic.")
            
            # Prepare data for Pub/Sub notification about the duplicate
            # "checksum_new" is the identifier from the currently processed transaction.
            # "checksum_old" is the identifier of the original transaction it conflicted with.
            
            checksum_new_for_pubsub = current_transaction.get("checksum", "") 
            checksum_old_for_pubsub = mosaic_processing_result.get("conflicting_checksum")
            
            pubsub_data_for_duplicate = {
                "company_id": current_transaction.get("company_id"),
                "account_number": current_transaction.get("account_number"),
                "checksum_new": checksum_new_for_pubsub, 
                "checksum_old": checksum_old_for_pubsub, 
                "bank": current_transaction.get("bank"),
                "date": time.strftime("%Y-%m-%d")
                
            }
            
            logger.info(f"Preparing to publish duplicate data to Pub/Sub topic 'duplicate-transactions': {pubsub_data_for_duplicate}")
            
            # Publish to Pub/Sub
            # IMPORTANT: If publish_response is a blocking synchronous function,
            # consider running it in a thread to avoid blocking the FastAPI event loop:
            # await asyncio.to_thread(publish_response, pubsub_data_for_duplicate, "duplicate-transactions")
            # If it's already async, then: await publish_response(...)
            publish_response(pubsub_data_for_duplicate, "duplicate-transactions") 
            logger.info("Duplicate data published to Pub/Sub.")

            # The LLM analysis part from your original code is kept here, commented out.
            # You would use 'current_transaction' and 'checksum_old_for_pubsub'.
            '''
            message_for_llm = (
                f"Analyze potential duplicate: new_tx_id='{checksum_new_for_pubsub}', "
                f"conflicts_with_tx_id='{checksum_old_for_pubsub}'. "
                f"Bank: {current_transaction.get('bank')}, "
                f"Company: {current_transaction.get('company_id')}, "
                f"Account: {current_transaction.get('account_number')}. "
                "Is this an update or truly different transactions?"
            )
            llm_result = await duplicates_llm_client.analyze_message(message=message_for_llm)
            logger.info(f"LLM analysis result: {llm_result}")
            
            pubsub_data_llm = {
                "company_id": current_transaction.get("company_id"),
                "account_number": current_transaction.get("account_number"),
                "checksum_new": checksum_new_for_pubsub,
                "checksum_old": checksum_old_for_pubsub, # Changed from checksum_silver for consistency
                "llm_classification": llm_result.get("classification", "unknown"),
                "llm_reason": llm_result.get("reason", ""),
                "detection_timestamp_utc": datetime.datetime.utcnow().isoformat() + "Z",
            }
            publish_response(pubsub_data_llm, "llm-transactions")
            logger.info("LLM analysis data published to Pub/Sub.")
            '''
            
            # The API response for a detected duplicate should include details from Mosaic's processing.
            # No need to call mosaic.add_checksum() again; process_transaction handles this internally.
            return {
                "status": "duplicate_detected",
                "details": mosaic_processing_result, # Contains all relevant flags and checksums
                "processed_transaction_id": current_transaction.get("checksum", "") # ID of the transaction just processed
            }
        
        elif mosaic_processing_result.get("error"):
            logger.warning(f"Mosaic processing returned an error: {mosaic_processing_result.get('error')}")
            # Depending on the error, you might still return parts of the result or a specific error response
            return {
                "status": "processing_error_from_mosaic",
                "details": mosaic_processing_result
            }
            
        else: # Not an exact duplicate and no error from Mosaic
            logger.info("Transaction processed by Mosaic: Not an exact duplicate.")
            # `mosaic.process_transaction` would have added the new checksum to Redis
            # and incremented the concept count for the pattern history.
            return {
                "status": "processed", # Or more descriptive like "new_transaction_recorded"
                "details": mosaic_processing_result
            }
            
    except HTTPException as http_exc:
        # Re-raise HTTPException to let FastAPI handle it
        raise http_exc
    except Exception as e:
        # Catch any other unexpected errors during request handling or processing
        logger.error(f"Unexpected error in /duplicates endpoint: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"An unexpected server error occurred: {str(e)}"
        )