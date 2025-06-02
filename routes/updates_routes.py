# routes/updates_routes.py

import base64
import json
import logging
import time
import sys
from fastapi import APIRouter, HTTPException, Request
from src.pubsub import publish_response
from src.transaction_update_detector import TransactionUpdateDetectorRedis

# Configure logging for Cloud Run
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout  # Use stdout instead of stderr for Cloud Run
)

router = APIRouter()
logger = logging.getLogger(__name__)

# LLMClient remains as is
try:
    from src.llm_client import LLMClient
    updates_llm_client = LLMClient(
        app_name="updates",
        default_host=(
            "https://transactions-update-agent-192085613711.us-east1.run.app"
            #"http://localhost:9000"
        )
    )
except ImportError:
    updates_llm_client = None
    logging.warning("LLMClient could not be imported or initialized.")

def filter_exact_matches(transactions: list) -> list:
    """
    Filter out transactions that are exact matches based on similarity metrics.
    Exact matches are defined as:
    - levenshtein_distance = 0
    - cosine_similarity = 1
    - jaro_winkler_similarity = 1
    """
    original_count = len(transactions)
    filtered_transactions = [
        transaction for transaction in transactions
        if not (
            transaction["metrics"]["levenshtein_distance"] == 0
            and transaction["metrics"]["cosine_similarity"] == 1
            and transaction["metrics"]["jaro_winkler_similarity"] == 1
        )
    ]
    discarded_count = original_count - len(filtered_transactions)
    if discarded_count > 0:
        logger.info(
            f"Discarded {discarded_count} transactions for being exact matches"
        )
    return filtered_transactions

@router.post("/updates")
async def process_transaction_update(request: Request):
    update_detector: TransactionUpdateDetectorRedis | None = (
        request.app.state.update_detector  # type: ignore
    )
    
    if not update_detector:
        logger.error(
            "Update detector is not available (app.state.update_detector is None)"
        )
        raise HTTPException(
            status_code=503,
            detail="Update service temporarily unavailable"
        )

    decoded_message_str = ""  # For early error logging
    data_b64_content = ""
    try:
        logger.info("Processing /updates request")
        body = await request.json()
        
        data_b64_content = (
            body.get("message", {}).get("data")
            if "message" in body
            else body.get("data")
        )

        if not data_b64_content:
            raise HTTPException(
                status_code=400,
                detail="Invalid message format. Expected 'data' or 'message.data'"
            )
        
        decoded_message_str = base64.b64decode(
            data_b64_content
        ).decode("utf-8").strip()
        logger.debug("Decoded message: %s", decoded_message_str)
        
        # Work directly with the dictionary parsed from JSON
        transaction_dict = json.loads(decoded_message_str)
        logger.debug("Transaction dictionary to process: %s", transaction_dict)
        
        updated_transactions = await update_detector.detect_updates(
            new_transaction=transaction_dict 
        )

        print(f"transaccion detectada: {updated_transactions}")
        # If no updates were found, return early
        if not updated_transactions:
            return {
                "processed_checksum": transaction_dict.get("checksum", "N/A"),
                "updates": []
            }
        
        # Filter exact transactions before sending to Pub/Sub
        updated_transactions = filter_exact_matches(updated_transactions)
    
        
        # Initialize llm_results list
        llm_results = []
        
        # If there are updates, send to Pub/Sub
        if updated_transactions:
            for update in updated_transactions:
                # First send to simility-transactions
                pubsub_data = {
                    "original_checksum": update["original_checksum"],
                    "new_checksum": update["new_checksum"],
                    "levenshtein_distance": update["metrics"]["levenshtein_distance"],
                    "cosine_similarity": update["metrics"]["cosine_similarity"],
                    "jaro_winkler_similarity": update["metrics"]["jaro_winkler_similarity"],
                    "account_number": transaction_dict["account_number"],
                    "bank": transaction_dict["bank"],
                    "company_id": transaction_dict["company_id"],
                    "date": time.strftime("%Y-%m-%d")
                }
                
                logger.info(
                    "Sending update to Pub/Sub simility-transactions: %s",
                    pubsub_data
                )
                publish_response(pubsub_data, "simility-transactions")
                
                # Then call LLM
                message_for_llm = (
                    f"checksum_new: {update['new_checksum']}, "
                    f"checksum_old: {update['original_checksum']}, "
                    f"account_number: {transaction_dict['account_number']}, "
                    f"bank: {transaction_dict['bank']}, "
                    f"company_id: {transaction_dict['company_id']}"
                )
                #message_for_llm = "checksum_new: b39d283eddabee9635bef4686816dc11be63bfe8, checksum_old: f426d93f71288a6b35cc7bd8d3d0ebe40579808b, account_number: 69565360201, bank: bajio, company_id: 11ea7663-07a9-439c-9026-9c5ab2f9f85f"
                
                try:
                    llm_result = await updates_llm_client.analyze_message(
                        message=message_for_llm
                    )
                    logger.info("LLM analysis result: %s", llm_result)
                    
                    # Store LLM result for response
                    llm_results.append({
                        "original_checksum": update["original_checksum"],
                        "new_checksum": update["new_checksum"],
                        "classification": llm_result["classification"],
                        "reason": llm_result["reason"]
                    })
                    
                    # Send LLM result to llm-simility-responses
                    llm_pubsub_data = {
                        "original_checksum": update["original_checksum"],
                        "new_checksum": update["new_checksum"],
                        "classification": llm_result["classification"],
                        "reason": llm_result["reason"],
                        "company_id": transaction_dict["company_id"],
                        "bank": transaction_dict["bank"],
                        "account_number": transaction_dict["account_number"],
                        "date": time.strftime("%Y-%m-%d")
                    }
                    
                    logger.info(
                        "Sending LLM result to Pub/Sub llm-simility-responses: %s",
                        llm_pubsub_data
                    )
                    publish_response(llm_pubsub_data, "llm-simility-responses")
                    
                except Exception as e:
                    logger.error("Error calling LLM: %s", str(e))

        return {
            "processed_checksum": transaction_dict.get("checksum", "N/A"),
            "updates": updated_transactions,
            "llm_analysis": llm_results
        }

    except json.JSONDecodeError as e:
        logger.error(
            "JSONDecodeError: %s. Decoded payload: %s",
            e,
            decoded_message_str,
            exc_info=True
        )
        raise HTTPException(
            status_code=400,
            detail=f"Error decoding JSON: {str(e)}"
        )
    except base64.binascii.Error as e:
        logger.error(
            "Base64DecodeError: %s. Base64 content: %s",
            e,
            data_b64_content,
            exc_info=True
        )
        raise HTTPException(
            status_code=400,
            detail=f"Error decoding base64: {str(e)}"
        )
    except Exception as e:
        logger.error(
            "Unexpected error processing update: %s",
            e,
            exc_info=True
        )
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )