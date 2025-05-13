import base64
import json
import time
from fastapi import APIRouter, HTTPException, Request
from ..lib.mosaic import Mosaic
from ..lib.llm_client import LLMClient
from ..lib.pubsub import publish_response


router = APIRouter()
mosaic = Mosaic()

# Initialize LLM client for duplicates analysis
duplicates_llm_client = LLMClient(
    app_name="duplicates",
    default_host=(
        "https://transactions-duplicate-agent-g3mwhumdcq-ue.a.run.app"
    )
)

@router.post("/duplicates")
async def process_transaction(request: Request):
    """
    Process transaction message and check for duplicates.
    """
    print("Processing transaction in duplicates route")
    try:
        # Get JSON body
        body = await request.json()
        
        # Handle both message formats
        if "message" in body and "data" in body["message"]:
            data = body["message"]["data"]
        elif "data" in body:
            data = body["data"]
        else:
            raise HTTPException(
                status_code=400,
                detail="Invalid message format. Expected 'data' or "
                       "'message.data'"
            )
        
        # Decode base64 data
        decoded_message = base64.b64decode(data).decode("utf-8").strip()

        # Parse JSON message
        transaction = json.loads(decoded_message)
        
        # Test data for transaction
        transaction = {
            "checksum": "peneismaello24",
            "concept": "PAGO NOM",
            "amount": -112114,
            "account_number": "01720163924114",
            "bank": "bancoazteca",
            "transaction_date": "2025-05-09",
            "company_id": "6eb86fe6-dc58-4e4e-ae6e-4d676dcb6c51",
            "metadata": {
                "origin": "plugin"                
            }
        }
        
        # Generate checksum
        checksum = mosaic.generate_checksum(transaction)    
        
        # Check if exists
        exists = await mosaic.exists_checksum(
            checksum,
            transaction["company_id"],
            transaction["bank"],
            transaction["account_number"]
        )
        
        if exists:
            # Get original checksum
            original_checksum = await mosaic.get_original_checksum(
                checksum,
                transaction["company_id"],
                transaction["bank"],
                transaction["account_number"]
            )
            
            # Prepare message for LLM
            message = (
                "Analyze these transaction checksums for potential "
                f"duplicates: {transaction.get('checksum', '')} and "
                f"{original_checksum}. "
                f"Bank: {transaction['bank']}, "
                f"Company ID: {transaction['company_id']}, "
                f"Account: {transaction['account_number']}. "
                "Determine if this is a transaction update or "
                "different transactions."
            )
            print("Message:")
            print(message)
            
            llm_result = await duplicates_llm_client.analyze_message(
                message=message,
            )
            
            print("LLM result:")
            print(llm_result)
            
            # Prepare data for Pub/Sub
            pubsub_data = {
                "company_id": transaction["company_id"],
                "account_number": transaction["account_number"],
                "checksum_new": transaction.get("checksum", ""),
                "checksum_silver": original_checksum,
                "type": llm_result.get("classification", "unknown"),
                "reason": llm_result.get("reason", ""),
                "date": time.strftime("%Y-%m-%d"),
            }
            
            # Publish to Pub/Sub
            publish_response(pubsub_data, "llm-transactions")
            success = await mosaic.add_checksum(
                checksum,
                transaction.get("checksum", ""),
                transaction["company_id"],
                transaction["bank"],
                transaction["account_number"]
            )
            
            return {
                "is_duplicate": True,
                "checksum": transaction.get("checksum", ""),
                "conflicting_checksum": original_checksum,
                "llm_analysis": llm_result
            }
        
        # If doesn't exist, add it
        success = await mosaic.add_checksum(
            checksum,
            transaction.get("checksum", ""),
            transaction["company_id"],
            transaction["bank"],
            transaction["account_number"]
        )
        
        
        return {
            "is_duplicate": False,
            "checksum": checksum
        }
            
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error processing transaction: {str(e)}"
        ) 