import base64
import json
import time
from fastapi import APIRouter, HTTPException, Request
from ..models.transaction_message import TransactionMessage
from ..lib.duplicate_detector import DuplicateDetector
from ..lib.similarity import TransactionUpdateDetector
from ..lib.llm_client import LLMClient
from ..lib.pubsub import publish_response


router = APIRouter()
duplicate_detector = DuplicateDetector()
update_detector = TransactionUpdateDetector(project_id="production-400914")

# Initialize LLM clients for different analyses
duplicates_llm_client = LLMClient(
    app_name="duplicates",
    default_host=(
        "https://transactions-duplicate-agent-g3mwhumdcq-ue.a.run.app"
    )
)

updates_llm_client = LLMClient(
    app_name="updates",
    default_host=(
        "http://localhost:9000"
    )
)


@router.post("/duplicates")
async def process_transaction(request: Request):
    """
    Process transaction message and check for duplicates.
    """
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
        json_message = json.loads(decoded_message)
        
        # Parse message using Pydantic model
        transaction = TransactionMessage(**json_message)
        
        # Test data
        
        # Test data for transaction
        # transaction.checksum = "80b314ab-a7d6-4db6-9044-a5c2a57ea910"
        # transaction.concept = (
        #     "COMISIÓN DIARIA PAGO SPEI FINANCIADO - 8575 - "
        #     "Paulina Martinez Zavala"
        # )
        # transaction.amount = -458213
        # transaction.account_number = "Clara" 
        # transaction.bank = "clara"
        # transaction.transaction_date = "2025-04-05"
        # transaction.company_id = "6eb86fe6-dc58-4e4e-ae6e-4d676dcb6c51"
        
        # Check for duplicates
        result = await duplicate_detector.check_duplicate(
            transaction.model_dump()
        )
        
        # If duplicate found, consult LLM
        if result.get("is_duplicate", False):
            # Prepare message for LLM
            message = (
                "Analyze these transaction checksums for potential "
                f"duplicates: {result['checksum']} and "
                f"{result['conflicting_checksum']}. Bank: {transaction.bank}, "
                f"Company ID: {transaction.company_id}, Account: "
                f"{transaction.account_number}. Determine if this is a "
                "transaction update or different transactions."
            )
            
            llm_result = await duplicates_llm_client.analyze_message(
                message=message,
            )
            
            print("LLM result:")
            print(llm_result)
            # Prepare data for Pub/Sub
            pubsub_data = {
                "company_id": transaction.company_id,
                "account_number": transaction.account_number,
                "checksum_new": result["checksum"],
                "checksum_silver": result["conflicting_checksum"],
                "type": llm_result.get("classification", "unknown"),
                "reason": llm_result.get("reason", ""),
                "date": time.strftime("%Y-%m-%d"),
            }
            

            # Publish to Pub/Sub
            publish_response(pubsub_data, "llm-transactions")
            
            result["llm_analysis"] = llm_result
            return result
            
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error processing transaction: {str(e)}"
        )




#############################################
#                                           #
#           Duplicates Route                #
#                                           #
#############################################

@router.post("/updates")
async def process_transaction_update(request: Request):
    """
    Process transaction message and check for potential updates.
    """
    try:
        print("Processing transaction update")
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
        print(decoded_message)
        
        # Parse JSON message
        json_message = json.loads(decoded_message)
        
        # Parse message using Pydantic model
        transaction = TransactionMessage(**json_message)
        transaction.checksum = "0a2d7a0d6edd92f4804502fa901f8b7f8843a449"
        transaction.concept = (
            "CHEQUE PAGADO NO. / CH-0003473 PAGO EN EFECTIVO"
        )
        transaction.amount = -116480
        transaction.account_number = "0156057799"
        transaction.bank = "bbva"
        transaction.transaction_date = "2025-03-22"
        transaction.company_id = "11ea7663-07a9-439c-9026-9c5ab2f9f85f"
        
        # Check for potential updates
        result = update_detector.detect_updates(
            transaction=transaction.model_dump(),
            transaction_date=transaction.transaction_date,
            account_number=transaction.account_number,
            company_id=transaction.company_id,
            bank=transaction.bank
        )
        
        if result.empty:
            return {"is_update": False}
            
        # Convert result to dict for response
        update_info = result.iloc[0].to_dict()
        update_info["is_update"] = True

        # Prepare message for LLM
        message = (
            "Analyze these transaction checksums for potential updates: "
            f"{update_info['checksum_new']} and {update_info['checksum_silver']}. "
            f"Bank: {transaction.bank}, Company ID: {transaction.company_id}, "
            f"Account: {transaction.account_number}."
            f"Similarity metrics: {update_info["levenshtein_distance"]}, "
            f"{update_info["cosine_similarity"]}, "
            f"{update_info["jaro_winkler_similarity"]}. "
            f"Determine if this is a transaction update or different transactions"
           
        )
 
        
        # Get LLM analysis
        llm_result = await updates_llm_client.analyze_message(
            message=message
        )
        
        print("Mensaje enviado al LLM:")
        print(message)
        print("Respuesta del LLM:")
        print(llm_result)
        
        
        return update_info
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error processing transaction update: {str(e)}"
        )

