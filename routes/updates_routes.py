import base64
import json
from fastapi import APIRouter, HTTPException, Request
#from src.models.transaction_message import TransactionMessage
from src.lib.similarity import TransactionUpdateDetector
from src.lib.llm_client import LLMClient

router = APIRouter()
update_detector = TransactionUpdateDetector(project_id="production-400914")

# Initialize LLM client for updates analysis
updates_llm_client = LLMClient(
    app_name="updates",
    default_host=(
        "http://localhost:9000"
    )
)


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
        
        # Test data for transaction
        # transaction.checksum = "0a2d7a0d6edd92f4804502fa901f8b7f8843a449"
        # transaction.concept = (
        #     "CHEQUE PAGADO NO. / CH-0003473 PAGO EN EFECTIVO"
        # )
        # transaction.amount = -116480
        # transaction.account_number = "0156057799"
        # transaction.bank = "bbva"
        # transaction.transaction_date = "2025-03-22"
        # transaction.company_id = "11ea7663-07a9-439c-9026-9c5ab2f9f85f"
        
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
            f"{update_info['checksum_new']} and "
            f"{update_info['checksum_silver']}. "
            f"Bank: {transaction.bank}, Company ID: {transaction.company_id}, "
            f"Account: {transaction.account_number}. "
            f"Similarity metrics: {update_info['levenshtein_distance']}, "
            f"{update_info['cosine_similarity']}, "
            f"{update_info['jaro_winkler_similarity']}. "
            "Determine if this is a transaction update or different "
            "transactions"
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