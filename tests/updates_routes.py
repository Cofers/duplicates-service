import base64
import json
from fastapi import APIRouter, HTTPException, Request

from src.transaction_update_detector import TransactionUpdateDetectorRedis
from src.lib.llm_client import LLMClient
from redis import asyncio as redis_async
import logging

redis_client_global: redis_async.Redis | None = None
update_detector_instance: TransactionUpdateDetectorRedis | None = None
router = APIRouter()

# Initialize LLM client for updates analysis
updates_llm_client = LLMClient(
    app_name="updates",
    default_host=(
        "http://localhost:9000"
    )
)


@router.on_event("startup")
async def startup_event():
    global redis_client_global, update_detector_instance
    # (Misma lógica de startup que en la Opción 1)
    # ...
    # Ejemplo simplificado:
    redis_url = "redis://localhost" # Cambia esto por tu URL real o config
    try:
        redis_client_global = redis_async.from_url(redis_url, decode_responses=False)
        await redis_client_global.ping()
        update_detector_instance = TransactionUpdateDetectorRedis(redis_client=redis_client_global)
        logging.info("TransactionUpdateDetectorRedis initialized successfully.")
    except Exception as e:
        logging.error(f"Failed to initialize Redis or TransactionUpdateDetectorRedis: {e}", exc_info=True)
        redis_client_global = None
        update_detector_instance = None

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
        print(result)
        return(result)
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