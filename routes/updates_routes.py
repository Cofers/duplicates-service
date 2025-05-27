# routes/updates_routes.py

import base64
import json
import logging
import time
from fastapi import APIRouter, HTTPException, Request
from src.pubsub import publish_response
# from typing import Any # Ya no es necesario para el modelo Pydantic aquí

# Importa tu clase TransactionUpdateDetectorRedis desde su ubicación correcta
try:
    from src.transaction_update_detector import TransactionUpdateDetectorRedis # Ajusta si tu ruta es diferente
except ImportError:
    logging.error("FALLO AL IMPORTAR TransactionUpdateDetectorRedis desde src.transaction_update_detector")
    class TransactionUpdateDetectorRedis: pass # Placeholder

router = APIRouter()
logger = logging.getLogger(__name__)

# El LLMClient se mantiene como lo tenías.
# try:
#     from src.lib.llm_client import LLMClient
#     updates_llm_client = LLMClient(...)
# except ImportError:
#     updates_llm_client = None
#     logging.warning("LLMClient no se pudo importar o inicializar.")


@router.post("/updates")
async def process_transaction_update(request: Request):
    update_detector: TransactionUpdateDetectorRedis | None = request.app.state.update_detector # type: ignore
    
    if not update_detector:
        logger.error("El detector de actualizaciones no está disponible (app.state.update_detector es None).")
        raise HTTPException(status_code=503, 
                            detail="Servicio de actualizaciones no disponible temporalmente (detector no inicializado).")

    decoded_message_str = "" # Para el logging en caso de error temprano
    data_b64_content = ""
    try:
        logger.info("Procesando solicitud /updates")
        body = await request.json()
        
        data_b64_content = body.get("message", {}).get("data") if "message" in body else body.get("data")

        if not data_b64_content:
            raise HTTPException(
                status_code=400,
                detail="Formato de mensaje inválido. Se esperaba 'data' (string base64) o 'message.data' (string base64)."
            )
        
        decoded_message_str = base64.b64decode(data_b64_content).decode("utf-8").strip()
        logger.debug(f"Mensaje decodificado: {decoded_message_str}")
        
        # Trabajar directamente con el diccionario parseado desde JSON
        transaction_dict = json.loads(decoded_message_str)
        logger.debug(f"Diccionario de transacción para procesar: {transaction_dict}")
        '''
        transaction_dict["transaction_date"] = "2025-05-26"
        transaction_dict["checksum"] = "489d5480655a88026dad3bed01394bea6f2d2bccc"
        transaction_dict["etl_checksum"] = "3e6bc8e597f676f7b24aa2f860974c64"
        transaction_dict["concept"] = "Anticipo OP 8805 / CGO TRANS ELEC ismaelssssss"
        transaction_dict["amount"] = -1040601
        transaction_dict["account_number"] = "65506535234"
        transaction_dict["bank"] = "santander"
        transaction_dict["account_alias"] = ""
        transaction_dict["currency"] = "MXN"
        transaction_dict["report_type"] = "3 Months - Extension CSV"
        transaction_dict["extraction_date"] = "2025-05-26 18:40:31.000000 UTC"
        transaction_dict["user_id"] = "81616deb-1bfe-4645-97db-2573cb9eb09b"
        transaction_dict["company_id"] = "3d52c627-498e-4e74-9dcd-f2af4953bb23"
        transaction_dict["reported_remaining"] = 5915374
        transaction_dict["created_at"] = "2025-05-26 00:00:00.000000 UTC"
        transaction_dict["metadata"] = [{
            "key": "origin",
            "value": "extension"
        }, {
            "key": "sucursal",
            "value": "0981"
        }, {
            "key": "description",
            "value": "CGO TRANS ELEC"
        }, {
            "key": "referencia",
            "value": "350241N742"
        }]
        '''
        
        updated_transactions = await update_detector.detect_updates(
            new_transaction=transaction_dict 
        )
        
        
        # Si hay actualizaciones, enviar a Pub/Sub
        if updated_transactions:
            for update in updated_transactions:
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
                
                logger.info(f"Enviando actualización a Pub/Sub: {pubsub_data}")
                publish_response(pubsub_data, "simility-transactions")
                logger.info("Actualización enviada a Pub/Sub")
        
        return {
            "processed_checksum": transaction_dict.get("checksum", "N/A"),
            "updates": updated_transactions
        }

    except json.JSONDecodeError as e:
        logger.error(f"JSONDecodeError: {e}. Payload decodificado: {decoded_message_str}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Error decodificando JSON: {str(e)}")
    except base64.binascii.Error as e:
        logger.error(f"Base64DecodeError: {e}. Contenido base64: {data_b64_content}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Error decodificando base64: {str(e)}")
    except Exception as e:
        logger.error(f"Error inesperado procesando actualización: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error interno del servidor: {str(e)}")