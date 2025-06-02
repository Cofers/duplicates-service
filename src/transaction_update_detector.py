# transaction_update_detector.py

import re
from collections import Counter
import math
import time
from functools import wraps
import logging

from Levenshtein import distance as levenshtein_distance
import jellyfish
import json
import asyncio
import datetime

from redis import asyncio as redis_async_pkg


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def measure_time(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start = time.time()
        if asyncio.iscoroutinefunction(func):
            result = await func(*args, **kwargs)
        else:
            result = func(*args, **kwargs)
        total_time = time.time() - start
        logger.info(f"Execution time {func.__name__}: {total_time:.2f} seconds")
        return result
    return wrapper

class TransactionUpdateDetectorRedis:
    def __init__(self, redis_client: redis_async_pkg.Redis, redis_ttl_seconds: int = 7 * 24 * 60 * 60):
        self.redis_client = redis_client
        self.redis_data_ttl_seconds = redis_ttl_seconds
        self.levenshtein_threshold = 3
        self.cosine_threshold = 0.8
        self.jaro_winkler_threshold = 0.9
        # NUEVO: Umbral para considerar una actualización basada en la longitud del concepto
        # Podrías ajustar esto, pero >= 0 significa que el nuevo concepto debe ser igual o más largo.
        self.concept_length_growth_threshold = 1

    def _format_field_for_key(self, field_value: any) -> str:
        """Formatea un campo genérico para ser parte de la clave Redis."""
        if self.custom_is_na(field_value): # Aseguramos que NaN o None se traten consistentemente
            return "N/A"
        return str(field_value).strip().replace(":", "_") # Evitar ':' en partes de la clave

    def _serialize_metadata_for_key(self, metadata: any) -> str:
        if not metadata or not isinstance(metadata, list):
            return "N/A"

        serialized_parts = []
        try:
            for item in metadata:
                if isinstance(item, dict) and 'key' in item and 'value' in item:
                    key_str = str(item['key'])
                    value_str = str(item['value'])
                    serialized_parts.append(key_str + value_str)
                else:
                    logger.warning(
                        f"Metadata item does not have the expected format "
                        f"{{'key':k, 'value':v}}: {item}. Using 'invalid_metadata_item'."
                    )
                    return "invalid_metadata_structure"

            if not serialized_parts:
                return "N/A"

            return "".join(serialized_parts)
        except Exception as e:
            logger.error(
                f"Error during simple metadata serialization (type: {type(metadata)}): {e}",
                exc_info=True
            )
            return "serialization_error"

    def _generate_redis_key_for_candidates(
        self,
        company_id: str,
        bank: str,
        account_number: str,
        transaction_date_str: str,
        amount_str: str,
        metadata_serialized_str: str,
        reported_remaining_str: str # NUEVO: Añadido reported_remaining a la clave
    ) -> str:
        """Generates Redis key including serialized metadata and reported remaining."""
        return (
            f"tx_cand:{company_id}:{bank}:{account_number}:{transaction_date_str}"
            f":{amount_str}:{reported_remaining_str}:{metadata_serialized_str}" # NUEVO: reported_remaining_str en la clave
        )

    def custom_is_na(self, value: any) -> bool:
        return value is None or (isinstance(value, float) and math.isnan(value))

    async def _get_candidates_from_redis(self, redis_key: str) -> list[dict[str, str]] | None:
        try:
            if not await self.redis_client.exists(redis_key):
                logger.debug(f"Redis key {redis_key} does not exist.")
                return None
            json_cand_list_bytes = await self.redis_client.lrange(redis_key, 0, -1)
            if not json_cand_list_bytes:
                logger.debug(f"Redis key {redis_key} exists but stores an empty list.")
                return []
            candidates = [
                json.loads(cand_str_bytes.decode('utf-8'))
                for cand_str_bytes in json_cand_list_bytes
            ]
            return candidates
        except json.JSONDecodeError as e:
            logger.error(
                f"JSON decode error for Redis key {redis_key}. "
                f"Data might be corrupted: {e}",
                exc_info=True
            )
            return None
        except Exception as e:
            logger.error(
                f"Error getting candidates from Redis key {redis_key}: {e}",
                exc_info=True
            )
            return None

    async def _store_list_to_redis(self, redis_key: str, list_to_store: list[dict[str, str]]):
        try:
            pipe = self.redis_client.pipeline()
            pipe.delete(redis_key)
            if list_to_store:
                for item_dict in list_to_store:
                    pipe.rpush(redis_key, json.dumps(item_dict))
            pipe.expire(redis_key, self.redis_data_ttl_seconds)
            await pipe.execute()
            action = "Stored" if list_to_store else "Stored empty list (marker)"
            logger.info(f"{action} with {len(list_to_store)} items in Redis for key: {redis_key}")
        except Exception as e:
            logger.error(
                f"Error storing list to Redis key {redis_key}: {e}",
                exc_info=True
            )

    def _normalize_text(self, text: any) -> str:
        if self.custom_is_na(text):
            return ""
        text_str = str(text).lower()
        text_str = re.sub(r"[^\w\s]", "", text_str) # Mantenemos espacios
        text_str = ' '.join(text_str.split())
        return text_str

    def _cosine_similarity(self, text1_norm: str, text2_norm: str) -> float:
        words1 = text1_norm.split()
        words2 = text2_norm.split()
        if not words1 or not words2: # Si alguno está vacío después de normalizar
            return 1.0 if not words1 and not words2 else 0.0 # Ambos vacíos son 100% similares

        vector1 = Counter(words1)
        vector2 = Counter(words2)
        intersection = set(vector1.keys()) & set(vector2.keys())
        numerator = sum([vector1[x] * vector2[x] for x in intersection])
        sum1 = sum([vector1[x] ** 2 for x in vector1.keys()])
        sum2 = sum([vector2[x] ** 2 for x in vector2.keys()])
        denominator = math.sqrt(sum1) * math.sqrt(sum2)
        if not denominator:
            return 0.0
        return float(numerator) / denominator

    def _jaro_winkler_similarity(self, text1_norm: str, text2_norm: str) -> float:
        if not text1_norm and not text2_norm: # Ambos vacíos son 100% similares
             return 1.0
        if not text1_norm or not text2_norm: # Uno vacío y el otro no
             return 0.0
        return jellyfish.jaro_winkler_similarity(text1_norm, text2_norm)

    @measure_time
    async def detect_updates(self, new_transaction: dict) -> list[dict]:
        try:
            company_id = new_transaction['company_id']
            bank = new_transaction['bank']
            account_number = new_transaction['account_number']

            date_input = new_transaction['transaction_date']

            if isinstance(date_input, datetime.datetime):
                transaction_date_obj = date_input.date()
            elif isinstance(date_input, datetime.date):
                transaction_date_obj = date_input
            elif isinstance(date_input, str):
                date_str_part = date_input.split(' ')[0] # Tomar solo la parte de la fecha si viene con hora
                transaction_date_obj = datetime.datetime.strptime(
                    date_str_part, '%Y-%m-%d'
                ).date()
            else:
                raise ValueError(
                    f"Unsupported date format or unexpected type: "
                    f"{type(date_input)} for value {date_input}"
                )
            transaction_date_str = transaction_date_obj.strftime('%Y-%m-%d')

            amount_str = self._format_field_for_key(new_transaction['amount'])
            # NUEVO: Procesar reported_remaining
            reported_remaining_str = self._format_field_for_key(new_transaction.get('reported_remaining', 'N/A'))

            concept_new_raw = str(new_transaction.get('concept', ''))
            checksum_new = new_transaction['checksum']
            metadata_new = new_transaction.get('metadata')

        except KeyError as e:
            logger.error(
                f"Required field missing in new_transaction: {e}",
                exc_info=True
            )
            return []
        except ValueError as e:
            logger.error(
                f"Error processing date '{new_transaction.get('transaction_date')}': {e}",
                exc_info=True
            )
            return []
        except Exception as e:
            logger.error(
                f"Error processing new_transaction fields: {e}",
                exc_info=True
            )
            return []

        metadata_serialized_str_new = self._serialize_metadata_for_key(metadata_new)

        redis_key = self._generate_redis_key_for_candidates(
            company_id, bank, account_number, transaction_date_str,
            amount_str, metadata_serialized_str_new,
            reported_remaining_str # NUEVO: Pasar reported_remaining_str
        )

        current_redis_candidates = await self._get_candidates_from_redis(redis_key)
        updated_transactions_details = []
        final_list_for_redis = []

        new_transaction_data_to_add = {"cs": checksum_new, "ctx": concept_new_raw}
        new_transaction_is_in_redis_list = False

        if current_redis_candidates is None:
            logger.info(
                f"No data or invalid data in Redis for key {redis_key}. "
                f"Will store new transaction."
            )
            final_list_for_redis = [new_transaction_data_to_add]
            await self._store_list_to_redis(redis_key, final_list_for_redis)
            return []
        else:
            logger.info(
                f"Found {len(current_redis_candidates)} candidates in Redis "
                f"for key {redis_key}."
            )
            normalized_concept_new = self._normalize_text(concept_new_raw)
            len_normalized_concept_new = len(normalized_concept_new)

            for cand in current_redis_candidates:
                checksum_candidate = cand['cs']
                if checksum_new == checksum_candidate:
                    new_transaction_is_in_redis_list = True
                    # Si ya existe exactamente esta transacción (mismo checksum),
                    # la consideramos para la lista final pero no es una actualización *de* otra.
                    # Y tampoco puede ser actualizada por sí misma.
                    continue

                concept_candidate_raw = cand['ctx']
                normalized_concept_candidate = self._normalize_text(concept_candidate_raw)
                len_normalized_concept_candidate = len(normalized_concept_candidate)

                # NUEVO: Condición de longitud: el nuevo concepto debe ser igual o más largo
                # que el candidato para ser considerado una "ampliación"
                if len_normalized_concept_new < len_normalized_concept_candidate:
                    logger.debug(
                        f"Skipping candidate {checksum_candidate} because new concept "
                        f"'{normalized_concept_new}' ({len_normalized_concept_new}) is shorter than "
                        f"candidate concept '{normalized_concept_candidate}' ({len_normalized_concept_candidate})."
                    )
                    continue
                
                # Adicionalmente, si el nuevo concepto es solo un poco más largo, podría ser
                # un buen indicador si la diferencia es pequeña.
                # concept_length_difference = len_normalized_concept_new - len_normalized_concept_candidate
                # if concept_length_difference < self.concept_length_growth_threshold:
                #    logger.debug(f"Skipping... concept length growth {concept_length_difference} is less than threshold {self.concept_length_growth_threshold}")
                #    continue


                lev_dist = levenshtein_distance(
                    normalized_concept_new, normalized_concept_candidate
                )
                cos_sim = self._cosine_similarity(
                    normalized_concept_new, normalized_concept_candidate
                )
                jaro_sim = self._jaro_winkler_similarity(
                    normalized_concept_new, normalized_concept_candidate
                )

                logger.info("Comparing concepts:")
                logger.info(f"  Candidate CS: {checksum_candidate}, Concept: '{concept_candidate_raw}' (Normalized: '{normalized_concept_candidate}')")
                logger.info(f"  New CS: {checksum_new}, Concept: '{concept_new_raw}' (Normalized: '{normalized_concept_new}')")
                logger.info(
                    f"  Metrics: L={lev_dist}, C={cos_sim:.4f}, J={jaro_sim:.4f}"
                )
                logger.info(
                    f"  Thresholds: L<={self.levenshtein_threshold}, "
                    f"C>={self.cosine_threshold}, J>={self.jaro_winkler_threshold}"
                )
                logger.info(
                    f"  Length check: len(new)={len_normalized_concept_new} >= len(cand)={len_normalized_concept_candidate} -> {len_normalized_concept_new >= len_normalized_concept_candidate}"
                )


                is_update = False
                # La condición de longitud ya se evaluó arriba, si queremos que sea mandatoria
                # para todos los tipos de similitud, la dejamos. Si no, se puede quitar de aquí
                # y evaluar por separado. Por ahora, asumimos que es una condición general
                # para el tipo de update que describes ("aumentar el concepto").

                if lev_dist <= self.levenshtein_threshold:
                    is_update = True
                    logger.info(f"Update candidate by Levenshtein (L={lev_dist})")
                elif cos_sim >= self.cosine_threshold:
                    is_update = True
                    logger.info(f"Update candidate by Cosine (C={cos_sim:.4f})")
                elif jaro_sim >= self.jaro_winkler_threshold:
                    is_update = True
                    logger.info(f"Update candidate by Jaro-Winkler (J={jaro_sim:.4f})")

                if is_update:
                    logger.info( # Cambiado de debug a info para más visibilidad en caso de match
                        f"Update found: new_cs={checksum_new} updates "
                        f"orig_cs={checksum_candidate} "
                        f"(L:{lev_dist}, C:{cos_sim:.2f}, J:{jaro_sim:.2f}). "
                        f"New concept len: {len_normalized_concept_new}, Candidate concept len: {len_normalized_concept_candidate}."
                    )
                    updated_transactions_details.append({
                        "original_checksum": checksum_candidate,
                        "new_checksum": checksum_new,
                        "metrics": {
                            "levenshtein_distance": lev_dist,
                            "cosine_similarity": round(cos_sim, 4),
                            "jaro_winkler_similarity": round(jaro_sim, 4)
                        },
                        "thresholds": {
                            "levenshtein": self.levenshtein_threshold,
                            "cosine": self.cosine_threshold,
                            "jaro_winkler": self.jaro_winkler_threshold
                        },
                        "concepts": { # Añadido para depuración y análisis
                            "original_normalized": normalized_concept_candidate,
                            "new_normalized": normalized_concept_new,
                            "original_raw": concept_candidate_raw,
                            "new_raw": concept_new_raw
                        }
                    })

            # Gestionar la lista final para Redis
            # Queremos que la lista en Redis contenga todos los conceptos únicos (por checksum)
            # que coinciden con la clave Redis.
            # Si la nueva transacción no estaba ya (por checksum), la añadimos.
            # Las transacciones que son actualizadas (identificadas en updated_transactions_details)
            # *NO* se eliminan de la lista de Redis, porque podrían ser actualizadas por
            # otra transacción futura, o la lógica de "actualización" podría cambiar.
            # El propósito de Redis aquí es mantener un conjunto de candidatos para una clave dada.

            final_list_for_redis = list(current_redis_candidates) # Copiamos para no modificar la original mientras iteramos
            if not new_transaction_is_in_redis_list:
                # Verificar si ya existe por contenido para evitar duplicados exactos si el checksum cambiara por error
                exists_by_content = any(
                    d['ctx'] == new_transaction_data_to_add['ctx'] for d in final_list_for_redis
                )
                if not exists_by_content:
                     final_list_for_redis.append(new_transaction_data_to_add)
                else:
                    logger.info(
                        f"New transaction with CS {checksum_new} has same concept as an existing "
                        f"candidate; not adding duplicate concept to Redis key {redis_key}."
                    )
            
            # Solo re-escribimos en Redis si la lista ha cambiado (se ha añadido la nueva transacción)
            # o si es la primera vez que se procesa algo para esta clave (cubierto por el `if current_redis_candidates is None`)
            if not new_transaction_is_in_redis_list: # Solo si es realmente nueva
                await self._store_list_to_redis(redis_key, final_list_for_redis)
            else:
                 logger.info(
                    f"New transaction {checksum_new} was already present by checksum in Redis key {redis_key}. "
                    f"Redis list not modified."
                 )


        if updated_transactions_details:
            logger.info(
                f"Found {len(updated_transactions_details)} original transaction(s) "
                f"updated by new transaction {checksum_new} for key {redis_key}"
            )

        return updated_transactions_details