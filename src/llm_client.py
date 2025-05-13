import os
import httpx
import json
import uuid
from typing import Dict, Any


class LLMClient:
    """
    Generic client for interacting with LLM APIs.
    Handles session management and message processing.
    """
    def __init__(
        self,
        app_name: str,
        default_host: str,
        session_id: str = None,
        user_id: str = "u_123",
        timeout: float = 60.0
    ):
        """
        Initialize LLM client.

        Args:
            app_name: Name of the LLM application
            default_host: Default host URL for the LLM API
            session_id: Session identifier (if None, a new UUID will be generated)
            user_id: User identifier
            timeout: Request timeout in seconds
        """
        self.app_name = app_name
        self.default_host = default_host
        # Prioriza LLM_API_HOST del entorno, si no, usa default_host
        self.llm_api_host = os.getenv("LLM_API_HOST", self.default_host)
        self.client = httpx.AsyncClient(timeout=timeout)
        self.session_id = session_id or f"s_{uuid.uuid4()}"
        self.user_id = user_id
        self.session_created = False

    async def ensure_session(self, initial_state: Dict[str, Any] = None) -> None:
        """
        Ensures LLM session exists, creates it if it doesn't.

        Args:
            initial_state: Initial state for the session
        """
        if not self.session_created:
            session_url = (
                f"{self.llm_api_host}/apps/{self.app_name}/users/"
                f"{self.user_id}/sessions/{self.session_id}"
            )
            try:
                session_response = await self.client.post(
                    session_url,
                    json={"state": initial_state or {}}
                )

                session_response.raise_for_status() # Lanza una excepción para códigos 4xx/5xx

                # Considerar verificar si el status code es específicamente 200 OK o 409 Conflict (ya existe)
                # if session_response.status_code == 200 or session_response.status_code == 409:
                #     self.session_created = True
                # else:
                #     raise Exception(f"Unexpected status code {session_response.status_code}: {session_response.text}")

                # Una vez que raise_for_status() no lanzó error, asumimos que la sesión está lista.
                self.session_created = True
                print(f"Session {self.session_id} ensured for user {self.user_id} in app {self.app_name}.")

            except httpx.HTTPStatusError as e:
                 # Manejar específicamente el caso "Session already exists" si es necesario
                 # Por ejemplo, si el ADK devuelve un código específico o texto en el cuerpo para esto.
                 # La implementación de ADK para crear sesión puede devolver 409 Conflict.
                 if e.response.status_code == 409:
                     print(f"Session {self.session_id} already exists.")
                     self.session_created = True # Si ya existe, para nosotros es "asegurada"
                 else:
                     print(f"HTTP error ensuring session: {e}")
                     raise Exception(f"HTTP error ensuring session: {e.response.text}") from e # Relanza con más contexto
            except httpx.RequestError as e:
                 print(f"Request error ensuring session: {e}")
                 raise Exception(f"Request error ensuring session: {e}") from e
            except Exception as e:
                 print(f"An unexpected error occurred ensuring session: {e}")
                 raise Exception(f"An unexpected error occurred ensuring session: {e}") from e


    async def analyze_message(
        self,
        message: str,
        initial_state: Dict[str, Any] = None
    ) -> Dict[str, str]:
        """
        Sends a message to the LLM and gets its analysis.

        Args:
            message: The message to analyze
            initial_state: Initial state for the session

        Returns:
            A dictionary containing 'classification' and 'reason' from the LLM response
        """
        try:
            await self.ensure_session(initial_state)

            run_url = f"{self.llm_api_host}/run"
            llm_response = await self.client.post(
                run_url,
                json={
                    "app_name": self.app_name,
                    "user_id": self.user_id,
                    "session_id": self.session_id,
                    "new_message": {
                        "role": "user",
                        "parts": [{"text": message}]
                    },
                    "streaming": False
                }
            )

            llm_response.raise_for_status()
            response_data = llm_response.json()
            
            # El último evento siempre contiene la clasificación
            last_event = response_data[-1]
            text = last_event["content"]["parts"][0]["text"]
            
            # Extraer CLASSIFICATION y REASON
            classification = text.split("CLASSIFICATION:")[1].split("\n")[0].strip()
            reason = text.split("REASON:")[1].split("\n")[0].strip()
            
            return {
                "classification": classification,
                "reason": reason
            }

        except httpx.HTTPStatusError as e:
            error_detail = e.response.text
            print(f"HTTP error during LLM analysis: {e} - Details: {error_detail}")
            try:
                error_data = e.response.json()
                raise Exception(f"LLM analysis failed: {error_data.get('detail', error_detail)}") from e
            except json.JSONDecodeError:
                raise Exception(f"LLM analysis failed: {error_detail}") from e

        except httpx.RequestError as e:
            print(f"Request error during LLM analysis: {e}")
            raise Exception(f"Request error during LLM analysis: {e}") from e
        except Exception as e:
            print(f"An unexpected error occurred during LLM analysis: {e}")
            raw_response_info = llm_response.json() if 'llm_response' in locals() and llm_response.text else "N/A"
            raise Exception(f"An unexpected error occurred during LLM analysis: {e}. Raw response: {raw_response_info}") from e