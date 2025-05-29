import os
import httpx
import json
import uuid
from typing import Dict, Any, Optional

class LLMClient:
    """
    Generic client for interacting with LLM APIs.
    Can handle new user/session per request or a persistent session.
    """
    def __init__(
        self,
        app_name: str,
        default_host: str,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
        timeout: float = 60.0
    ):
        """
        Initialize LLM client.

        Args:
            app_name: Name of the LLM application
            default_host: Default host URL for the LLM API
            user_id: Optional default user identifier for the instance
            session_id: Optional default session identifier for the instance
            timeout: Request timeout in seconds
        """
        self.app_name = app_name
        self.default_host = default_host
        self.llm_api_host = os.getenv("LLM_API_HOST", self.default_host)
        self.client = httpx.AsyncClient(timeout=timeout)
        
        self.user_id = user_id
        self.session_id = session_id
        self.instance_session_created = False

    async def ensure_session(
        self,
        initial_state: Optional[Dict[str, Any]] = None,
        user_id_to_ensure: Optional[str] = None,
        session_id_to_ensure: Optional[str] = None
    ) -> None:
        """
        Ensures an LLM session exists, creates it if it doesn't.
        Uses provided IDs or falls back to instance's default IDs.

        Args:
            initial_state: Initial state for the session.
            user_id_to_ensure: Specific user ID for this session.
            session_id_to_ensure: Specific session ID for this session.
        """
        target_user_id = user_id_to_ensure or self.user_id
        target_session_id = session_id_to_ensure or self.session_id

        if not target_user_id or not target_session_id:
            raise ValueError("User ID and Session ID must be specified either at instance level or per call for ensure_session.")

        is_ensuring_instance_session = (target_user_id == self.user_id and
                                        target_session_id == self.session_id)
        
        if is_ensuring_instance_session and self.instance_session_created:
            print(f"Instance session {target_session_id} already ensured for user {target_user_id}.")
            return

        session_url = (
            f"{self.llm_api_host}/apps/{self.app_name}/users/"
            f"{target_user_id}/sessions/{target_session_id}"
        )
        try:
            session_response = await self.client.post(
                session_url,
                json={"state": initial_state or {}}
            )
            session_response.raise_for_status()
            
            if is_ensuring_instance_session:
                self.instance_session_created = True
            print(f"Session {target_session_id} ensured for user {target_user_id} in app {self.app_name}.")

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 409: # Conflict - Session already exists
                print(f"Session {target_session_id} already exists for user {target_user_id}.")
                if is_ensuring_instance_session:
                    self.instance_session_created = True
            else:
                print(f"HTTP error ensuring session {target_session_id}: {e.response.status_code} - {e.response.text}")
                raise Exception(f"HTTP error ensuring session: {e.response.text}") from e
        except httpx.RequestError as e:
            print(f"Request error ensuring session {target_session_id}: {e}")
            raise Exception(f"Request error ensuring session: {e}") from e
        except Exception as e:
            print(f"An unexpected error occurred ensuring session {target_session_id}: {e}")
            raise Exception(f"An unexpected error occurred ensuring session: {e}") from e

    async def analyze_message(
        self,
        message: str,
        initial_state: Optional[Dict[str, Any]] = None
    ) -> Dict[str, str]:
        """
        Sends a message to the LLM and gets its analysis.
        Creates a NEW user and session for EACH call.

        Args:
            message: The message to analyze
            initial_state: Initial state for the new session

        Returns:
            A dictionary containing 'classification' and 'reason' from the LLM response
        """
        call_user_id = f"u_{uuid.uuid4()}"
        call_session_id = f"s_{uuid.uuid4()}"
        llm_response = None 

        print(f"Analyzing message for new user: {call_user_id}, new session: {call_session_id}")

        try:
            await self.ensure_session(
                initial_state=initial_state,
                user_id_to_ensure=call_user_id,
                session_id_to_ensure=call_session_id
            )

            run_url = f"{self.llm_api_host}/run"
            payload = {
                "app_name": self.app_name,
                "user_id": call_user_id,
                "session_id": call_session_id,
                "new_message": {
                    "role": "user",
                    "parts": [{"text": message}]
                },
                "streaming": False
            }
            llm_response = await self.client.post(run_url, json=payload)
            llm_response.raise_for_status()
            response_data = llm_response.json()
            
            if not response_data:
                raise ValueError("LLM response data is empty.")
            
            last_event = response_data[-1]
            if not isinstance(last_event, dict) or "content" not in last_event or \
               not isinstance(last_event["content"], dict) or "parts" not in last_event["content"] or \
               not isinstance(last_event["content"]["parts"], list) or not last_event["content"]["parts"] or \
               not isinstance(last_event["content"]["parts"][0], dict) or \
               "text" not in last_event["content"]["parts"][0]:
                raise ValueError(f"Unexpected structure in LLM response's last event: {last_event}")

            text_content = last_event["content"]["parts"][0]["text"]
            
            classification_parts = text_content.split("CLASSIFICATION:")
            if len(classification_parts) < 2:
                raise ValueError(f"CLASSIFICATION not found in LLM response: {text_content}")
            classification = classification_parts[1].split("\n")[0].strip()
            
            reason_parts = text_content.split("REASON:")
            if len(reason_parts) < 2:
                raise ValueError(f"REASON not found in LLM response: {text_content}")
            reason = reason_parts[1].split("\n")[0].strip()
            
            return {
                "classification": classification,
                "reason": reason
            }

        except httpx.HTTPStatusError as e:
            error_detail = e.response.text if e.response else str(e)
            print(f"HTTP error during LLM analysis for {call_user_id}/{call_session_id}: {e} - Details: {error_detail}")
            try:
                error_data = e.response.json() if e.response else {}
                raise Exception(f"LLM analysis failed: {error_data.get('detail', error_detail)}") from e
            except json.JSONDecodeError:
                raise Exception(f"LLM analysis failed with non-JSON error: {error_detail}") from e
        except httpx.RequestError as e:
            print(f"Request error during LLM analysis for {call_user_id}/{call_session_id}: {e}")
            raise Exception(f"Request error during LLM analysis: {e}") from e
        except ValueError as e: 
            print(f"Data error during LLM analysis for {call_user_id}/{call_session_id}: {e}")
            raw_response_info = llm_response.text if llm_response else "N/A"
            raise Exception(f"Data error during LLM analysis: {e}. Raw response: {raw_response_info}") from e
        except Exception as e:
            print(f"An unexpected error occurred during LLM analysis for {call_user_id}/{call_session_id}: {e}")
            raw_response_info = llm_response.text if llm_response else "N/A"
            raise Exception(f"An unexpected error occurred during LLM analysis: {e}. Raw response: {raw_response_info}") from e