import logging
from typing import Dict, Any
from .mosaic import Mosaic

logger = logging.getLogger(__name__)


class DuplicateDetector:
    """
    Detector de transacciones duplicadas.
    """
    def __init__(self):
        self.mosaic = Mosaic()

    async def check_duplicate(
        self, transaction: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Verifica si una transacción es duplicada.
        """
        try:
            return await self.mosaic.process_transaction(transaction)
        except Exception as e:
            logger.error(f"Error verificando duplicado: {str(e)}")
            raise 