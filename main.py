import sys
import os
import logging # Para el logging en el lifespan
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager # Para el lifespan
from redis import asyncio as redis_async_pkg # Cliente Redis asíncrono

# --- Importaciones de tus módulos ---
try:
    from src.transaction_update_detector import TransactionUpdateDetectorRedis
    from routes.updates_routes import router as updates_router
except ImportError as e:
    logger = logging.getLogger("main_app")
    logger.error(f"Error importando módulos: {e}. Verifica tus rutas de importación y la estructura del proyecto.")
    logger.error(f"sys.path actual: {sys.path}")
    raise ImportError(f"No se pudieron importar los módulos necesarios: {e}")

from routes.duplicates_routes import router as duplicates_router

# Configuración básica de logging (puedes hacerla más avanzada)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("main_app") # Logger específico para esta parte

# Add the application root directory to the sys.path
app_root = os.path.abspath(os.path.dirname(__file__))
if app_root not in sys.path:
    sys.path.insert(0, app_root)
src_path = os.path.join(app_root, 'src') # Común tener una carpeta 'src'
if src_path not in sys.path:
    sys.path.insert(0, src_path)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Iniciando aplicación y recursos (lifespan)...")
    app.state.redis_client = None
    app.state.update_detector = None
    
    # --- Conexión a Redis ---
    # Idealmente, REDIS_URL vendría de variables de entorno o un archivo de configuración
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/2") 
    logger.info(f"Intentando conectar a Redis en: {redis_url}")
    
    try:
        # Crear y conectar el cliente Redis
        client = redis_async_pkg.from_url(redis_url, decode_responses=False)
        await client.ping() # Verificar conexión
        app.state.redis_client = client # Guardar en el estado de la app
        logger.info("Conectado a Redis exitosamente.")

        # Crear la instancia del detector con el cliente Redis
        # Asegúrate que TransactionUpdateDetectorRedis se importó correctamente arriba
        if TransactionUpdateDetectorRedis.__name__ != "TransactionUpdateDetectorRedis": # Placeholder check
             logger.warning("TransactionUpdateDetectorRedis no se importó correctamente, usando placeholder.")
        app.state.update_detector = TransactionUpdateDetectorRedis(redis_client=app.state.redis_client)
        logger.info("TransactionUpdateDetectorRedis inicializado.")

    except redis_async_pkg.exceptions.ConnectionError as e:
        logger.error(f"Error de conexión a Redis: {e}", exc_info=True)
        # La aplicación podría seguir funcionando sin Redis, pero el detector no.
        # Depende de tus requisitos si quieres que la app falle al iniciar.
    except AttributeError as e:
        logger.error(f"AttributeError durante la inicialización (posiblemente redis_client es None o TransactionUpdateDetectorRedis no se importó): {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Error inesperado durante el inicio (Redis o Detector): {e}", exc_info=True)
        # Limpiar si algo falló parcialmente
        if hasattr(app.state, 'redis_client') and app.state.redis_client: # type: ignore
            await app.state.redis_client.close() # type: ignore
        app.state.redis_client = None
        app.state.update_detector = None
        

    yield  # La aplicación se ejecuta aquí

    # --- Lógica de Apagado (Shutdown) ---
    logger.info("Apagando aplicación y liberando recursos (lifespan)...")
    if hasattr(app.state, 'redis_client') and app.state.redis_client: # type: ignore
        try:
            await app.state.redis_client.close() # type: ignore
            logger.info("Conexión a Redis cerrada.")
        except Exception as e:
            logger.error(f"Error cerrando la conexión a Redis: {e}", exc_info=True)
    
    # Limpiar el estado
    app.state.redis_client = None
    app.state.update_detector = None
    logger.info("Recursos de lifespan limpiados.")


app = FastAPI(
    title="Duplicates & Updates Service", # Título actualizado
    description="Service for detecting duplicate and updated transactions", # Descripción actualizada
    version="0.1.0",
    lifespan=lifespan # <--- AÑADIDO EL LIFESPAN MANAGER
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # En producción, reemplaza con orígenes específicos
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Incluir routers
app.include_router(
    duplicates_router,
    prefix="/transactions", # Es buena práctica añadir prefijos a los routers
    tags=["duplicates"]
)

# Incluir el router para las actualizaciones
app.include_router(
    updates_router,
    prefix="/transactions",
    tags=["updates"]
)


@app.get("/")
async def root():
    """
    Endpoint raíz para verificar si el servicio está corriendo.
    """
    # Podrías añadir una verificación del estado de Redis aquí si quieres
    redis_status = "conectado" if hasattr(app.state, 'redis_client') and app.state.redis_client else "desconectado"
    detector_status = "inicializado" if hasattr(app.state, 'update_detector') and app.state.update_detector else "no inicializado"
    
    return {
        "status": "ok", 
        "message": "Duplicates & Updates Service is running",
        "redis_status": redis_status,
        "update_detector_status": detector_status
    }

# Si quieres ejecutar esto directamente con uvicorn (para desarrollo)
# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
#     # Nota: 'main:app' asume que este archivo se llama main.py