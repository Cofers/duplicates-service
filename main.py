import sys
import os
import logging  # For lifespan logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager  # For lifespan
from redis import asyncio as redis_async_pkg  # Async Redis client

# --- Module imports ---
try:
    from src.transaction_update_detector import TransactionUpdateDetectorRedis
    from routes.updates_routes import router as updates_router
except ImportError as e:
    logger = logging.getLogger("main_app")
    logger.error(f"Error importing modules: {e}. Check your import paths and project structure.")
    logger.error(f"Current sys.path: {sys.path}")
    raise ImportError(f"Could not import required modules: {e}")

from routes.duplicates_routes import router as duplicates_router

# Basic logging configuration (can be made more advanced)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("main_app")  # Specific logger for this part

# Add the application root directory to the sys.path
app_root = os.path.abspath(os.path.dirname(__file__))
if app_root not in sys.path:
    sys.path.insert(0, app_root)
src_path = os.path.join(app_root, 'src')  # Common to have a 'src' folder
if src_path not in sys.path:
    sys.path.insert(0, src_path)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting application and resources (lifespan)...")
    app.state.redis_client = None
    app.state.update_detector = None
    
    # --- Redis Connection ---
    # Build Redis URL using REDIS_HOST and database 2
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_url = f"redis://{redis_host}:6379/2"
    logger.info(f"Attempting to connect to Redis at: {redis_url}")
    
    try:
        # Create and connect Redis client
        client = redis_async_pkg.from_url(redis_url, decode_responses=False)
        await client.ping()  # Verify connection
        app.state.redis_client = client  # Save in app state
        logger.info("Successfully connected to Redis.")

        # Create detector instance with Redis client
        # Make sure TransactionUpdateDetectorRedis was imported correctly above
        if TransactionUpdateDetectorRedis.__name__ != "TransactionUpdateDetectorRedis":  # Placeholder check
            logger.warning("TransactionUpdateDetectorRedis not imported correctly, using placeholder.")
        app.state.update_detector = TransactionUpdateDetectorRedis(redis_client=app.state.redis_client)
        logger.info("TransactionUpdateDetectorRedis initialized.")

    except redis_async_pkg.exceptions.ConnectionError as e:
        logger.error(f"Redis connection error: {e}", exc_info=True)
        # Application could continue without Redis, but detector won't work
        # Depends on your requirements if you want the app to fail on startup
    except AttributeError as e:
        logger.error(f"AttributeError during initialization (possibly redis_client is None or TransactionUpdateDetectorRedis not imported): {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error during startup (Redis or Detector): {e}", exc_info=True)
        # Clean up if something partially failed
        if hasattr(app.state, 'redis_client') and app.state.redis_client:  # type: ignore
            await app.state.redis_client.close()  # type: ignore
        app.state.redis_client = None
        app.state.update_detector = None

    yield  # Application runs here

    # --- Shutdown Logic ---
    logger.info("Shutting down application and releasing resources (lifespan)...")
    if hasattr(app.state, 'redis_client') and app.state.redis_client:  # type: ignore
        try:
            await app.state.redis_client.close()  # type: ignore
            logger.info("Redis connection closed.")
        except Exception as e:
            logger.error(f"Error closing Redis connection: {e}", exc_info=True)
    
    # Clean up state
    app.state.redis_client = None
    app.state.update_detector = None
    logger.info("Lifespan resources cleaned up.")


app = FastAPI(
    title="Duplicates & Updates Service",  # Updated title
    description="Service for detecting duplicate and updated transactions",  # Updated description
    version="0.1.0",
    lifespan=lifespan  # <--- ADDED LIFESPAN MANAGER
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(
    duplicates_router,
    prefix="/transactions",  # Good practice to add prefixes to routers
    tags=["duplicates"]
)

# Include updates router
app.include_router(
    updates_router,
    prefix="/transactions",
    tags=["updates"]
)


@app.get("/")
async def root():
    """
    Root endpoint to verify if the service is running.
    """
    # You could add Redis status verification here if you want
    redis_status = "connected" if hasattr(app.state, 'redis_client') and app.state.redis_client else "disconnected"
    detector_status = "initialized" if hasattr(app.state, 'update_detector') and app.state.update_detector else "not initialized"
    
    return {
        "status": "ok", 
        "message": "Duplicates & Updates Service is running",
        "redis_status": redis_status,
        "update_detector_status": detector_status
    }

# If you want to run this directly with uvicorn (for development)
# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
#     # Note: 'main:app' assumes this file is called main.py