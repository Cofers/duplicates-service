import sys
import os
from fastapi import FastAPI, APIRouter
from fastapi.middleware.cors import CORSMiddleware
from src.api.duplicates_routes import router as duplicates_router

# Add the application root directory to the sys.path
# This is necessary when running from a different directory or with tools
# like gunicorn that might change the working directory or not fully respect PYTHONPATH
# The expected root in this Docker container is /app, which is the directory containing main.py and the 'src' folder
app_root = os.path.abspath(os.path.dirname(__file__))
if app_root not in sys.path:
    sys.path.insert(0, app_root)
# You could also directly use:
# if '/app' not in sys.path:
#    sys.path.insert(0, '/app')
# but the os.path version is generally more flexible.

app = FastAPI(
    title="Duplicates Service",
    description="Service for detecting duplicate transactions",
    version="0.1.0",
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include duplicates router directly in the app
app.include_router(
    duplicates_router,
    tags=["duplicates"]
)


@app.get("/")
async def root():
    """
    Root endpoint to check if the service is running.
    """
    return {"status": "ok", "message": "Duplicates Service is running"}