from fastapi import APIRouter
from .duplicates_routes import router as duplicates_router
from .updates_routes import router as updates_router

# Create main router
router = APIRouter()

# Include sub-routers
router.include_router(duplicates_router)
router.include_router(updates_router)
