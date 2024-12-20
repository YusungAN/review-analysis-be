from fastapi import APIRouter

from api.endpoint import analysis, csvfile, data

api_router = APIRouter()
api_router.include_router(analysis.router, tags=["analysis"])
# api_router.include_router(user.router, prefix="/users", tags=["user"])
api_router.include_router(csvfile.router, tags=["csvfile"])
api_router.include_router(data.router, prefix="/data", tags=["data"])