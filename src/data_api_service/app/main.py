from fastapi import FastAPI
from src.data_api_service.app.api.routes import router as comparison_router

app = FastAPI(
    title="Financial Data API Service",
    version="0.1.0",
)

@app.get("/")
async def root():
    return {"status": "ok"}

app.include_router(comparison_router, prefix="/metrics", tags=["metrics"])