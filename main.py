from fastapi import FastAPI
from app.api.ingest import router as ingest_router

app = FastAPI(title="Recovery Ingestion Service")

app.include_router(ingest_router)