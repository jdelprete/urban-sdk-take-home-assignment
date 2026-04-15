from fastapi import FastAPI

from app.api.routes import router

app = FastAPI(
    title="Urban SDK Traffic Service",
    version="0.1.0",
    description="Traffic speed aggregation microservice backed by PostgreSQL/PostGIS.",
)
app.include_router(router)

