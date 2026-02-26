import os
from contextlib import asynccontextmanager

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from ariesql.api import chat_router
from ariesql.config import settings
from ariesql.container import get_container, init_container
from ariesql.logger import Logger

load_dotenv()

logger = Logger(__name__).get_logger()


# Check if temp/ and media/ directories exist, if not create them
if not os.path.exists("temp"):
    os.makedirs("temp")
    logger.info("Created temp/ directory.")

if not os.path.exists("media"):
    os.makedirs("media")
    logger.info("Created media/ directory.")

logger.info(
    f"Starting ArieSQL server on table manifest: {settings.DATABASE_MANIFEST_PATH}"
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Eagerly initialise all DI singletons so the first request is fast."""
    logger.info("Initializing DI container...")
    init_container()
    logger.info("DI container ready.")
    yield
    logger.info("Shutting down – unwiring DI container...")
    get_container().unwire()
    logger.info("DI container unwired.")


app = FastAPI(
    title="ArieSQL",
    description="Natural language SQL agent API",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve generated media files (charts, etc.)
app.mount("/media", StaticFiles(directory="media"), name="media")

app.include_router(chat_router, prefix="/api/v1")


@app.get("/health")
async def health_check():
    return {"status": "ok"}


if __name__ == "__main__":
    logger.info("Starting ArieSQL server...")
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )
