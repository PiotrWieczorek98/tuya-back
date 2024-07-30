import os
import asyncio
import logging
from fastapi import FastAPI
from contextlib import asynccontextmanager
from dotenv import load_dotenv, find_dotenv

from database import get_db_connection
from polling import poll_device
from endpoints import get_router

load_dotenv(find_dotenv())

# Load env
FREQUENCY = int(os.environ.get("FREQUENCY") or 10)

# Initialize FastAPI
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Actions to be taken on startup
    task = asyncio.create_task(poll_device())
    yield

    # Actions to be taken on shutdown
    task.cancel()
    db_conn = get_db_connection()
    db_conn.close()
    logger.info('Polling device ended')

app = FastAPI(lifespan=lifespan) # type: ignore
app.include_router(get_router())

logger = logging.getLogger('uvicorn.error')

# To run the server, use: uvicorn main:app --reload
