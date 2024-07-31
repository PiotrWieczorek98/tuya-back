import asyncio
from fastapi import FastAPI
from contextlib import asynccontextmanager
from polling import poll_device, transfer_data_to_hourly, generate_missing_data
from endpoints import get_router

# Initialize FastAPI
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Actions to be taken on startup
    generate_missing_data_task = asyncio.create_task(generate_missing_data())
    poll_device_task = asyncio.create_task(poll_device())
    transfer_data_task = asyncio.create_task(transfer_data_to_hourly())
    yield

    # Actions to be taken on shutdown
    generate_missing_data_task.cancel()
    poll_device_task.cancel()
    transfer_data_task.cancel()

app = FastAPI(lifespan=lifespan) # type: ignore
app.include_router(get_router())


# To run the server, use: uvicorn main:app --reload
# Or uncomment to run with debugger
import uvicorn
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)