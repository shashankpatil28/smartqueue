# In app/main.py

import redis.asyncio as redis
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager

# Add the Task schema import
from .schemas import Task
from .core.config import REDIS_HOST, REDIS_PORT

# --- Lifespan function remains the same ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    print(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}...")
    try:
        app.state.redis = await redis.from_url(
            f"redis://{REDIS_HOST}:{REDIS_PORT}",
            decode_responses=True
        )
        await app.state.redis.ping()
        print("Successfully connected to Redis.")
    except Exception as e:
        print(f"Could not connect to Redis: {e}")
        app.state.redis = None
    yield
    if app.state.redis:
        await app.state.redis.close()
        print("Redis connection closed.")

app = FastAPI(title="SmartQueue API", version="1.0", lifespan=lifespan)

# --- Root and test-redis endpoints remain the same ---
@app.get("/")
def read_root():
    return {"message": "SmartQueue API is running"}

@app.get("/test-redis")
async def test_redis_connection():
    if not app.state.redis:
        raise HTTPException(status_code=503, detail="Redis connection not available")
    await app.state.redis.ping()
    return {"status": "success", "message": "Redis connection is healthy"}


# --- ADD THE NEW ENDPOINT BELOW ---

@app.post("/submit", status_code=202)
async def submit_task(task: Task):
    """
    Accepts a task and pushes it onto the 'default_queue' in Redis.
    """
    if not app.state.redis:
        raise HTTPException(status_code=503, detail="Redis connection not available")

    try:
        # Convert the Pydantic task model to a JSON string for storage
        task_data = task.model_dump_json()

        # LPUSH adds the task to the beginning (left side) of the list.
        # A worker using RPOP will process tasks in FIFO order.
        await app.state.redis.lpush("default_queue", task_data)

        return {"message": "Task accepted", "task_id": task.task_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to queue task: {str(e)}")