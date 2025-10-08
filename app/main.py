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


# Let's use a new queue name for clarity
PRIORITY_QUEUE_NAME = "priority_queue"

@app.post("/submit", status_code=202)
async def submit_task(task: Task):
    """
    Accepts a task and adds it to the priority queue (a Redis Sorted Set).
    """
    if not app.state.redis:
        raise HTTPException(status_code=503, detail="Redis connection not available")

    try:
        task_data = task.model_dump_json()

        # Sorted Set members must be unique. We'll ensure this by using
        # the unique task_id we generate in the Pydantic model.
        # This prevents identical tasks from being treated as duplicates.
        unique_task_member = f"{task.task_id}:{task_data}"

        # ZADD adds a member to the sorted set with a score.
        # We use the task's priority as the score.
        await app.state.redis.zadd(
            PRIORITY_QUEUE_NAME,
            {unique_task_member: task.priority}
        )

        return {"message": "Task accepted with priority " + str(task.priority), "task_id": task.task_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to queue task: {str(e)}")