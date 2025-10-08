# In app/main.py

import redis.asyncio as redis
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager

# Add the Task schema import
from .schemas import Task
from .core.config import REDIS_HOST, REDIS_PORT

# --- Add these for Round Robin logic ---
from threading import Lock
round_robin_counter = 0
rr_lock = Lock()
ROUND_ROBIN_QUEUES = ["rr_queue_1", "rr_queue_2", "rr_queue_3"]

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
    Accepts a task and queues it based on priority, mapping it to
    a weighted queue for the WRR scheduler.
    """
    if not app.state.redis:
        raise HTTPException(status_code=503, detail="Redis connection not available")

    task_data = task.model_dump_json()

    # Map priority score to a specific weighted queue
    if task.priority >= 8: # Priority 8, 9, 10
        target_queue = "wrr_queue_high"
    elif task.priority >= 4: # Priority 4, 5, 6, 7
        target_queue = "wrr_queue_medium"
    else: # Priority 0, 1, 2, 3
        target_queue = "wrr_queue_low"

    # For weighted queues, we can just use a simple list (FIFO)
    await app.state.redis.lpush(target_queue, task_data)
    
    return {"message": f"Task accepted to weighted queue '{target_queue}'", "task_id": task.task_id}