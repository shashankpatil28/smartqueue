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
    Accepts a task and queues it based on priority or round-robin strategy.
    """
    global round_robin_counter
    if not app.state.redis:
        raise HTTPException(status_code=503, detail="Redis connection not available")

    task_data = task.model_dump_json()

    # --- Use Priority Queue if priority is explicitly high or low ---
    if task.priority > 5 or task.priority < 5:
        unique_task_member = f"{task.task_id}:{task_data}"
        await app.state.redis.zadd("priority_queue", {unique_task_member: task.priority})
        return {"message": "Task accepted to priority queue", "task_id": task.task_id}
    
    # --- Otherwise, distribute to Round Robin queues ---
    else:
        with rr_lock:
            queue_index = round_robin_counter % len(ROUND_ROBIN_QUEUES)
            target_queue = ROUND_ROBIN_QUEUES[queue_index]
            round_robin_counter += 1
        
        # We use LPUSH for FIFO behavior within each round-robin queue
        await app.state.redis.lpush(target_queue, task_data)
        return {"message": f"Task accepted to round-robin queue '{target_queue}'", "task_id": task.task_id}