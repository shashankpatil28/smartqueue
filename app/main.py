# In app/main.py

import os
from contextlib import asynccontextmanager
from threading import Lock

import redis.asyncio as redis
from fastapi import FastAPI, HTTPException

from .core.config import REDIS_HOST, REDIS_PORT
from .schemas import Task

# --- Configuration for Different Schedulers ---
# Read the active strategy from the environment to match the worker's behavior
SCHEDULING_STRATEGY = os.getenv("SCHEDULING_STRATEGY", "priority").lower()

# For Priority Scheduler
PRIORITY_QUEUE_NAME = "priority_queue"

# For Round Robin Scheduler
ROUND_ROBIN_QUEUES = ["rr_queue_1", "rr_queue_2", "rr_queue_3"]
round_robin_counter = 0
rr_lock = Lock()

# --- Lifespan Manager for Redis Connection ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handles Redis connection pool startup and shutdown."""
    print(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}...")
    try:
        app.state.redis = await redis.from_url(
            f"redis://{REDIS_HOST}:{REDIS_PORT}", decode_responses=True
        )
        await app.state.redis.ping()
        print(f"Successfully connected to Redis. API is configured for '{SCHEDULING_STRATEGY}' strategy.")
    except Exception as e:
        print(f"Could not connect to Redis: {e}")
        app.state.redis = None
    yield
    if app.state.redis:
        await app.state.redis.close()
        print("Redis connection closed.")

app = FastAPI(title="SmartQueue API", version="1.0", lifespan=lifespan)

# --- Health Check Endpoints ---
@app.get("/")
def read_root():
    return {"message": "SmartQueue API is running"}

@app.get("/test-redis")
async def test_redis_connection():
    if not app.state.redis:
        raise HTTPException(status_code=503, detail="Redis connection not available")
    await app.state.redis.ping()
    return {"status": "success", "message": "Redis connection is healthy"}

# --- Task Submission Endpoint ---
@app.post("/submit", status_code=202)
async def submit_task(task: Task):
    """
    Accepts a task and queues it using the currently configured scheduling strategy.
    """
    global round_robin_counter
    if not app.state.redis:
        raise HTTPException(status_code=503, detail="Redis connection not available")

    task_data = task.model_dump_json()

    # --- LOGIC TO HANDLE DIFFERENT SCHEDULING STRATEGIES ---

    if SCHEDULING_STRATEGY == "priority":
        unique_task_member = f"{task.task_id}:{task_data}"
        await app.state.redis.zadd(PRIORITY_QUEUE_NAME, {unique_task_member: task.priority})
        message = f"Task accepted to priority queue with priority {task.priority}"

    elif SCHEDULING_STRATEGY == "round_robin":
        with rr_lock:
            queue_index = round_robin_counter % len(ROUND_ROBIN_QUEUES)
            target_queue = ROUND_ROBIN_QUEUES[queue_index]
            round_robin_counter += 1
        await app.state.redis.lpush(target_queue, task_data)
        message = f"Task accepted to round-robin queue '{target_queue}'"

    elif SCHEDULING_STRATEGY == "weighted_round_robin":
        if task.priority >= 8: target_queue = "wrr_queue_high"
        elif task.priority >= 4: target_queue = "wrr_queue_medium"
        else: target_queue = "wrr_queue_low"
        await app.state.redis.lpush(target_queue, task_data)
        message = f"Task accepted to weighted queue '{target_queue}'"

    else:
        raise HTTPException(status_code=500, detail=f"Invalid scheduling strategy '{SCHEDULING_STRATEGY}' configured.")

    return {"message": message, "task_id": task.task_id}