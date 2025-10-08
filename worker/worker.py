# In worker/worker.py

import redis
import json
import time
import os
from concurrent.futures import ThreadPoolExecutor
# Add threading for our non-blocking retry delay
from threading import Timer
from tasks import TASK_REGISTRY

# --- Constants remain the same ---
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = 6379
PRIORITY_QUEUE_NAME = "priority_queue"
MAX_WORKERS = 5

# Create a single, persistent Redis connection for the re-queuing logic
# This avoids creating a new connection for every retry.
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def requeue_task(task_dict: dict):
    """Adds a task back to the priority queue."""
    try:
        print(f"Re-queuing task {task_dict.get('task_id')}...")
        task_data = json.dumps(task_dict)
        unique_task_member = f"{task_dict.get('task_id')}:{task_data}"
        
        # Use the original priority when re-queuing
        priority = task_dict.get("priority", 5)
        
        redis_client.zadd(PRIORITY_QUEUE_NAME, {unique_task_member: priority})
    except Exception as e:
        print(f"Failed to re-queue task: {e}")

def process_task(task_data: str):
    """
    Deserializes and executes the task, with retry logic.
    """
    task = json.loads(task_data)
    task_name = task.get("task_name")
    
    try:
        print(f"🏃 Executing task '{task_name}' (Attempt {task.get('attempts', 0) + 1})...")
        task_function = TASK_REGISTRY[task_name]
        # Execute the function and capture the result
        task_function(task.get("payload", {}))
        
    except Exception as e:
        print(f"❌ Task '{task_name}' failed: {e}")
        
        attempts = task.get("attempts", 0) + 1
        max_retries = task.get("max_retries", 3)
        
        if attempts < max_retries:
            task["attempts"] = attempts
            # Exponential backoff: 5s, 20s, 45s, ...
            delay = 5 * (attempts ** 2) 
            print(f"🔁 Scheduling retry {attempts}/{max_retries} in {delay} seconds.")
            # Use a non-blocking timer to re-queue the task
            Timer(delay, requeue_task, [task]).start()
        else:
            print(f"💀 Task '{task_name}' failed permanently after {max_retries} retries. Moving to dead-letter queue (simulation).")
            # In a real system, you would move this task to another queue for inspection.
            # For now, we'll just log it.
            redis_client.lpush("dead_letter_queue", task_data)

# --- The main() function remains exactly the same ---
def main():
    print("🚀 Starting SmartQueue Worker with Priority Scheduling & Retries...")
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    print(f"Worker connected to Redis, listening on queue '{PRIORITY_QUEUE_NAME}'")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        while True:
            try:
                result = r.bzpopmax(PRIORITY_QUEUE_NAME)
                if result is None: continue
                _, unique_task_member, score = result
                print(f"\n📨 Dispatching task with priority {int(score)}...")
                task_data = unique_task_member.split(":", 1)[1]
                executor.submit(process_task, task_data)
            except redis.exceptions.ConnectionError as e:
                print(f"Redis connection error: {e}. Retrying in 5 seconds...")
                time.sleep(5)
            except Exception as e:
                print(f"An unexpected error occurred in the main loop: {e}")

if __name__ == "__main__":
    main()