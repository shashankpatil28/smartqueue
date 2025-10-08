# In worker/worker.py

import redis
import json
import time
import os
from concurrent.futures import ThreadPoolExecutor
from tasks import TASK_REGISTRY

# --- Constants remain the same ---
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = 6379
QUEUE_NAME = "default_queue"
# As per the resume, we'll use 5 worker threads
MAX_WORKERS = 5

def process_task(task_data: str):
    """
    The function executed by each thread in the pool.
    It deserializes, looks up, and executes the task.
    """
    try:
        print(f"📥 Processing task data...")
        
        # Deserialize the JSON string back into a Python dictionary
        task = json.loads(task_data)
        
        task_name = task.get("task_name")
        payload = task.get("payload", {})
        
        # Look up the function in our registry
        task_function = TASK_REGISTRY.get(task_name)
        
        if task_function:
            print(f"🏃 Executing task '{task_name}'...")
            task_function(payload) # Execute the function with its payload
        else:
            print(f"⚠️ Error: Task '{task_name}' not found in registry.")
    except json.JSONDecodeError:
        print(f"Error: Could not decode task data: {task_data}")
    except Exception as e:
        print(f"An unexpected error occurred during task processing: {e}")


def main():
    print("🚀 Starting SmartQueue Worker with ThreadPool...")
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    
    print(f"Worker connected to Redis, listening on queue '{QUEUE_NAME}'")

    # The 'with' statement ensures threads are cleaned up properly
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        while True:
            try:
                # The main thread blocks here waiting for a new task
                _, task_data = r.brpop(QUEUE_NAME)
                
                print(f"\n📨 Dispatching task to thread pool...")
                # Submit the task processing to the thread pool.
                # This call is non-blocking; it returns immediately.
                executor.submit(process_task, task_data)

            except redis.exceptions.ConnectionError as e:
                print(f"Redis connection error: {e}. Retrying in 5 seconds...")
                time.sleep(5)
            except Exception as e:
                print(f"An unexpected error occurred in the main loop: {e}")

if __name__ == "__main__":
    main()