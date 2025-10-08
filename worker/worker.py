# In worker/worker.py

import redis
import json
import time
import os
from tasks import TASK_REGISTRY

# Get Redis connection details from environment variables
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = 6379
QUEUE_NAME = "default_queue"

def main():
    print("🚀 Starting SmartQueue Worker...")
    # `decode_responses=True` is crucial to get strings from Redis, not bytes
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    
    print(f"Worker connected to Redis, listening on queue '{QUEUE_NAME}'")

    while True:
        try:
            # BRPOP is a blocking command. It waits efficiently until an item 
            # is available on the list, then pops and returns it.
            # The '0' timeout means it will wait indefinitely.
            # The result is a tuple: (queue_name, item_data)
            _, task_data = r.brpop(QUEUE_NAME)
            
            print(f"\n📥 Received new task...")
            
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

        except redis.exceptions.ConnectionError as e:
            print(f"Redis connection error: {e}. Retrying in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()