# In worker/worker.py

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
# Add threading for our non-blocking retry delay
from threading import Timer

import redis

# --- Local Imports ---
from scheduler import (PriorityScheduler, RoundRobinScheduler,
                       WeightedRoundRobinScheduler)
from tasks import TASK_REGISTRY

# --- Constants ---
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = 6379
# This is the dedicated queue for failed tasks that need retrying.
RETRY_QUEUE_NAME = "priority_queue"
MAX_WORKERS = 5

# Create a single, persistent Redis connection for the re-queuing logic.
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def requeue_task(task_dict: dict):
    """
    Adds a failed task to the priority queue for a retry attempt.

    Design Note: All failed tasks, regardless of their original queue, are
    escalated to the priority queue. This ensures that failing tasks are
    given higher precedence, which is a common and useful pattern.
    """
    try:
        task_id = task_dict.get('task_id')
        print(f"Re-queuing task {task_id} to '{RETRY_QUEUE_NAME}'...")

        task_data = json.dumps(task_dict)
        unique_task_member = f"{task_id}:{task_data}"
        
        # Use the original priority when re-queuing.
        priority = task_dict.get("priority", 5)
        
        redis_client.zadd(RETRY_QUEUE_NAME, {unique_task_member: priority})
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
        # Using .get() is safer and avoids a KeyError if the task is not found.
        task_function = TASK_REGISTRY.get(task_name)
        if not task_function:
            raise ValueError(f"Task '{task_name}' not found in registry.")

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
            # Use a non-blocking timer to re-queue the task.
            Timer(delay, requeue_task, [task]).start()
        else:
            print(f"💀 Task '{task_name}' failed permanently after {max_retries} retries. Moving to dead-letter queue.")
            # Move the failed task to a simple list for manual inspection.
            redis_client.lpush("dead_letter_queue", task_data)

def main():
    # Read the desired strategy from an environment variable.
    strategy = os.getenv("SCHEDULING_STRATEGY", "priority").lower()
    
    print(f"🚀 Starting SmartQueue Worker...")
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    # Instantiate the correct scheduler based on the strategy.
    if strategy == "round_robin":
        scheduler = RoundRobinScheduler(r)
        print("Using Round Robin scheduling strategy.")
    elif strategy == "weighted_round_robin":
        scheduler = WeightedRoundRobinScheduler(r)
        print("Using Weighted Round Robin scheduling strategy.")
    else: # Default to priority
        scheduler = PriorityScheduler(r)
        print("Using Priority scheduling strategy.")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        while True:
            try:
                # The worker's only job is to ask the scheduler for a task.
                task_data = scheduler.get_task()
                
                if task_data:
                    # And then submit it to the thread pool.
                    executor.submit(process_task, task_data)

            except redis.exceptions.ConnectionError as e:
                print(f"Redis connection error: {e}. Retrying in 5 seconds...")
                time.sleep(5)
            except Exception as e:
                print(f"An unexpected error occurred in the main loop: {e}")

if __name__ == "__main__":
    main()