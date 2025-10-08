# In worker/scheduler.py

import redis
from abc import ABC, abstractmethod

class BaseScheduler(ABC):
    """
    Abstract base class for all schedulers. Defines the interface.
    """
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client

    @abstractmethod
    def get_task(self):
        """
        Fetches the next task from Redis according to the strategy.
        This method should be blocking.
        """
        pass

class PriorityScheduler(BaseScheduler):
    """
    Fetches tasks based on the highest priority using a Sorted Set.
    """
    QUEUE_NAME = "priority_queue"
    
    def get_task(self):
        # This is the same logic we had before
        result = self.redis.bzpopmax(self.QUEUE_NAME)
        if result:
            _, unique_task_member, score = result
            print(f"\n📨 PriorityScheduler: Fetched task with priority {int(score)}...")
            # Extract the original JSON data
            return unique_task_member.split(":", 1)[1]
        return None

class RoundRobinScheduler(BaseScheduler):
    """
    Fetches tasks from a list of queues in a round-robin fashion.
    """
    # We'll listen to three queues for this strategy
    QUEUE_NAMES = ["rr_queue_1", "rr_queue_2", "rr_queue_3"]

    def get_task(self):
        # BRPOP atomically checks and pops from the first non-empty list.
        # It's perfect for round-robin behavior.
        # It returns a tuple: (queue_name, item_data)
        result = self.redis.brpop(self.QUEUE_NAMES)
        if result:
            queue, task_data = result
            print(f"\n📨 RoundRobinScheduler: Fetched task from '{queue}'...")
            return task_data
        return None