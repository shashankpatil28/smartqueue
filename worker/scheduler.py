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


class WeightedRoundRobinScheduler(BaseScheduler):
    """
    Fetches tasks from queues based on a predefined weight for each queue.
    Queues with a higher weight are polled more frequently.
    """
    QUEUES_WITH_WEIGHTS = {
        "wrr_queue_high": 5,
        "wrr_queue_medium": 3,
        "wrr_queue_low": 1,
    }

    def __init__(self, redis_client: redis.Redis):
        super().__init__(redis_client)
        self._sequence = self._generate_sequence()
        self._index = 0
        print(f"WRR sequence generated: {self._sequence}")

    def _generate_sequence(self):
        """Generates the weighted list of queues to poll."""
        sequence = []
        for queue_name, weight in self.QUEUES_WITH_WEIGHTS.items():
            sequence.extend([queue_name] * weight)
        return sequence

    def get_task(self):
        """
        Cycles through the weighted sequence, attempting to pop a task from each.
        """
        # Loop indefinitely until a task is found
        while True:
            # Get the next queue from our weighted sequence
            queue_name = self._sequence[self._index]
            
            # Move to the next index for the next call, looping back to the start
            self._index = (self._index + 1) % len(self._sequence)

            # BLPOP with a 1-second timeout. If a task exists, it's returned.
            # If not, it returns None after 1s, and we continue to the next queue.
            result = self.redis.blpop([queue_name], timeout=1)

            if result:
                queue, task_data = result
                print(f"\n📨 WeightedRRScheduler: Fetched task from '{queue}'...")
                return task_data
            # If no task was found, the loop continues to the next queue in the sequence.