import json
import logging
from typing import List
from executor.task_queue.redis_client import RedisClient
from executor.task_queue.models import QueuePayload

logger = logging.getLogger(__name__)

class QueuePublisher:
    """
    Handles publishing tasks to Redis queues.
    Uses Lists for standard queues and Sorted Sets for delayed/retry queues.
    """
    def __init__(self, queue_name: str = "tasks:default"):
        self.queue_name = queue_name
        self.redis = RedisClient.get_client()

    def _get_priority_queue(self, priority_level: str) -> str:
        mapping = {
            "P1": f"{self.queue_name}:critical",
            "P2": f"{self.queue_name}:high",
            "P3": f"{self.queue_name}:medium",
            "P4": f"{self.queue_name}:low"
        }
        return mapping.get(priority_level.upper(), f"{self.queue_name}:medium")

    async def publish(self, payload: QueuePayload) -> bool:
        """
        Push a task to the specific priority queue list.
        """
        try:
            data = payload.model_dump_json()
            target_q = self._get_priority_queue(payload.priority_level)
            await self.redis.rpush(target_q, data)
            logger.debug(f"Published task {payload.task_id} to {target_q}")
            return True
        except Exception as e:
            logger.error(f"Failed to publish task {payload.task_id}: {e}")
            return False

    async def publish_batch(self, payloads: List[QueuePayload]) -> int:
        """
        Push a batch of tasks efficiently to their respective priority queues.
        """
        if not payloads:
            return 0
        try:
            from collections import defaultdict
            grouped = defaultdict(list)
            for p in payloads:
                grouped[self._get_priority_queue(p.priority_level)].append(p.model_dump_json())
                
            async with self.redis.pipeline(transaction=True) as pipe:
                for target_q, data_list in grouped.items():
                    pipe.rpush(target_q, *data_list)
                await pipe.execute()
                
            logger.info(f"Published {len(payloads)} tasks across priority queues.")
            return len(payloads)
        except Exception as e:
            logger.error(f"Failed to publish batch: {e}")
            return 0

    async def schedule_delayed(self, payload: QueuePayload, delay_seconds: int) -> bool:
        """
        Schedule a task for later execution using a Sorted Set.
        Score = current timestamp + delay.
        """
        try:
            import time
            execute_at = time.time() + delay_seconds
            data = payload.model_dump_json()
            
            # Use a separate sorted set queue for delayed tasks
            delayed_queue = f"{self.queue_name}:delayed"
            await self.redis.zadd(delayed_queue, {data: execute_at})
            logger.debug(f"Scheduled task {payload.task_id} on {delayed_queue} for in {delay_seconds}s")
            return True
        except Exception as e:
            logger.error(f"Failed to schedule task {payload.task_id}: {e}")
            return False
