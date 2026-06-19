import json
import logging
import time
from typing import Optional, List
from executor.task_queue.redis_client import RedisClient
from executor.task_queue.models import QueuePayload

import random

logger = logging.getLogger(__name__)

class QueueConsumer:
    """
    Handles consuming tasks from Redis queues with Priority weighting, DLQ and Delayed logic.
    """
    def __init__(self, queue_name: str = "tasks:default", fetch_timeout: int = 2):
        self.queue_name = queue_name
        self.delayed_queue = f"{queue_name}:delayed"
        self.dlq_name = f"{queue_name}:dlq"
        self.redis = RedisClient.get_client()
        self.fetch_timeout = fetch_timeout

    def _get_poll_queues(self) -> List[str]:
        """
        Returns the queue keys ordered by weighted priority to prevent starvation.
        Critical (60%), High (25%), Medium (10%), Low (5%) chance to be first.
        """
        q_critical = f"{self.queue_name}:critical"
        q_high = f"{self.queue_name}:high"
        q_medium = f"{self.queue_name}:medium"
        q_low = f"{self.queue_name}:low"
        
        r = random.random()
        if r < 0.60:
            return [q_critical, q_high, q_medium, q_low]
        elif r < 0.85:
            return [q_high, q_critical, q_medium, q_low]
        elif r < 0.95:
            return [q_medium, q_critical, q_high, q_low]
        else:
            return [q_low, q_critical, q_high, q_medium]

    async def consume(self) -> Optional[QueuePayload]:
        """
        Block and pop a task using priority-weighted queues.
        """
        try:
            # First check for any delayed tasks that are due
            await self._process_delayed_tasks()

            queues_to_poll = self._get_poll_queues()
            # BLPOP blocks until a task is available on ANY of the queues
            result = await self.redis.blpop(queues_to_poll, timeout=self.fetch_timeout)
            if result:
                _, data = result
                return QueuePayload.model_validate_json(data)
            return None
        except Exception as e:
            logger.error(f"Error consuming task: {e}")
            return None

    def _get_priority_queue(self, priority_level: str) -> str:
        mapping = {
            "P1": f"{self.queue_name}:critical",
            "P2": f"{self.queue_name}:high",
            "P3": f"{self.queue_name}:medium",
            "P4": f"{self.queue_name}:low"
        }
        return mapping.get(priority_level.upper(), f"{self.queue_name}:medium")

    async def _process_delayed_tasks(self):
        """
        Move due delayed tasks from sorted set back to their respective priority queues.
        """
        try:
            now = time.time()
            # Get tasks where score (execute_at) <= now
            due_tasks = await self.redis.zrangebyscore(
                self.delayed_queue, 0, now, start=0, num=50
            )
            
            if due_tasks:
                from collections import defaultdict
                grouped = defaultdict(list)
                
                # Parse payloads to route them to the correct queue
                for task_json in due_tasks:
                    payload = QueuePayload.model_validate_json(task_json)
                    target_q = self._get_priority_queue(payload.priority_level)
                    grouped[target_q].append(task_json)
                    
                # Use a pipeline to ensure atomicity
                async with self.redis.pipeline(transaction=True) as pipe:
                    for target_q, data_list in grouped.items():
                        pipe.rpush(target_q, *data_list)
                    # Remove them from the delayed queue
                    pipe.zremrangebyscore(self.delayed_queue, 0, now)
                    await pipe.execute()
                    
                logger.debug(f"Moved {len(due_tasks)} delayed tasks back to active priority queues")
        except Exception as e:
            logger.error(f"Failed to process delayed tasks: {e}")

    async def send_to_dlq(self, payload: QueuePayload, reason: str):
        """
        Move a permanently failed task to the Dead Letter Queue.
        """
        try:
            # We can attach the reason to the payload or store it separately.
            data = payload.model_dump()
            data['_dlq_reason'] = reason
            data_str = json.dumps(data)
            
            await self.redis.rpush(self.dlq_name, data_str)
            logger.warning(f"Task {payload.task_id} sent to DLQ: {reason}")
            return True
        except Exception as e:
            logger.error(f"Failed to send task {payload.task_id} to DLQ: {e}")
            return False
