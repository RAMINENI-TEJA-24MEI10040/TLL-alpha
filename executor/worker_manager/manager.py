import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict
from executor.task_queue.redis_client import RedisClient
from executor.task_queue.models import QueuePayload
from executor.task_queue.publisher import QueuePublisher
from executor.workers.engine import WorkerEngine
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from executor.persistence.database import AsyncSessionLocal
from executor.persistence.models import Task, TaskStatus, ScanStatus
from executor.metrics.prometheus import EXECUTOR_ACTIVE_WORKERS, EXECUTOR_QUEUE_DEPTH

logger = logging.getLogger(__name__)

class WorkerPoolManager:
    """
    Dynamically scales WorkerEngines based on Queue Depth.
    """
    def __init__(self, queue_base_name: str = "tasks:default", min_workers: int = 5, max_workers: int = 100, idle_timeout: int = 60):
        self.queue_base_name = queue_base_name
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.idle_timeout = idle_timeout
        self.redis = RedisClient.get_client()
        
        self.active_engines: List[WorkerEngine] = []
        self._running = False
        self._monitor_task = None
        
    async def start(self):
        """Start the auto-scaling pool manager."""
        self._running = True
        logger.info(f"Starting WorkerPoolManager (min: {self.min_workers}, max: {self.max_workers})")

        # Recover any stale tasks left in PROCESSING state before starting workers
        await self._recover_stale_tasks()
        
        # Start minimum workers
        await self._scale_up(self.min_workers)
        
        # Start background monitor loop
        self._monitor_task = asyncio.create_task(self._monitor_loop())

    async def stop(self):
        """Graceful shutdown of all workers."""
        self._running = False
        if self._monitor_task:
            self._monitor_task.cancel()
            
        logger.info(f"Shutting down {len(self.active_engines)} active worker engines...")
        shutdown_tasks = [engine.stop() for engine in self.active_engines]
        await asyncio.gather(*shutdown_tasks, return_exceptions=True)
        self.active_engines.clear()
        
    async def _get_total_queue_depth(self) -> int:
        """Calculates total pending tasks across all priority queues."""
        queues = [
            f"{self.queue_base_name}:critical",
            f"{self.queue_base_name}:high",
            f"{self.queue_base_name}:medium",
            f"{self.queue_base_name}:low",
        ]
        total = 0
        for q in queues:
            depth = await self.redis.llen(q)
            priority_label = q.split(":")[-1]
            EXECUTOR_QUEUE_DEPTH.labels(priority=priority_label).set(depth)
            total += depth
        return total

    async def _monitor_loop(self):
        """Background loop evaluating policies."""
        iteration = 0
        while self._running:
            try:
                queue_depth = await self._get_total_queue_depth()
                current_workers = len(self.active_engines)
                
                # Cleanup idle workers first
                await self._cleanup_idle_workers()
                current_workers = len(self.active_engines)
                
                # Calculate desired workers based on policy
                desired_workers = self._calculate_desired_workers(queue_depth)
                
                if current_workers < desired_workers:
                    to_add = min(desired_workers - current_workers, self.max_workers - current_workers)
                    if to_add > 0:
                        logger.info(f"Queue depth {queue_depth}. Scaling UP: adding {to_add} workers.")
                        await self._scale_up(to_add)
                
                # Update Prometheus metrics
                EXECUTOR_ACTIVE_WORKERS.set(len(self.active_engines))
                
                # Periodically recover stale tasks (every 30 seconds)
                iteration += 1
                if iteration % 6 == 0:
                    await self._recover_stale_tasks()
                
            except Exception as e:
                logger.error(f"Error in ScalingEngine loop: {e}")
                
            await asyncio.sleep(5)  # Check every 5 seconds

    def _calculate_desired_workers(self, queue_depth: int) -> int:
        """
        Policy mapping queue depth to worker count.
        10 tasks -> 5 workers
        100 tasks -> 10 workers
        1000 tasks -> 50 workers
        10000 tasks -> 100 workers
        """
        if queue_depth >= 10000:
            return 100
        elif queue_depth >= 1000:
            return 50
        elif queue_depth >= 100:
            return 10
        elif queue_depth >= 10:
            return 5
        else:
            return self.min_workers

    async def _scale_up(self, count: int):
        """Spawns 'count' new WorkerEngines."""
        for _ in range(count):
            if len(self.active_engines) >= self.max_workers:
                break
            engine = WorkerEngine(queue_name=self.queue_base_name)
            self.active_engines.append(engine)
            # Run in background
            asyncio.create_task(engine.start())

    async def _count_active_heartbeats(self) -> int:
        """Count active worker heartbeat keys in Redis."""
        count = 0
        try:
            async for _ in self.redis.scan_iter(match="worker:heartbeat:*"):
                count += 1
        except Exception as e:
            logger.warning(f"Failed to count active heartbeats: {e}")
        return count

    async def _recover_stale_tasks(self):
        """Recover stale tasks left in PROCESSING state after a previous crash or restart."""
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=max(self.idle_timeout * 3, 60))
        recovered = 0

        async with AsyncSessionLocal() as session:
            stmt = select(Task).options(selectinload(Task.scan)).where(
                Task.status == TaskStatus.PROCESSING.value,
                Task.updated_at < cutoff
            )
            result = await session.execute(stmt)
            stale_tasks = result.scalars().all()

            if not stale_tasks:
                return

            publisher = QueuePublisher(queue_name=self.queue_base_name)
            for task in stale_tasks:
                if task.scan and task.scan.status in (ScanStatus.RUNNING.value, ScanStatus.PENDING.value):
                    task.status = TaskStatus.QUEUED.value
                    payload = QueuePayload(
                        task_id=str(task.id),
                        scan_id=str(task.scan_id),
                        method=task.method,
                        url=task.url,
                        headers=task.headers,
                        payload=task.payload,
                        max_retries=task.max_retries,
                        priority_level="P3"
                    )
                    await publisher.publish(payload)
                    recovered += 1
                else:
                    task.status = TaskStatus.FAILED.value
                    logger.info(f"Marked stale task {task.id} as FAILED because scan status is {task.scan.status if task.scan else 'None'}")

            await session.commit()

        if recovered > 0:
            logger.info(f"Recovered and requeued {recovered} stale PROCESSING task(s).")

    async def _cleanup_idle_workers(self):
        """Terminates workers that are idle beyond threshold, respecting min_workers."""
        idle_engines = [e for e in self.active_engines if e.is_idle(self.idle_timeout)]
        
        for engine in idle_engines:
            if len(self.active_engines) <= self.min_workers:
                break # Reached floor
                
            logger.info(f"Worker {engine.worker_id} idle for >{self.idle_timeout}s. Scaling DOWN.")
            await engine.stop()
            self.active_engines.remove(engine)
