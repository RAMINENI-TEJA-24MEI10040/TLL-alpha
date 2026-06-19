import asyncio
import logging
from typing import List, Dict
from executor.task_queue.redis_client import RedisClient
from executor.workers.engine import WorkerEngine
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

    async def _cleanup_idle_workers(self):
        """Terminates workers that are idle beyond threshold, respecting min_workers."""
        idle_engines = [e for e in self.active_engines if e.is_idle(self.idle_timeout)]
        
        for engine in idle_engines:
            if len(self.active_engines) <= self.min_workers:
                break # Reached floor
                
            logger.info(f"Worker {engine.worker_id} idle for >{self.idle_timeout}s. Scaling DOWN.")
            await engine.stop()
            self.active_engines.remove(engine)
