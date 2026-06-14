import os
os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///./debug_test.db"

import asyncio
import uuid
import logging
import fakeredis.aioredis
from executor.task_queue.redis_client import RedisClient
from executor.task_queue.publisher import QueuePublisher
from executor.worker_manager.manager import WorkerPoolManager
from executor.persistence.database import engine, AsyncSessionLocal, Base
from executor.persistence.models import Scan, Task, TaskStatus
from executor.task_queue.models import QueuePayload
from sqlalchemy import select

logging.basicConfig(level=logging.INFO)

async def main():
    # create tables if missing
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # patch Redis to use fakeredis
    fake_redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    RedisClient._use_fake = True
    RedisClient._fake_client = fake_redis

    async with AsyncSessionLocal() as session:
        scan = Scan(name='debug-scan', target='http://example.com', config={})
        session.add(scan)
        await session.commit()
        await session.refresh(scan)
        tasks = []
        for i in range(3):
            t = Task(scan_id=scan.id, method='GET', url=f'http://example.com/api/{i}', status=TaskStatus.QUEUED.value, max_retries=0)
            session.add(t)
            tasks.append(t)
        await session.commit()
        for t in tasks:
            await session.refresh(t)

    publisher = QueuePublisher('tasks:debug')
    # publish tasks with payloads
    async with AsyncSessionLocal() as session:
        for t in tasks:
            payload = QueuePayload(task_id=str(t.id), scan_id=str(t.scan_id), method=t.method, url=t.url, headers=t.headers, payload=t.payload, max_retries=t.max_retries, priority_level='P3')
            await publisher.publish(payload)

    manager = WorkerPoolManager('tasks:debug', min_workers=1, max_workers=2, idle_timeout=5)
    # patch rate limiter acquires and http executor to return immediate success
    from executor.rate_limiter.limiter import RateLimiter
    from executor.workers.http_executor import HttpExecutor
    original_acquire = RateLimiter.acquire
    original_execute = HttpExecutor.execute
    async def mock_acquire(self, url, scan_id):
        return True
    async def mock_execute(self, method, url, headers=None, payload=None):
        return 200, 10.0, {'content-type': 'application/json'}, '{"success": true}'
    RateLimiter.acquire = mock_acquire
    HttpExecutor.execute = mock_execute

    task = asyncio.create_task(manager.start())
    await asyncio.sleep(3)
    # stop manager
    await manager.stop()
    await RedisClient.close()
    # inspect tasks
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Task))
        rows = result.scalars().all()
        for row in rows:
            print(f"Task {row.id} status={row.status} attempts={row.attempts}")

if __name__ == '__main__':
    asyncio.run(main())
