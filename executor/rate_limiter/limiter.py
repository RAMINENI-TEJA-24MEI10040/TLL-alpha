import time
import asyncio
import logging
from urllib.parse import urlparse
from executor.task_queue.redis_client import RedisClient
from executor.rate_limiter.config import RateLimiterConfigLoader

logger = logging.getLogger(__name__)

# Redis Lua Script for Atomic Token Bucket
# ARGV[1] = capacity, ARGV[2] = refill_rate, ARGV[3] = now, ARGV[4] = requested
TOKEN_BUCKET_SCRIPT = """
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

local bucket = redis.call("HMGET", key, "tokens", "last_refill", "adaptive_multiplier")
local tokens = tonumber(bucket[1])
local last_refill = tonumber(bucket[2])
local multiplier = tonumber(bucket[3]) or 1.0

if not tokens then
    tokens = capacity
    last_refill = now
end

-- Apply adaptive multiplier to refill rate (if 429 was detected)
local effective_rate = refill_rate * multiplier
local time_passed = math.max(0, now - last_refill)
local new_tokens = time_passed * effective_rate

tokens = math.min(capacity, tokens + new_tokens)

if tokens >= requested then
    tokens = tokens - requested
    redis.call("HMSET", key, "tokens", tokens, "last_refill", now, "adaptive_multiplier", multiplier)
    redis.call("EXPIRE", key, math.ceil(capacity / effective_rate) * 2)
    return 1
else
    redis.call("HMSET", key, "tokens", tokens, "last_refill", now, "adaptive_multiplier", multiplier)
    return 0
end
"""

class RateLimiter:
    def __init__(self):
        self.redis = RedisClient.get_client()
        self.config_loader = RateLimiterConfigLoader()
        self.script_sha = None

    async def _load_script(self):
        if not self.script_sha:
            self.script_sha = await self.redis.script_load(TOKEN_BUCKET_SCRIPT)

    async def acquire(self, url: str, scan_id: str, tokens: int = 1, timeout: int = 30) -> bool:
        """
        Wait until tokens are available for global, domain, and scan.
        """
        await self._load_script()
        
        domain = urlparse(url).netloc
        config = self.config_loader.config
        
        domain_rate = config.domains.get(domain, None)
        global_rate = config.global_limit
        
        # We define a capacity equal to the rate (1 second burst)
        keys_and_args = []
        now = time.time()
        
        # 1. Global Bucket
        keys_and_args.append(("rl:global", global_rate, global_rate, now, tokens))
        
        # 2. Domain Bucket (if configured)
        if domain_rate:
            keys_and_args.append((f"rl:domain:{domain}", domain_rate, domain_rate, now, tokens))
            
        # 3. Scan Bucket (arbitrary 100 req/sec limit per scan by default to prevent a single scan from hogging)
        scan_rate = 100.0
        keys_and_args.append((f"rl:scan:{scan_id}", scan_rate, scan_rate, now, tokens))
        
        start_time = time.time()
        while time.time() - start_time < timeout:
            all_acquired = True
            
            # We must acquire all buckets. If we fail one, we must wait and retry.
            # In a strict implementation, we would rollback, but for high throughput 
            # optimistic checking is fine, or pipeline eval.
            for key, cap, rate, current_time, req in keys_and_args:
                res = await self.redis.evalsha(self.script_sha, 1, key, cap, rate, time.time(), req)
                if res == 0:
                    all_acquired = False
                    break
                    
            if all_acquired:
                return True
                
            # Wait before retrying (adaptive sleep)
            await asyncio.sleep(0.1)
            
        logger.warning(f"RateLimiter timed out after {timeout}s for URL: {url}")
        return False

    async def report_429(self, url: str):
        """
        Adaptive Rate Limiting: Reduce the domain's multiplier on 429 response.
        """
        domain = urlparse(url).netloc
        key = f"rl:domain:{domain}"
        
        # Halve the multiplier (min 0.1)
        bucket = await self.redis.hmget(key, "adaptive_multiplier")
        multiplier = float(bucket[0]) if bucket[0] else 1.0
        
        new_multiplier = max(0.1, multiplier * 0.5)
        await self.redis.hset(key, "adaptive_multiplier", new_multiplier)
        
        # Set a cooldown key to slowly recover it (implemented in a background job normally)
        logger.warning(f"Detected 429 for {domain}. Halved multiplier to {new_multiplier}")
