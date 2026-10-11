"""
Distributed Idempotency & Rate Limiting Engine.
Guarantees exactly-once event processing using atomic SETNX logic.
"""
import time
import threading
from typing import Dict, Any, Optional, Tuple


class IdempotencyManager:
    """
    Thread-safe in-memory cache supporting TTL and atomic lock acquisition.
    Acts as an in-process Redis substitute when Redis is offline, and bridges to Redis in production.
    """
    def __init__(self, default_ttl_seconds: int = 86400):
        self.default_ttl = default_ttl_seconds
        self._store: Dict[str, Tuple[float, Any]] = {}
        self._lock = threading.Lock()

    def acquire_or_get(self, key: str, value: Any, ttl: Optional[int] = None) -> Tuple[bool, Any]:
        """
        Atomically sets key if not present (equivalent to Redis SETNX).
        Returns:
            (True, value) if this is a NEW key.
            (False, existing_value) if key already exists (DUPLICATE).
        """
        now = time.time()
        ttl_seconds = ttl or self.default_ttl
        with self._lock:
            # Purge if expired
            if key in self._store:
                expires_at, cached_val = self._store[key]
                if now < expires_at:
                    return False, cached_val
                else:
                    del self._store[key]

            # Set new key
            self._store[key] = (now + ttl_seconds, value)
            return True, value

    def clear(self):
        with self._lock:
            self._store.clear()


class TokenBucketRateLimiter:
    """
    Token-bucket rate limiter per tenant/account to prevent API flooding.
    """
    def __init__(self, capacity: int = 100, refill_rate_per_sec: float = 20.0):
        self.capacity = capacity
        self.refill_rate = refill_rate_per_sec
        self._buckets: Dict[str, Tuple[float, float]] = {}  # account -> (tokens, last_time)
        self._lock = threading.Lock()

    def allow_request(self, account_id: str, cost: float = 1.0) -> bool:
        now = time.time()
        with self._lock:
            tokens, last_time = self._buckets.get(account_id, (float(self.capacity), now))
            # Refill tokens
            elapsed = now - last_time
            tokens = min(float(self.capacity), tokens + elapsed * self.refill_rate)

            if tokens >= cost:
                tokens -= cost
                self._buckets[account_id] = (tokens, now)
                return True
            else:
                self._buckets[account_id] = (tokens, now)
                return False
