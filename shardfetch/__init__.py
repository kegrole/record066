"""shardfetch: sharded, concurrency-limited, retry-aware URL fetcher.

Only the Python standard library is used.
"""

from .stop import StopEvent
from .worker import ShardFetcher

__all__ = ["StopEvent", "ShardFetcher"]
