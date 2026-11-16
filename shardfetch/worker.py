"""Worker threads, global concurrency limiting, and the fetch facade."""

import threading

from .retry import FetchError, fetch_with_retry, http_fetch
from .sharding import make_shards


class ConcurrencyLimiter:
    """Bounded semaphore plus an observed in-flight peak counter."""

    def __init__(self, limit):
        if limit < 1:
            raise ValueError("concurrency limit must be >= 1")
        self._semaphore = threading.BoundedSemaphore(limit)
        self._in_flight = 0
        self._peak = 0
        self._lock = threading.Lock()

    def __enter__(self):
        self._semaphore.acquire()
        with self._lock:
            self._in_flight += 1
            if self._in_flight > self._peak:
                self._peak = self._in_flight
        return self

    def __exit__(self, exc_type, exc, tb):
        with self._lock:
            self._in_flight -= 1
        self._semaphore.release()
        return False

    @property
    def peak(self):
        with self._lock:
            return self._peak


class ShardFetcher:
    """Fetch a list of URLs using one shard per worker thread.

    The number of worker threads (shards) and the global concurrency limit
    are independent: all workers share one :class:`ConcurrencyLimiter`, so
    the server-visible in-flight peak never exceeds *concurrency*.
    """

    def __init__(
        self,
        urls,
        workers=4,
        max_retries=3,
        timeout=1.0,
        concurrency=None,
        stop_event=None,
        fetch=None,
    ):
        self.urls = list(urls)
        self.workers = workers
        self.max_retries = max_retries
        self.timeout = timeout
        self.concurrency = workers if concurrency is None else concurrency
        self.stop_event = stop_event
        self._fetch = http_fetch if fetch is None else fetch
        self._limiter = ConcurrencyLimiter(self.concurrency)
        self._records = [None] * len(self.urls)
        self._total_retries = 0
        self._retries_lock = threading.Lock()

    def stop(self):
        """External stop interface (also usable without injecting an event)."""
        if self.stop_event is None:
            raise RuntimeError("no StopEvent was injected")
        self.stop_event.stop()

    def run(self):
        """Run all shards and return the input-ordered list of records."""
        shards = make_shards(self.urls, self.workers)
        threads = []
        for worker_id, shard in enumerate(shards):
            thread = threading.Thread(
                target=self._worker, args=(worker_id, shard), name="shard-%d" % worker_id
            )
            thread.daemon = True
            threads.append(thread)
            thread.start()
        for thread in threads:
            thread.join()
        return list(self._records)

    def _worker(self, worker_id, shard):
        for index, url in shard:
            if self.stop_event is not None and self.stop_event.is_set():
                # No new task may start after the stop signal; remaining
                # URLs in this shard are recorded as skipped.
                self._fill_skipped(shard, index)
                return
            record = self._fetch_one(index, url)
            self._records[index] = record

    def _fill_skipped(self, shard, from_index):
        for index, _url in shard:
            if index >= from_index:
                self._records[index] = {
                    "index": index,
                    "url": self.urls[index],
                    "status": None,
                    "retries": 0,
                    "outcome": "skipped",
                    "error": None,
                }

    def _fetch_one(self, index, url):
        with self._limiter:
            # Re-check inside the limiter: a signal may have arrived while
            # queued, in which case this task must not start either.
            if self.stop_event is not None and self.stop_event.is_set():
                return {
                    "index": index,
                    "url": url,
                    "status": None,
                    "retries": 0,
                    "outcome": "skipped",
                    "error": None,
                }
            try:
                status, retries = fetch_with_retry(
                    url,
                    self.max_retries,
                    self.timeout,
                    fetch=self._fetch,
                )
            except FetchError as exc:
                retries = getattr(exc, "retries", self.max_retries)
                with self._retries_lock:
                    self._total_retries += retries
                return {
                    "index": index,
                    "url": url,
                    "status": getattr(exc, "status", None),
                    "retries": retries,
                    "outcome": "failed",
                    "error": str(exc),
                }
            with self._retries_lock:
                self._total_retries += retries
            return {
                "index": index,
                "url": url,
                "status": status,
                "retries": retries,
                "outcome": "succeeded",
                "error": None,
            }

    @property
    def peak_concurrency(self):
        return self._limiter.peak

    @property
    def total_retries(self):
        with self._retries_lock:
            return self._total_retries
