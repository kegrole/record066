"""Deterministic sharding of an input list across worker threads."""


def make_shards(items, workers):
    """Split *items* into *workers* contiguous, index-preserving shards.

    Every original item keeps its global input index inside its
    ``(index, item)`` tuple, so downstream merging never depends on the
    order in which shards happen to finish.

    Empty trailing shards are returned as empty lists rather than dropped,
    so the number of shards always equals *workers* (which must be >= 1).
    """
    if workers < 1:
        raise ValueError("workers must be >= 1")
    total = len(items)
    base, extra = divmod(total, workers)
    shards = []
    start = 0
    for worker_id in range(workers):
        size = base + (1 if worker_id < extra else 0)
        end = start + size
        shards.append([(index, items[index]) for index in range(start, end)])
        start = end
    return shards
