"""Merge per-task records back into strict input order."""


def merge_results(records, expected_total):
    """Validate and order records by their original input index.

    Merging is independent of completion/thread order: every record carries
    its own input index, and the returned list is sorted by that index. The
    indices must form a contiguous 0..expected_total-1 sequence with exactly
    one record per input URL.
    """
    if records is None or any(record is None for record in records):
        raise ValueError("every input URL must have a corresponding record")
    ordered = sorted(records, key=lambda record: record["index"])
    indices = [record["index"] for record in ordered]
    if indices != list(range(expected_total)):
        raise ValueError(
            "merged indices must be contiguous 0..%d, got %r"
            % (expected_total - 1, indices)
        )
    return ordered
