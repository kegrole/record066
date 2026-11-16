"""Aggregate records into the required summary report."""

from .merge import merge_results

REPORT_FIELDS = (
    "total",
    "succeeded",
    "failed",
    "skipped",
    "peak_concurrency",
    "retries",
)


def build_report(records, peak_concurrency, total_retries, total=None):
    """Build a report dict; success/failure/skipped are counted separately."""
    if total is None:
        total = len(records)
    ordered = merge_results(records, total)
    succeeded = sum(1 for record in ordered if record["outcome"] == "succeeded")
    failed = sum(1 for record in ordered if record["outcome"] == "failed")
    skipped = sum(1 for record in ordered if record["outcome"] == "skipped")
    if succeeded + failed + skipped != total:
        raise ValueError("outcome counts do not add up to total")
    report = {
        "total": total,
        "succeeded": succeeded,
        "failed": failed,
        "skipped": skipped,
        "peak_concurrency": peak_concurrency,
        "retries": total_retries,
        "results": ordered,
    }
    return report
