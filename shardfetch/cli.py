"""Command-line entry point with optional local mock-site subprocess."""

import argparse
import json
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request

from .report import build_report
from .stop import StopEvent
from .worker import ShardFetcher

MOCK_PORT = 8099


def load_urls(path):
    with open(path, "r", encoding="utf-8") as handle:
        return [line.strip() for line in handle if line.strip()]


def wait_for_port(port, timeout=10.0):
    """Block until the local mock server answers (or *timeout* elapses)."""
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(
                "http://127.0.0.1:%d/stats" % port, timeout=0.5
            ) as response:
                if response.status == 200:
                    return True
        except (urllib.error.URLError, OSError) as exc:
            last_error = exc
            time.sleep(0.1)
    raise RuntimeError("mock server on port %d did not start: %s" % (port, last_error))


def start_mock_if_requested(urls_path, use_mock):
    """Launch mock_site.py located next to *urls_path* in a child process."""
    if not use_mock:
        return None
    directory = os.path.dirname(os.path.abspath(urls_path))
    mock_path = os.path.join(directory, "mock_site.py")
    if not os.path.exists(mock_path):
        return None
    process = subprocess.Popen(
        [sys.executable, mock_path],
        cwd=directory,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        wait_for_port(MOCK_PORT)
    except Exception:
        process.terminate()
        process.wait(timeout=5)
        raise
    return process


def stop_mock(process):
    if process is None:
        return
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        prog="shardfetch", description="Sharded, concurrency-limited URL fetcher."
    )
    parser.add_argument("--urls", required=True, help="text file with one URL per line")
    parser.add_argument("--workers", type=int, default=4, help="worker thread count")
    parser.add_argument(
        "--concurrency",
        type=int,
        default=None,
        help="global in-flight concurrency limit (default: same as --workers)",
    )
    parser.add_argument("--retries", type=int, default=3, help="max retries per URL")
    parser.add_argument("--timeout", type=float, default=1.0, help="per-request timeout seconds")
    parser.add_argument("--out", default="out/report.json", help="report output path")
    parser.add_argument(
        "--no-mock",
        action="store_true",
        help="do not auto-launch mock_site.py next to the urls file",
    )
    return parser.parse_args(argv)


def run_fetch(urls, args):
    stop_event = StopEvent()
    stop_event.install_signal_handlers((signal.SIGINT, signal.SIGTERM))
    fetcher = ShardFetcher(
        urls,
        workers=args.workers,
        max_retries=args.retries,
        timeout=args.timeout,
        concurrency=args.concurrency,
        stop_event=stop_event,
    )
    try:
        records = fetcher.run()
    finally:
        stop_event.restore_signal_handlers()
    return build_report(records, fetcher.peak_concurrency, fetcher.total_retries, len(urls))


def write_report(report, out_path):
    out_dir = os.path.dirname(os.path.abspath(out_path))
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as handle:
        json.dump(report, handle, ensure_ascii=False, indent=2)


def main(argv=None):
    args = parse_args(argv)
    urls = load_urls(args.urls)
    mock_process = start_mock_if_requested(args.urls, not args.no_mock)
    try:
        report = run_fetch(urls, args)
        write_report(report, args.out)
    finally:
        stop_mock(mock_process)
    print(
        "total=%d succeeded=%d failed=%d skipped=%d peak_concurrency=%d retries=%d"
        % (
            report["total"],
            report["succeeded"],
            report["failed"],
            report["skipped"],
            report["peak_concurrency"],
            report["retries"],
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
