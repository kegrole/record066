"""unittest coverage for every acceptance criterion of shardfetch.

Run from the repository root with either::

    python -m unittest test_shardfetch -v
    python -m unittest tests.test_shardfetch -v
"""

import json
import os
import random
import signal
import subprocess
import sys
import threading
import time
import unittest
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from demo.mock_site import FAIL_IDS, TIMEOUT_IDS, MockSiteServer  # noqa: E402
from shardfetch.merge import merge_results  # noqa: E402
from shardfetch.report import REPORT_FIELDS, build_report  # noqa: E402
from shardfetch.sharding import make_shards  # noqa: E402
from shardfetch.stop import StopEvent  # noqa: E402
from shardfetch.worker import ShardFetcher  # noqa: E402

TEST_PORT = 8123
STOP_PORT = 8124
N_URLS = 60
WORKERS = 4
MAX_RETRIES = 3
TIMEOUT = 0.2


def url_for(port, index):
    return "http://127.0.0.1:%d/u/%04d" % (port, index)


def make_urls(port, count=N_URLS):
    return [url_for(port, index) for index in range(count)]


def path_of(url):
    return "/" + url.split("/", 3)[-1]


def fetch_once(urls, port, concurrency=None, stop_event=None, workers=WORKERS,
               max_retries=MAX_RETRIES, timeout=TIMEOUT):
    fetcher = ShardFetcher(
        urls,
        workers=workers,
        max_retries=max_retries,
        timeout=timeout,
        concurrency=concurrency,
        stop_event=stop_event,
    )
    records = fetcher.run()
    report = build_report(
        records, fetcher.peak_concurrency, fetcher.total_retries, len(urls)
    )
    return fetcher, report, records


class ShardFetchTestCase(unittest.TestCase):
    server = None

    @classmethod
    def setUpClass(cls):
        cls.server = MockSiteServer(TEST_PORT, delay=0.0).start()
        for _ in range(100):
            try:
                urllib.request.urlopen(
                    "http://127.0.0.1:%d/stats" % TEST_PORT, timeout=0.5
                ).close()
                break
            except OSError:
                time.sleep(0.05)

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()

    def setUp(self):
        self.server.reset_stats()

    # Criteria 1 & 2: 60 URLs, 3 permanent 500s + 2 permanent timeouts,
    # exactly 55 successes and 5 failures.
    def test_full_run_counts_success_and_failure(self):
        urls = make_urls(TEST_PORT)
        _fetcher, report, records = fetch_once(urls, TEST_PORT)

        self.assertEqual(report["total"], 60)
        self.assertEqual(report["succeeded"], 55)
        self.assertEqual(report["failed"], 5)
        self.assertEqual(report["skipped"], 0)

        failed_records = [r for r in records if r["outcome"] == "failed"]
        failed_ids = sorted(
            int(r["url"].rsplit("/", 1)[-1]) for r in failed_records
        )
        self.assertEqual(failed_ids, sorted(FAIL_IDS | TIMEOUT_IDS))
        for record in failed_records:
            url_id = int(record["url"].rsplit("/", 1)[-1])
            if url_id in FAIL_IDS:
                self.assertEqual(record["status"], 500)
            else:
                self.assertIn(url_id, TIMEOUT_IDS)
                self.assertIsNone(record["status"])

    # Criterion 3: server-observed peak respects the global limit and is
    # independent of the worker count (4 workers but limit 2 -> peak <= 2).
    def test_peak_concurrency_limited_independently_of_workers(self):
        urls = make_urls(TEST_PORT)

        _f, report_four, _r = fetch_once(urls, TEST_PORT, concurrency=4, workers=4)
        server_peak_four = self.server.stats()["peak_concurrency"]
        self.assertLessEqual(report_four["peak_concurrency"], 4)
        self.assertLessEqual(server_peak_four, 4)
        self.assertGreaterEqual(server_peak_four, 1)

        self.server.reset_stats()
        _f, report_two, _r = fetch_once(urls, TEST_PORT, concurrency=2, workers=4)
        server_peak_two = self.server.stats()["peak_concurrency"]
        self.assertLessEqual(report_two["peak_concurrency"], 2)
        self.assertLessEqual(server_peak_two, 2)
        self.assertGreaterEqual(server_peak_two, 1)

    # Criterion 4: each failed URL retries exactly 3 times (4 attempts);
    # accumulated retries are exactly 15.
    def test_retries_are_bounded_and_counted(self):
        urls = make_urls(TEST_PORT)
        _fetcher, report, records = fetch_once(urls, TEST_PORT)

        failed_records = [r for r in records if r["outcome"] == "failed"]
        self.assertEqual(len(failed_records), 5)
        for record in failed_records:
            self.assertEqual(record["retries"], 3)
        self.assertEqual(report["retries"], 15)
        self.assertEqual(sum(r["retries"] for r in records), 15)

        counts = self.server.stats()["request_counts"]
        for record in failed_records:
            self.assertEqual(counts[path_of(record["url"])], 4)
        for record in records:
            if record["outcome"] == "succeeded":
                self.assertEqual(counts[path_of(record["url"])], 1)

    # Criterion 5: merged indices are contiguous 0..59, one record per URL,
    # and results align by URL across a shuffled rerun.
    def test_merge_order_and_shuffle_stability(self):
        urls = make_urls(TEST_PORT)
        _f, _report, records = fetch_once(urls, TEST_PORT)

        ordered = merge_results(records, len(urls))
        self.assertEqual([r["index"] for r in ordered], list(range(60)))
        self.assertEqual(len(ordered), 60)
        for index, record in enumerate(ordered):
            self.assertEqual(record["index"], index)
            self.assertEqual(record["url"], urls[index])

        shuffled = list(urls)
        random.Random(20261116).shuffle(shuffled)
        _f2, _r2, shuffled_records = fetch_once(shuffled, TEST_PORT)
        by_url_original = {r["url"]: r for r in ordered}
        by_url_shuffled = {r["url"]: r for r in shuffled_records}
        self.assertEqual(set(by_url_original), set(by_url_shuffled))
        for url, original in by_url_original.items():
            other = by_url_shuffled[url]
            self.assertEqual(other["url"], original["url"])
            self.assertEqual(other["status"], original["status"])
            self.assertEqual(other["retries"], original["retries"])
            self.assertEqual(other["outcome"], original["outcome"])
        shuffled_ordered = merge_results(shuffled_records, len(shuffled))
        self.assertEqual([r["index"] for r in shuffled_ordered], list(range(60)))
        self.assertEqual([r["url"] for r in shuffled_ordered], shuffled)

    # Criterion 6: mid-run external stop; never-contacted URLs have zero
    # server requests and skipped == 60 - distinct contacted URLs.
    def test_external_stop_marks_untouched_urls_skipped(self):
        delay = 0.15
        server = MockSiteServer(STOP_PORT, delay=delay).start()
        self.addCleanup(server.stop)
        urls = make_urls(STOP_PORT)
        stop_event = StopEvent()
        fetcher = ShardFetcher(
            urls,
            workers=4,
            max_retries=0,
            timeout=2.0,
            concurrency=2,
            stop_event=stop_event,
        )

        def trigger_stop():
            time.sleep(delay * 1.2)
            stop_event.stop()

        threading.Thread(target=trigger_stop, daemon=True).start()
        records = fetcher.run()
        report = build_report(
            records, fetcher.peak_concurrency, fetcher.total_retries, len(urls)
        )

        counts = server.stats()["request_counts"]
        contacted = set(counts)
        self.assertGreater(len(contacted), 0)
        self.assertLess(len(contacted), 60)
        skipped_records = [r for r in records if r["outcome"] == "skipped"]
        self.assertTrue(skipped_records)
        for record in skipped_records:
            self.assertEqual(counts.get(path_of(record["url"]), 0), 0)
        self.assertEqual(len(skipped_records), 60 - len(contacted))
        self.assertEqual(report["skipped"], 60 - len(contacted))
        self.assertEqual(
            report["succeeded"] + report["failed"] + report["skipped"],
            report["total"],
        )
        self.assertEqual(
            merge_results(records, 60),
            sorted(records, key=lambda r: r["index"]),
        )

    # Criterion 7: the six required report fields exist and counts add up.
    def test_report_fields_and_invariant(self):
        urls = make_urls(TEST_PORT)
        _fetcher, report, _records = fetch_once(urls, TEST_PORT)
        for field in REPORT_FIELDS:
            self.assertIn(field, report)
        self.assertEqual(report["total"], 60)
        self.assertEqual(report["succeeded"], 55)
        self.assertEqual(report["failed"], 5)
        self.assertEqual(report["skipped"], 0)
        self.assertEqual(report["retries"], 15)
        self.assertEqual(
            report["succeeded"] + report["failed"] + report["skipped"],
            report["total"],
        )

    def test_sharding_preserves_global_indices(self):
        urls = make_urls(TEST_PORT)
        shards = make_shards(urls, 4)
        self.assertEqual(len(shards), 4)
        flattened = [item for shard in shards for item in shard]
        self.assertEqual([index for index, _url in flattened], list(range(60)))
        self.assertEqual([url for _index, url in flattened], urls)
        self.assertEqual([len(s) for s in make_shards(list(range(7)), 4)], [2, 2, 2, 1])
        self.assertEqual(make_shards([], 3), [[], [], []])

    def test_stop_event_programmatic_and_signal(self):
        stop_event = StopEvent()
        self.assertFalse(stop_event.is_set())
        stop_event.stop()
        self.assertTrue(stop_event.is_set())

        signal_event = StopEvent()
        signal_event.install_signal_handlers((signal.SIGUSR1,))
        try:
            os.kill(os.getpid(), signal.SIGUSR1)
            time.sleep(0.05)
            self.assertTrue(signal_event.is_set())
        finally:
            signal_event.restore_signal_handlers()


class CliDemoTestCase(unittest.TestCase):
    # End-to-end demo command: CLI auto-launches and tears down mock_site.py.
    def test_demo_command_runs_locally_and_writes_report(self):
        out_path = os.path.join(ROOT, "out", "cli_test_report.json")
        if os.path.exists(out_path):
            os.remove(out_path)
        cmd = [
            sys.executable, "-m", "shardfetch.cli",
            "--urls", "demo/urls.txt",
            "--workers", "4",
            "--concurrency", "4",
            "--retries", "3",
            "--timeout", "0.2",
            "--out", out_path,
        ]
        completed = subprocess.run(
            cmd, cwd=ROOT, capture_output=True, text=True, timeout=60
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("total=60", completed.stdout)
        self.assertIn("succeeded=55", completed.stdout)
        self.assertIn("failed=5", completed.stdout)
        self.assertIn("peak_concurrency", completed.stdout)
        with open(out_path, "r", encoding="utf-8") as handle:
            report = json.load(handle)
        self.assertEqual(report["total"], 60)
        self.assertEqual(report["succeeded"], 55)
        self.assertEqual(report["failed"], 5)
        self.assertEqual(report["retries"], 15)
        self.assertEqual(len(report["results"]), 60)
