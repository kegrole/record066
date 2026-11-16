"""Cooperative, thread-safe stop signal with signal-handler support."""

import signal
import threading


class StopEvent:
    """A stop flag that can be set programmatically or by OS signals.

    Workers poll :meth:`is_set` between tasks. Once set, no new task may
    start; tasks already in flight are allowed to finish.
    """

    def __init__(self):
        self._event = threading.Event()
        self._installed = {}

    def stop(self):
        """External stop interface; safe to call from any thread."""
        self._event.set()

    def is_set(self):
        return self._event.is_set()

    def wait(self, timeout=None):
        return self._event.wait(timeout)

    def install_signal_handlers(self, sigints=(signal.SIGINT, signal.SIGTERM)):
        """Install handlers that trigger a graceful stop (main thread only).

        Previously installed handlers are remembered so they can be restored
        with :meth:`restore_signal_handlers`.
        """
        for sig in sigints:
            previous = signal.getsignal(sig)
            self._installed[sig] = previous
            signal.signal(sig, self._handle_signal)

    def _handle_signal(self, signum, frame):
        self.stop()

    def restore_signal_handlers(self):
        for sig, previous in self._installed.items():
            signal.signal(sig, previous)
        self._installed.clear()
