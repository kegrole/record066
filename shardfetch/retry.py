"""Single-URL HTTP fetch with bounded retries (stdlib only)."""

import socket
import urllib.error
import urllib.request


class FetchError(Exception):
    """Raised when every allowed attempt for one URL has failed."""


def http_fetch(url, timeout=1.0):
    """Perform one HTTP GET and return the response status code.

    Any transport/timeout/HTTP-level failure raises :class:`FetchError`.
    """
    request = urllib.request.Request(url, method="GET")
    try:
        response = urllib.request.urlopen(request, timeout=timeout)
    except urllib.error.HTTPError as exc:
        error = FetchError("HTTP %s for %s" % (exc.code, url))
        error.status = exc.code
        raise error from exc
    except (urllib.error.URLError, socket.timeout, TimeoutError, OSError) as exc:
        raise FetchError("%s for %s" % (exc, url)) from exc
    else:
        try:
            return response.status
        finally:
            response.close()


def fetch_with_retry(url, max_retries, timeout, fetch=http_fetch):
    """Fetch *url*, allowing *max_retries* retries (i.e. max_retries+1 tries).

    Returns ``(status, retries)`` where *retries* counts attempts beyond
    the first. Raises :class:`FetchError` once all attempts fail; the
    exception carries the final attempt's retry count and last status (if
    any response was received).
    """
    if max_retries < 0:
        raise ValueError("max_retries must be >= 0")
    last_error = None
    last_status = None
    for attempt in range(max_retries + 1):
        try:
            status = fetch(url, timeout=timeout)
            return status, attempt
        except FetchError as exc:
            last_error = exc
            last_status = getattr(exc, "status", None)
    error = FetchError(str(last_error))
    error.retries = max_retries
    error.status = last_status
    raise error
