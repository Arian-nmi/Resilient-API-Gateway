import time
from collections import deque


class CircuitBreaker:
    """closed -> open (error rate over a sliding window) -> half_open (one probe) -> closed/open."""

    def __init__(self, window: int, min_calls: int, threshold: float, open_seconds: float):
        self.results: deque[bool] = deque(maxlen=window)
        self.min_calls = min_calls
        self.threshold = threshold
        self.open_seconds = open_seconds
        self.state = "closed"
        self.opened_at = 0.0
        self.probing = False

    def allow(self) -> bool:
        if self.state == "open":
            if time.monotonic() - self.opened_at < self.open_seconds:
                return False
            self.state = "half_open"
            self.probing = False
        if self.state == "half_open":
            if self.probing:
                return False
            self.probing = True
        return True

    def record(self, ok: bool) -> None:
        if self.state == "open":  # stale result from a request started before opening
            return
        if self.state == "half_open":
            self.probing = False
            if ok:
                self.state = "closed"
                self.results.clear()
            else:
                self._open()
            return
        self.results.append(ok)
        n = len(self.results)
        if n >= self.min_calls and (n - sum(self.results)) / n >= self.threshold:
            self._open()

    def neutral(self) -> None:
        """Outcome says nothing about health (429, client error, cancelled): free the probe slot."""
        if self.state == "half_open":
            self.probing = False

    def _open(self) -> None:
        self.state = "open"
        self.opened_at = time.monotonic()
        self.results.clear()
        self.probing = False
