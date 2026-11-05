import time
from collections import deque


class CircuitBreaker:
    """closed -> open (error rate over a sliding window) -> half_open (a few probes) -> closed/open.

    Providers here take 1-10 s to answer, so a *single* half-open probe would keep the provider
    out of rotation for up to a whole timeout. We allow `half_open_probes` concurrent probes;
    the first success closes the breaker, the first failure re-opens it.
    """

    def __init__(self, window: int, min_calls: int, threshold: float, open_seconds: float,
                 half_open_probes: int = 3, on_transition=None):
        self.results: deque[bool] = deque(maxlen=window)
        self.min_calls = min_calls
        self.threshold = threshold
        self.open_seconds = open_seconds
        self.half_open_probes = half_open_probes
        self.on_transition = on_transition   # callable(new_state) for metrics/logs
        self.state = "closed"
        self.opened_at = 0.0
        self.probes = 0

    def _set(self, state: str) -> None:
        self.state = state
        if self.on_transition:
            self.on_transition(state)

    def allow(self) -> bool:
        if self.state == "open":
            if time.monotonic() - self.opened_at < self.open_seconds:
                return False
            self._set("half_open")
            self.probes = 0
        if self.state == "half_open":
            if self.probes >= self.half_open_probes:
                return False
            self.probes += 1
        return True

    def record(self, ok: bool) -> None:
        if self.state == "open":  # stale result from a request started before opening
            return
        if self.state == "half_open":
            if ok:
                self.results.clear()
                self._set("closed")
            else:
                self._open()
            return
        self.results.append(ok)
        n = len(self.results)
        if n >= self.min_calls and (n - sum(self.results)) / n >= self.threshold:
            self._open()

    def neutral(self) -> None:
        """Outcome says nothing about health (429, client error, cancelled): free the probe slot."""
        if self.state == "half_open" and self.probes > 0:
            self.probes -= 1

    def _open(self) -> None:
        self.opened_at = time.monotonic()
        self.results.clear()
        self.probes = 0
        self._set("open")
