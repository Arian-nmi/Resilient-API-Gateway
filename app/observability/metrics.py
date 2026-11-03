from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

_BREAKER = {"closed": 0, "half_open": 1, "open": 2}
_LAT = (0.05, 0.1, 0.25, 0.5, 1, 2, 3, 5, 7.5, 10, 15, 25)


class Metrics:
    """Own registry (not the global one) so the app can be instantiated several times in tests."""

    def __init__(self):
        r = self.registry = CollectorRegistry()
        self.requests = Counter("gateway_requests_total", "Requests by final outcome",
                                ["outcome", "provider"], registry=r)
        self.request_seconds = Histogram("gateway_request_duration_seconds",
                                         "End-to-end /v1/process latency", buckets=_LAT, registry=r)
        self.attempts = Counter("gateway_provider_attempts_total", "Upstream attempts",
                                ["provider", "result"], registry=r)
        self.provider_seconds = Histogram("gateway_provider_latency_seconds", "Upstream call latency",
                                          ["provider", "result"], buckets=_LAT, registry=r)
        self.in_flight = Gauge("gateway_in_flight_requests", "Requests being processed", registry=r)
        self.breaker_state = Gauge("gateway_breaker_state", "0 closed, 1 half-open, 2 open",
                                   ["provider"], registry=r)
        self.ingress_rejected = Counter("gateway_ingress_rejected_total", "429s from our own limiter",
                                        registry=r)
        self.redis_fallback = Counter("gateway_redis_fallback_total",
                                      "Times the Redis limiter failed and the local one was used", registry=r)
        self.queue_depth = Gauge("gateway_persist_queue_depth", "Audit records waiting to be written",
                                 registry=r)
        self.persist_dropped = Counter("gateway_persist_dropped_total",
                                       "Audit records dropped (queue full / DB down)", registry=r)
        self.persist_written = Counter("gateway_persist_written_total", "Audit records written", registry=r)
        self.persist_batch = Histogram("gateway_persist_batch_size", "Rows per COPY batch",
                                       buckets=(1, 10, 50, 100, 250, 500, 1000, 2500), registry=r)

    def render(self, router, writer) -> bytes:
        for p in router.providers:
            self.breaker_state.labels(p.name).set(_BREAKER[p.breaker.state])
        self.queue_depth.set(writer.depth if writer else 0)
        return generate_latest(self.registry)
