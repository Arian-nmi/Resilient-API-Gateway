from decouple import config


class Settings:
    """All tunables come from env vars prefixed with GW_ (e.g. GW_A_RATE=40).

    Sensible local-dev defaults are baked in so the gateway boots with zero config;
    a real .env / env overrides them. Tests can also pass overrides as kwargs.
    """

    def __init__(self, **overrides):
        # Provider upstreams
        self.provider_a_url = config("GW_PROVIDER_A_URL", default="http://92.114.51.251:8001/v1/process")
        self.provider_b_url = config("GW_PROVIDER_B_URL", default="http://92.114.51.251:8002/v1/process")
        self.provider_c_url = config("GW_PROVIDER_C_URL", default="http://92.114.51.251:8003/v1/process")

        # Provider A: fast but strictly rate limited -> throttle ourselves before it does.
        # The limit is shared by all replicas through Redis (it is A's limit, not ours).
        self.a_rate = config("GW_A_RATE", default=40.0, cast=float)
        self.a_burst = config("GW_A_BURST", default=60, cast=int)
        self.a_timeout = config("GW_A_TIMEOUT", default=3.0, cast=float)
        self.a_max_concurrency = config("GW_A_MAX_CONCURRENCY", default=100, cast=int)

        # Providers B/C: slow (~10s), flaky (~10% errors), higher limits
        self.bc_timeout = config("GW_BC_TIMEOUT", default=11.0, cast=float)
        self.bc_max_concurrency = config("GW_BC_MAX_CONCURRENCY", default=500, cast=int)

        # Retry / failover
        self.max_attempts = config("GW_MAX_ATTEMPTS", default=3, cast=int)
        self.backoff_base = config("GW_BACKOFF_BASE", default=0.1, cast=float)
        self.request_deadline = config("GW_REQUEST_DEADLINE", default=25.0, cast=float)

        # Protecting our own service
        self.ingress_rate = config("GW_INGRESS_RATE", default=5000.0, cast=float)
        self.ingress_burst = config("GW_INGRESS_BURST", default=5000, cast=int)
        self.max_in_flight = config("GW_MAX_IN_FLIGHT", default=2000, cast=int)

        # Circuit breaker (per provider, per instance)
        self.breaker_window = config("GW_BREAKER_WINDOW", default=20, cast=int)
        self.breaker_min_calls = config("GW_BREAKER_MIN_CALLS", default=10, cast=int)
        self.breaker_threshold = config("GW_BREAKER_THRESHOLD", default=0.5, cast=float)
        self.breaker_open_seconds = config("GW_BREAKER_OPEN_SECONDS", default=10.0, cast=float)

        # Redis (shared limiter)
        self.redis_url = config("GW_REDIS_URL", default="redis://localhost:6379/0")
        self.redis_timeout = config("GW_REDIS_TIMEOUT", default=0.5, cast=float)
        self.redis_retry_after_failure = config("GW_REDIS_RETRY_AFTER_FAILURE",
                                                default=5.0, cast=float)
        self.database_url = config("GW_DATABASE_URL", default="")
        if not self.database_url:
            pg_user = config("POSTGRES_USER", default="gateway")
            pg_password = config("POSTGRES_PASSWORD", default="gateway")
            pg_db = config("POSTGRES_DB", default="gateway")
            pg_host = config("GW_PG_HOST", default="localhost")
            pg_port = config("GW_PG_PORT", default="5432")
            self.database_url = (f"postgresql://{pg_user}:{pg_password}@"
                                 f"{pg_host}:{pg_port}/{pg_db}")
        self.writer_queue_size = config("GW_WRITER_QUEUE_SIZE", default=50_000, cast=int)
        self.writer_batch_size = config("GW_WRITER_BATCH_SIZE", default=1000, cast=int)
        self.writer_flush_interval = config("GW_WRITER_FLUSH_INTERVAL", default=0.5, cast=float)
        self.writer_enabled = config("GW_WRITER_ENABLED", default=True, cast=bool)

        self.log_level = config("GW_LOG_LEVEL", default="INFO")

        # Programmatic overrides (used by the test suite, e.g. Settings(backoff_base=0.001)).
        for key, value in overrides.items():
            setattr(self, key, value)


settings = Settings()