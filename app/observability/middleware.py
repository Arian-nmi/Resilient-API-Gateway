import logging
import re
import time
import uuid

from .logging import trace_id_var

log = logging.getLogger("gateway.access")
_SAFE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
_QUIET = {"/metrics", "/health"}


class TraceIdMiddleware:
    """Pure ASGI middleware: reuse or create X-Trace-Id, expose it to logs via a ContextVar,
    echo it in the response and emit one structured access-log line per request."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        incoming = dict(scope["headers"]).get(b"x-trace-id", b"").decode("latin-1")
        trace_id = incoming if _SAFE.match(incoming) else uuid.uuid4().hex
        state = scope.setdefault("state", {})
        state["trace_id"] = trace_id
        token = trace_id_var.set(trace_id)
        started = time.perf_counter()
        status = 500

        async def send_wrapper(message):
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
                message["headers"] = list(message.get("headers", [])) + [
                    (b"x-trace-id", trace_id.encode())]
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            if scope["path"] not in _QUIET:
                log.info("request", extra={
                    "method": scope["method"], "path": scope["path"], "status": status,
                    "duration_ms": round((time.perf_counter() - started) * 1000, 2),
                    "outcome": state.get("outcome"), "provider": state.get("provider"),
                    "attempts": state.get("attempts"),
                })
            trace_id_var.reset(token)
