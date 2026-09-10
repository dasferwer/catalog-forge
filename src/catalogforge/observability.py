import logging
import time
from uuid import uuid4

from fastapi import FastAPI, Request
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from starlette.responses import Response

REQUESTS = Counter("http_requests_total", "HTTP requests", ["method", "route", "status"])
LATENCY = Histogram("http_request_duration_seconds", "HTTP request duration", ["route"])
logger = logging.getLogger("api")


def instrument(app: FastAPI):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    @app.middleware("http")
    async def trace(request: Request, call_next):
        request_id = str(uuid4())
        start = time.perf_counter()
        response = await call_next(request)
        route = getattr(request.scope.get("route"), "path", "unmatched")
        duration = time.perf_counter() - start
        REQUESTS.labels(request.method, route, response.status_code).inc()
        LATENCY.labels(route).observe(duration)
        response.headers["X-Request-ID"] = request_id
        logger.info(
            "request_id=%s method=%s route=%s status=%s seconds=%.4f",
            request_id,
            request.method,
            route,
            response.status_code,
            duration,
        )
        return response

    @app.get("/metrics", include_in_schema=False)
    async def metrics():
        return Response(generate_latest(), headers={"Content-Type": CONTENT_TYPE_LATEST})
