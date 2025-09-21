from fastapi import FastAPI, Request
from .routes import router as items_router
from . import db
from .health import router as health_router
import time
import logging

logger = logging.getLogger(__name__)


def create_app() -> FastAPI:
    app = FastAPI(title="Example FastAPI + DuckDB")

    # request id + simple request logging middleware
    @app.middleware("http")
    async def request_id_middleware(request: Request, call_next):
        # prefer incoming header or generate one
        req_id = request.headers.get("x-request-id")
        if not req_id:
            import uuid

            req_id = str(uuid.uuid4())

        request.state.request_id = req_id
        start = time.time()
        response = await call_next(request)
        duration = (time.time() - start) * 1000
        # include request id in response headers
        response.headers["X-Request-ID"] = req_id
        logger.info("%s %s %s %.2fms request_id=%s", request.method, request.url.path, response.status_code, duration, req_id)
        return response

    app.include_router(items_router)
    app.include_router(health_router)
    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
