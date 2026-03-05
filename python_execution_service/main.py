"""Python Execution Service – FastAPI application entrypoint."""

import logging

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from python_execution_service import sqlite_store
from python_execution_service.helpers import load_persisted_runs
from python_execution_service.routes import register_routes

logger = logging.getLogger(__name__)

app = FastAPI(title="Python Execution Service", version="0.1.0")


# ── Exception handlers ──────────────────────────────────────────

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    if request.url.path == "/v1/runs/start":
        body_bytes = await request.body()
        body_text = body_bytes.decode("utf-8", errors="replace")
        logger.error(
            "Validation error on /v1/runs/start. body=%s errors=%s",
            body_text,
            exc.errors(),
        )
    return JSONResponse(status_code=422, content={"detail": exc.errors()})


# ── Startup ─────────────────────────────────────────────────────

sqlite_store.init_schema()
load_persisted_runs()
register_routes(app)
