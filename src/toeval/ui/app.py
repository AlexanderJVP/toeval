"""FastAPI app behind `toeval ui`: serves the page and a JSON snapshot (ADR 0007).

Local only: uvicorn binds 127.0.0.1, and TrustedHostMiddleware rejects other Host headers, so a
web page elsewhere can't read this API through DNS rebinding.
"""

import logging
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime
from importlib.resources import files
from typing import Any

from fastapi import FastAPI
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from toeval.config import Settings
from toeval.ui.scheduler import TaskInfo, query_task
from toeval.ui.snapshot import StateBusyError, build_snapshot, health, read_lock

log = logging.getLogger(__name__)

STATIC = files("toeval.ui") / "static"
TASK_REFRESH_SECONDS = 30.0


def _utcnow() -> datetime:
    return datetime.now(UTC)


class TaskCache:
    """The scheduled task's state, refreshed in the background (the query takes ~2 s)."""

    def __init__(self, query: Callable[[], TaskInfo | None], max_age: float) -> None:
        self._query = query
        self._max_age = max_age
        self._value: TaskInfo | None = None
        self._fetched_at = float("-inf")
        self._refreshing = False
        self._lock = threading.Lock()

    def get(self, *, wait: bool = False) -> TaskInfo | None:
        with self._lock:
            stale = time.monotonic() - self._fetched_at > self._max_age
            start = stale and not self._refreshing
            if start:
                self._refreshing = True
        if start:
            if wait:
                self._refresh()
            else:
                threading.Thread(target=self._refresh, daemon=True).start()
        return self._value

    def _refresh(self) -> None:
        try:
            value = self._query()
        except Exception:
            log.exception("could not query the scheduled task")
            value = None
        with self._lock:
            self._value = value
            self._fetched_at = time.monotonic()
            self._refreshing = False


def create_app(
    settings: Settings,
    *,
    task_query: Callable[[], TaskInfo | None] = query_task,
    clock: Callable[[], datetime] = _utcnow,
    allowed_hosts: tuple[str, ...] = ("127.0.0.1", "localhost"),
    task_refresh_seconds: float = TASK_REFRESH_SECONDS,
) -> FastAPI:
    app = FastAPI(title="toeval", docs_url=None, redoc_url=None, openapi_url=None)
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=list(allowed_hosts))
    app.mount("/static", StaticFiles(directory=str(STATIC)), name="static")
    tasks = TaskCache(task_query, task_refresh_seconds)
    last_good: dict[str, Any] = {}

    @app.get("/", include_in_schema=False)
    def index() -> FileResponse:
        return FileResponse(str(STATIC / "index.html"), headers={"Cache-Control": "no-cache"})

    @app.get("/api/snapshot")
    def snapshot() -> JSONResponse:
        now = clock()
        try:
            snap = build_snapshot(settings, now=now, task=tasks.get())
        except StateBusyError:
            # A run holds the state DB. Serve the last readable snapshot, marked as such.
            if not last_good:
                return JSONResponse(
                    {"state_available": False, "generated_at": now.isoformat()}, status_code=503
                )
            # The lock (who is running) is read live; the rest is as of `generated_at`.
            snap = dict(last_good) | {
                "state_available": False,
                "lock": read_lock(settings.lock_path),
            }
            snap["health"] = health(snap)
        else:
            last_good.clear()
            last_good.update(snap)
        return JSONResponse(snap, headers={"Cache-Control": "no-store"})

    return app
