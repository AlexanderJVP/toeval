"""FastAPI app behind `toeval ui`: serves the page, a JSON snapshot and the progress events of
the running process (ADR 0007).

Local only: uvicorn binds 127.0.0.1, and TrustedHostMiddleware rejects other Host headers, so a
web page elsewhere can't read this API through DNS rebinding.

Actions (POST) also need the per-session token that is embedded in the page. Another site can't
read the page, so it can't send the token; the custom header also forces a CORS preflight, which
this app never approves. Requests with a foreign `Origin` are refused as well.
"""

import logging
import secrets
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime
from importlib.resources import files
from typing import Any
from urllib.parse import urlsplit

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from toeval.config import Settings
from toeval.connectors import registry
from toeval.events import read_events
from toeval.sources_file import set_stream_option
from toeval.ui.actions import BusyError, JobRunner
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


class StreamRef(BaseModel):
    source: str = Field(max_length=64)
    stream: str | None = Field(default=None, max_length=64)


class PauseRequest(BaseModel):
    source: str = Field(max_length=64)
    stream: str = Field(max_length=64)
    paused: bool


def create_app(
    settings: Settings,
    *,
    task_query: Callable[[], TaskInfo | None] = query_task,
    clock: Callable[[], datetime] = _utcnow,
    allowed_hosts: tuple[str, ...] = ("127.0.0.1", "localhost"),
    task_refresh_seconds: float = TASK_REFRESH_SECONDS,
    jobs: JobRunner | None = None,
    token: str | None = None,
) -> FastAPI:
    app = FastAPI(title="toeval", docs_url=None, redoc_url=None, openapi_url=None)
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=list(allowed_hosts))
    app.mount("/static", StaticFiles(directory=str(STATIC)), name="static")
    tasks = TaskCache(task_query, task_refresh_seconds)
    runner = jobs or JobRunner(settings)
    session_token = token or secrets.token_urlsafe(32)
    last_good: dict[str, Any] = {}

    def authorized(request: Request, x_toeval_token: str = Header("", max_length=200)) -> None:
        origin = request.headers.get("origin")
        if origin is not None and urlsplit(origin).hostname not in allowed_hosts:
            raise HTTPException(403, "foreign origin")
        if not secrets.compare_digest(x_toeval_token, session_token):
            raise HTTPException(403, "missing or stale session token: reload the page")

    def not_busy() -> None:
        lock = read_lock(settings.lock_path)
        if lock and lock.get("alive"):
            raise HTTPException(409, f"a run is in progress (pid {lock.get('pid')}); try after it")

    def known(source: str, stream: str | None) -> None:
        connector = registry.CONNECTORS.get(source)
        if connector is None:
            raise HTTPException(404, f"unknown source {source!r}")
        if stream is not None:
            try:
                connector.spec.stream(stream)
            except KeyError as exc:
                raise HTTPException(404, str(exc)) from None

    def start(label: str, args: list[str]) -> JSONResponse:
        not_busy()
        try:
            job = runner.start(label, args)
        except BusyError as exc:
            raise HTTPException(409, str(exc)) from None
        return JSONResponse(job.as_json(), status_code=202)

    @app.get("/", include_in_schema=False)
    def index() -> HTMLResponse:
        page = (STATIC / "index.html").read_text(encoding="utf-8")
        return HTMLResponse(
            page.replace("__TOEVAL_TOKEN__", session_token),
            headers={"Cache-Control": "no-store"},
        )

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

    @app.post("/api/actions/ingest", dependencies=[Depends(authorized)])
    def fetch_now(body: StreamRef) -> JSONResponse:
        known(body.source, body.stream)
        target = f"{body.source}/{body.stream}" if body.stream else body.source
        args = ["ingest", body.source, *([body.stream] if body.stream else [])]
        return start(f"fetch {target}", args)

    @app.post("/api/actions/transform", dependencies=[Depends(authorized)])
    def rebuild() -> JSONResponse:
        return start("rebuild warehouse", ["transform"])

    @app.post("/api/actions/pause", dependencies=[Depends(authorized)])
    def pause(body: PauseRequest) -> JSONResponse:
        known(body.source, body.stream)
        try:
            set_stream_option(
                settings.sources_path,
                body.source,
                body.stream,
                "paused",
                True if body.paused else None,
            )
        except (KeyError, ValueError, OSError) as exc:
            raise HTTPException(
                400, f"could not update {settings.sources_path.name}: {exc}"
            ) from None
        log.info(
            "%s/%s %s from the console",
            body.source,
            body.stream,
            "paused" if body.paused else "resumed",
        )
        # Show it right away, even while a run keeps the state DB (and a fresh snapshot) locked.
        for stream in last_good.get("streams", []):
            if (stream["source"], stream["stream"]) == (body.source, body.stream):
                stream["paused"] = body.paused
        return JSONResponse({"source": body.source, "stream": body.stream, "paused": body.paused})

    @app.get("/api/jobs")
    def job_list() -> JSONResponse:
        return JSONResponse({"jobs": runner.recent()}, headers={"Cache-Control": "no-store"})

    @app.get("/api/events")
    def events(cursor: str | None = Query(None, max_length=100)) -> JSONResponse:
        """New events after `cursor`; without one, the most recent events and a cursor."""
        items, next_cursor = read_events(settings.events_path, cursor)
        return JSONResponse(
            {"events": items, "cursor": next_cursor}, headers={"Cache-Control": "no-store"}
        )

    return app
