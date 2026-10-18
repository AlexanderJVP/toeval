"""HTTP client shared by all connectors: rate limiting, retries with backoff, auth, conditional GET.

Connectors never talk to httpx directly; the base class hands them an `HttpClient` configured from
the source's `HttpPolicy` and `Auth`.
"""

import json
import logging
import os
import random
import time
from collections.abc import Callable, Mapping, MutableMapping
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

from toeval.connectors.spec import ApiKeyAuth, Auth, BearerAuth, HttpPolicy, NoAuth

log = logging.getLogger(__name__)

Clock = Callable[[], float]
Sleep = Callable[[float], None]


class MissingSecretError(RuntimeError):
    pass


class HttpError(RuntimeError):
    def __init__(self, message: str, status: int | None = None, url: str | None = None) -> None:
        super().__init__(message)
        self.status = status
        self.url = url


def resolve_auth(
    auth: Auth, environ: Mapping[str, str] = os.environ
) -> tuple[dict[str, str], dict[str, str]]:
    """Return (headers, query params) for the auth scheme. Fails before any request is sent."""
    if isinstance(auth, NoAuth):
        return {}, {}
    secret = environ.get(auth.env_var)
    if not secret:
        raise MissingSecretError(
            f"environment variable {auth.env_var} is not set (add it to .env; see .env.example)"
        )
    if isinstance(auth, BearerAuth):
        return {"Authorization": f"Bearer {secret}"}, {}
    assert isinstance(auth, ApiKeyAuth)
    if auth.header is not None:
        return {auth.header: secret}, {}
    assert auth.query_param is not None
    return {}, {auth.query_param: secret}


class TokenBucket:
    """Allows `burst` requests at once, refilling at `rate` per second."""

    def __init__(self, rate: float, burst: int, clock: Clock, sleep: Sleep) -> None:
        self._rate = rate
        self._capacity = float(burst)
        self._tokens = float(burst)
        self._clock = clock
        self._sleep = sleep
        self._last = clock()

    def acquire(self) -> None:
        self._refill()
        if self._tokens < 1:
            wait = (1 - self._tokens) / self._rate
            self._sleep(wait)
            self._refill()
        self._tokens -= 1

    def _refill(self) -> None:
        now = self._clock()
        self._tokens = min(self._capacity, self._tokens + (now - self._last) * self._rate)
        self._last = now


@dataclass(frozen=True)
class Response:
    status: int
    content: bytes
    headers: httpx.Headers
    url: str

    @property
    def not_modified(self) -> bool:
        return self.status == 304

    def json(self) -> Any:
        return json.loads(self.content)


def _retry_after_seconds(headers: httpx.Headers, now: datetime) -> float | None:
    value = headers.get("Retry-After")
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    return max(0.0, (when - now).total_seconds())


class HttpClient:
    def __init__(
        self,
        policy: HttpPolicy,
        auth: Auth,
        *,
        user_agent: str,
        validators: MutableMapping[str, dict[str, str]] | None = None,
        transport: httpx.BaseTransport | None = None,
        clock: Clock = time.monotonic,
        sleep: Sleep = time.sleep,
        rng: random.Random | None = None,
        environ: Mapping[str, str] = os.environ,
    ) -> None:
        self._policy = policy
        headers, self._auth_params = resolve_auth(auth, environ)
        self._client = httpx.Client(
            headers={"User-Agent": user_agent, **headers},
            timeout=policy.timeout.total_seconds(),
            transport=transport,
            follow_redirects=True,
        )
        self._bucket = TokenBucket(policy.requests_per_second, policy.burst, clock, sleep)
        self._sleep = sleep
        self._rng = rng or random.Random()
        # Conditional-GET validators per request key; the caller owns persistence.
        self.validators: MutableMapping[str, dict[str, str]] = (
            validators if validators is not None else {}
        )
        self.requests_sent = 0

    def close(self) -> None:
        self._client.close()

    def get(
        self,
        url: str,
        params: Mapping[str, Any] | None = None,
        *,
        conditional_key: str | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Response:
        """GET with rate limiting and retries.

        With `conditional_key`, sends stored ETag / Last-Modified and may return a 304 response
        (`response.not_modified`); validators are updated on 200.
        """
        request_headers = dict(headers or {})
        if conditional_key is not None:
            stored = self.validators.get(conditional_key, {})
            if "etag" in stored:
                request_headers["If-None-Match"] = stored["etag"]
            if "last_modified" in stored:
                request_headers["If-Modified-Since"] = stored["last_modified"]
        all_params = {**(params or {}), **self._auth_params}

        attempt = 0
        while True:
            self._bucket.acquire()
            self.requests_sent += 1
            try:
                r = self._client.get(url, params=all_params, headers=request_headers)
            except httpx.TransportError as exc:
                reason, delay = f"{type(exc).__name__}: {exc}", None
            else:
                if r.status_code < 400:  # includes 304 Not Modified
                    if conditional_key is not None and r.status_code == 200:
                        self._store_validators(conditional_key, r.headers)
                    return Response(r.status_code, r.content, r.headers, str(r.url))
                if r.status_code not in self._policy.retry_statuses:
                    raise HttpError(
                        f"GET {r.url} failed with {r.status_code}: {r.text[:300]}",
                        status=r.status_code,
                        url=str(r.url),
                    )
                reason = f"HTTP {r.status_code}"
                delay = _retry_after_seconds(r.headers, datetime.now(UTC))

            if attempt >= self._policy.max_retries:
                raise HttpError(f"GET {url} gave up after {attempt + 1} attempts ({reason})")
            if delay is None:
                delay = self._backoff(attempt)
            log.warning(
                "GET %s: %s, retry %d/%d in %.1fs",
                url,
                reason,
                attempt + 1,
                self._policy.max_retries,
                delay,
            )
            self._sleep(delay)
            attempt += 1

    def _backoff(self, attempt: int) -> float:
        """Exponential backoff with full jitter."""
        cap = min(
            self._policy.backoff_max.total_seconds(),
            self._policy.backoff_base.total_seconds() * 2**attempt,
        )
        return self._rng.uniform(0, cap)

    def _store_validators(self, key: str, headers: httpx.Headers) -> None:
        found = {}
        if etag := headers.get("ETag"):
            found["etag"] = etag
        if last_modified := headers.get("Last-Modified"):
            found["last_modified"] = last_modified
        if found:
            self.validators[key] = found
        else:
            self.validators.pop(key, None)
