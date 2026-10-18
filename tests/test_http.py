import random
from datetime import timedelta

import httpx
import pytest

from toeval.connectors.http import (
    HttpClient,
    HttpError,
    MissingSecretError,
    TokenBucket,
    resolve_auth,
)
from toeval.connectors.spec import ApiKeyAuth, BearerAuth, HttpPolicy, NoAuth


class Recorder:
    def __init__(self, responses: list[httpx.Response | Exception]) -> None:
        self.responses = responses
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def client(
    responses: list[httpx.Response | Exception],
    sleeps: list[float],
    policy: HttpPolicy | None = None,
    auth=None,  # type: ignore[no-untyped-def]
    environ: dict[str, str] | None = None,
) -> tuple[HttpClient, Recorder]:
    rec = Recorder(responses)
    c = HttpClient(
        policy or HttpPolicy(requests_per_second=1000, burst=1000, max_retries=3),
        auth or NoAuth(),
        user_agent="toeval-test",
        transport=httpx.MockTransport(rec),
        sleep=sleeps.append,
        rng=random.Random(0),
        environ=environ or {},
    )
    return c, rec


def test_retries_server_errors_with_backoff_then_succeeds() -> None:
    sleeps: list[float] = []
    c, rec = client(
        [httpx.Response(503), httpx.Response(502), httpx.Response(200, json=[1])], sleeps
    )

    r = c.get("https://x.test/a")

    assert r.json() == [1]
    assert len(rec.requests) == 3
    assert len(sleeps) == 2
    # full jitter, capped at base * 2^attempt
    assert 0 <= sleeps[0] <= 1
    assert 0 <= sleeps[1] <= 2


def test_retries_connection_errors() -> None:
    sleeps: list[float] = []
    c, rec = client([httpx.ConnectError("down"), httpx.Response(200, json={})], sleeps)

    c.get("https://x.test/a")

    assert len(rec.requests) == 2


def test_honours_retry_after_on_429() -> None:
    sleeps: list[float] = []
    c, _ = client([httpx.Response(429, headers={"Retry-After": "7"}), httpx.Response(200)], sleeps)

    c.get("https://x.test/a")

    assert sleeps == [7.0]


def test_does_not_retry_client_errors() -> None:
    sleeps: list[float] = []
    c, rec = client([httpx.Response(404, text="nope")], sleeps)

    with pytest.raises(HttpError, match="404") as info:
        c.get("https://x.test/a")

    assert info.value.status == 404
    assert len(rec.requests) == 1
    assert sleeps == []


def test_gives_up_after_max_retries() -> None:
    sleeps: list[float] = []
    c, rec = client([httpx.Response(500)] * 4, sleeps)

    with pytest.raises(HttpError, match="gave up after 4 attempts"):
        c.get("https://x.test/a")

    assert len(rec.requests) == 4


def test_backoff_is_capped() -> None:
    sleeps: list[float] = []
    policy = HttpPolicy(
        requests_per_second=1000,
        burst=1000,
        max_retries=10,
        backoff_base=timedelta(seconds=1),
        backoff_max=timedelta(seconds=5),
    )
    c, _ = client([httpx.Response(500)] * 10 + [httpx.Response(200)], sleeps, policy)

    c.get("https://x.test/a")

    assert max(sleeps) <= 5


def test_conditional_get_sends_and_updates_validators() -> None:
    sleeps: list[float] = []
    c, rec = client(
        [
            httpx.Response(200, json=[], headers={"ETag": '"v1"', "Last-Modified": "Mon"}),
            httpx.Response(304),
        ],
        sleeps,
    )

    first = c.get("https://x.test/a", conditional_key="a")
    second = c.get("https://x.test/a", conditional_key="a")

    assert not first.not_modified
    assert second.not_modified
    assert rec.requests[1].headers["If-None-Match"] == '"v1"'
    assert rec.requests[1].headers["If-Modified-Since"] == "Mon"
    assert c.validators == {"a": {"etag": '"v1"', "last_modified": "Mon"}}


def test_sends_user_agent_and_auth_query_param() -> None:
    sleeps: list[float] = []
    auth = ApiKeyAuth(env_var="KEY", query_param="token")
    c, rec = client([httpx.Response(200)], sleeps, auth=auth, environ={"KEY": "s3cret"})

    c.get("https://x.test/a", {"q": 1})

    assert rec.requests[0].headers["User-Agent"] == "toeval-test"
    assert rec.requests[0].url.params["token"] == "s3cret"
    assert rec.requests[0].url.params["q"] == "1"


def test_resolve_auth_variants() -> None:
    env = {"K": "v"}
    assert resolve_auth(NoAuth(), env) == ({}, {})
    assert resolve_auth(BearerAuth(env_var="K"), env) == ({"Authorization": "Bearer v"}, {})
    assert resolve_auth(ApiKeyAuth(env_var="K", header="X-Api-Key"), env) == (
        {"X-Api-Key": "v"},
        {},
    )


def test_missing_secret_fails_before_any_request() -> None:
    with pytest.raises(MissingSecretError, match="TELRAAM_API_KEY"):
        client([], [], auth=ApiKeyAuth(env_var="TELRAAM_API_KEY", header="X-Api-Key"))


def test_token_bucket_allows_burst_then_paces() -> None:
    now = [0.0]
    sleeps: list[float] = []

    def sleep(s: float) -> None:
        sleeps.append(s)
        now[0] += s

    bucket = TokenBucket(rate=3, burst=5, clock=lambda: now[0], sleep=sleep)
    for _ in range(8):
        bucket.acquire()

    assert len(sleeps) == 3  # 5 free, then 3 paced
    assert sum(sleeps) == pytest.approx(1.0)  # 3 tokens at 3/s
