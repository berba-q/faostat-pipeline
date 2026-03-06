"""
FAOSTAT HTTP client with rate limiting (max 2 req/s) and automatic retries.
"""

import asyncio
import base64
import json as _json
import logging
import os
import time
import warnings
from typing import Any

import httpx
from dotenv import find_dotenv, load_dotenv, set_key
from tenacity import retry, stop_after_attempt, wait_exponential

load_dotenv()

logger = logging.getLogger("faostat_pipeline")

BASE_URL = os.getenv("FAOSTAT_BASE_URL", "https://faostatservices.fao.org/api/v1")
API_TOKEN = os.getenv("FAOSTAT_API_TOKEN", "")
FAOSTAT_USERNAME = os.getenv("FAOSTAT_USERNAME", "")
FAOSTAT_PASSWORD = os.getenv("FAOSTAT_PASSWORD", "")

# API hard cap: 3 req/s per IP (single-process only — see _throttle docstring for multi-worker note).
_RATE_LIMIT = 2  # stay below the 3/s cap to leave headroom for bursts / other clients
_MIN_INTERVAL = 1.0 / _RATE_LIMIT  # seconds between requests
_last_request_time: float = 0.0
_rate_lock = asyncio.Lock()


class FAOSTATAuthError(Exception):
    """Raised when the API token is missing or invalid."""


class FAOSTATRateLimitError(Exception):
    """Raised when the API rate limit is exceeded."""


class FAOSTATServerError(Exception):
    """Raised when the API returns a 5xx server error."""


def _check_token_expiry(token: str) -> None:
    """Raise if JWT is expired; warn if less than 10 minutes remain."""
    try:
        payload_b64 = token.split(".")[1]
        payload_b64 += "=" * (4 - len(payload_b64) % 4)
        claims = _json.loads(base64.urlsafe_b64decode(payload_b64))
        exp = claims.get("exp")
        if exp is None:
            return
        remaining = exp - time.time()
        if remaining <= 0:
            raise FAOSTATAuthError(
                f"Your FAOSTAT_API_TOKEN expired {abs(int(remaining))} seconds ago. "
                "Please log in again at the developer portal and update your .env file."
            )
        if remaining < 600:  # less than 10 minutes
            warnings.warn(
                f"Your FAOSTAT_API_TOKEN expires in {int(remaining)} seconds "
                f"({int(remaining / 60)} minutes). Consider refreshing soon.",
                stacklevel=3,
            )
    except (IndexError, ValueError, KeyError):
        pass  # not a JWT or can't decode — skip check


def _save_token_to_env(token: str) -> None:
    """Persist a refreshed token back to the .env file in-place."""
    env_path = find_dotenv(usecwd=True)
    if env_path:
        set_key(env_path, "FAOSTAT_API_TOKEN", token)
        logger.info("Refreshed token saved to %s", env_path)
    else:
        logger.warning("No .env file found — refreshed token not persisted to disk.")


def _retry_on_transient(retry_state) -> bool:
    """Retry on transport errors, 5xx server errors, and 429 rate limit responses."""
    exc = retry_state.outcome.exception()
    if exc is None:
        return False
    if isinstance(exc, httpx.TransportError):
        return True
    if isinstance(exc, FAOSTATRateLimitError):
        return True
    if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code >= 500:
        return True
    return False


class FAOSTATClient:
    """Async HTTP client for the FAOSTAT REST API."""

    def __init__(
        self,
        token: str | None = None,
        base_url: str | None = None,
        username: str | None = None,
        password: str | None = None,
    ):
        self.token = token or API_TOKEN
        self.base_url = (base_url or BASE_URL).rstrip("/")
        self.username = username or FAOSTAT_USERNAME
        self.password = password or FAOSTAT_PASSWORD

        if not self.token and not (self.username and self.password):
            raise FAOSTATAuthError(
                "No FAOSTAT_API_TOKEN and no FAOSTAT_USERNAME/FAOSTAT_PASSWORD set. "
                "Add your credentials to .env to enable auto-authentication."
            )

        if self.token:
            _check_token_expiry(self.token)
        self._client: httpx.AsyncClient | None = None

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
        }

    async def _refresh_token(self) -> None:
        """Fetch a new access token via POST /auth/login and persist it to .env."""
        if not (self.username and self.password):
            raise FAOSTATAuthError(
                "Token expired and FAOSTAT_USERNAME/FAOSTAT_PASSWORD not set. "
                "Add credentials to .env to enable auto-refresh."
            )
        logger.info("Refreshing FAOSTAT access token...")
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{self.base_url}/auth/login",
                data={"username": self.username, "password": self.password},
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            if response.status_code == 401:
                raise FAOSTATAuthError(
                    "Auth refresh failed: invalid credentials (401). "
                    "Check FAOSTAT_USERNAME and FAOSTAT_PASSWORD in .env."
                )
            response.raise_for_status()
            data = response.json()

        new_token = (
            data.get("access_token")
            or data.get("AccessToken")
            or data.get("token")
        )
        if not new_token:
            raise FAOSTATAuthError(
                f"Auth endpoint did not return a token. Response keys: {list(data.keys())}"
            )

        self.token = new_token
        if self._client:
            self._client.headers["Authorization"] = f"Bearer {new_token}"
        _save_token_to_env(new_token)
        logger.info("Token refreshed successfully.")

    async def _ensure_valid_token(self) -> None:
        """Proactively refresh if the current token is expired or missing."""
        if not self.token:
            await self._refresh_token()
            return
        try:
            _check_token_expiry(self.token)
        except FAOSTATAuthError:
            await self._refresh_token()

    async def __aenter__(self) -> "FAOSTATClient":
        if not self.token:
            await self._refresh_token()
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            headers=self._headers(),
            timeout=60.0,
        )
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self._client:
            await self._client.aclose()

    async def _throttle(self) -> None:
        """Enforce max 2 requests per second globally within this process.

        NOTE: This lock is process-scoped (asyncio.Lock). If the MCP server runs
        with multiple worker processes, each worker enforces its own limit
        independently and the combined rate from one IP can exceed the 3 req/s
        API cap. Use a shared rate limiter (e.g. Redis, file lock) for
        multi-worker deployments.
        """
        global _last_request_time
        async with _rate_lock:
            now = time.monotonic()
            elapsed = now - _last_request_time
            if elapsed < _MIN_INTERVAL:
                await asyncio.sleep(_MIN_INTERVAL - elapsed)
            _last_request_time = time.monotonic()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=60),
        retry=_retry_on_transient,
        reraise=True,
    )
    async def _get_raw(self, path: str, params: dict[str, Any] | None = None) -> Any:
        """Send a GET request with rate limiting and transient-error retries."""
        await self._throttle()
        assert self._client is not None, "Use client as async context manager"
        response = await self._client.get(path, params=params)
        logger.debug(
            "GET %s -> %d (%d bytes, %s)",
            path, response.status_code, len(response.content),
            response.headers.get("content-type", "no content-type"),
        )
        _raise_for_status(response)
        if not response.content:
            return {"status": response.status_code}
        try:
            return response.json()
        except ValueError:
            logger.warning("Non-JSON response from GET %s: %.500s", path, response.text)
            return {"status": response.status_code, "text": response.text}

    async def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        """Send a GET request, refreshing the token once on auth failure."""
        await self._ensure_valid_token()
        try:
            return await self._get_raw(path, params)
        except FAOSTATAuthError:
            logger.warning("Auth error on GET %s — refreshing token and retrying.", path)
            await self._refresh_token()
            return await self._get_raw(path, params)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=60),
        retry=_retry_on_transient,
        reraise=True,
    )
    async def _post_raw(self, path: str, json: Any = None) -> Any:
        """Send a POST request with rate limiting and transient-error retries."""
        await self._throttle()
        assert self._client is not None, "Use client as async context manager"
        response = await self._client.post(path, json=json)
        logger.debug(
            "POST %s -> %d (%d bytes, %s)",
            path, response.status_code, len(response.content),
            response.headers.get("content-type", "no content-type"),
        )
        _raise_for_status(response)
        if not response.content:
            return {"status": response.status_code}
        try:
            return response.json()
        except ValueError:
            logger.warning("Non-JSON response from POST %s: %.500s", path, response.text)
            return {"status": response.status_code, "text": response.text}

    async def post(self, path: str, json: Any = None) -> Any:
        """Send a POST request, refreshing the token once on auth failure."""
        await self._ensure_valid_token()
        try:
            return await self._post_raw(path, json)
        except FAOSTATAuthError:
            logger.warning("Auth error on POST %s — refreshing token and retrying.", path)
            await self._refresh_token()
            return await self._post_raw(path, json)


def _raise_for_status(response: httpx.Response) -> None:
    """Raise meaningful errors for common HTTP status codes."""
    try:
        body = response.text.strip()[:500]
    except Exception:
        body = ""
    detail = f" Server response: {body}" if body else ""

    if response.status_code == 401:
        raise FAOSTATAuthError(f"401 Unauthorized — invalid or expired API token.{detail}")
    if response.status_code == 403:
        raise FAOSTATAuthError(
            f"403 Forbidden — authentication failed.{detail} "
            "If your token expired, log in again at the developer portal and update .env."
        )
    if response.status_code == 429:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            logger.warning("429 Rate limit — server requests retry after %s s.", retry_after)
        raise FAOSTATRateLimitError(f"429 Rate limit exceeded.{detail}")
    response.raise_for_status()
