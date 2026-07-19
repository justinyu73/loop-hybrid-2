#!/usr/bin/env python3
"""Named CI conclusion source adapter for the G6 external-verdict path.

The adapter is intentionally narrow: it fetches one conclusion for one stable
operation key, accepts only an exact response contract, and never turns source
or credential errors into a retry verdict.  A pending CI result returns None;
only explicit ``success`` or ``failure`` conclusions are returned to the
existing ``external_verdict`` reducer.

Production construction requires an HTTPS endpoint and both environment
values ``LH_CI_CONCLUSION_URL`` and ``LH_CI_TOKEN``.  Tests can inject a
transport, but still use the same endpoint and credential validation without
opening a network connection.
"""
from __future__ import annotations

import json
import os
from typing import Any, Callable, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen


SOURCE_SCHEMA = "loop-hybrid-ci-conclusion-http/v1"
PENDING_CONCLUSIONS = {"pending", "queued", "in_progress"}
TERMINAL_CONCLUSIONS = {"success", "failure"}


class CIAdapterError(RuntimeError):
    """A CI source cannot provide a trustworthy conclusion."""


class CICredentialsMissing(CIAdapterError):
    """The configured CI source has no endpoint or credential."""


class CIResponseInvalid(CIAdapterError):
    """The source response is outside the exact adapter contract."""


Transport = Callable[[str, Mapping[str, str], float], bytes]


def _http_transport(url: str, headers: Mapping[str, str], timeout: float) -> bytes:
    request = Request(url, headers=dict(headers), method="GET")
    try:
        with urlopen(request, timeout=timeout) as response:
            return response.read()
    except HTTPError as exc:
        raise CIAdapterError(f"CI source returned HTTP {exc.code}") from exc
    except (URLError, TimeoutError, OSError) as exc:
        raise CIAdapterError("CI source is unavailable") from exc


def _endpoint_with_operation_key(endpoint: str, op_key: str) -> str:
    parts = urlsplit(endpoint)
    query = [item for item in parse_qsl(parts.query, keep_blank_values=True) if item[0] != "operation_key"]
    query.append(("operation_key", op_key))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def _normalize_response(raw: bytes, op_key: str) -> dict[str, str] | None:
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CIResponseInvalid("CI source response must be UTF-8 JSON") from exc
    if not isinstance(payload, dict) or set(payload) != {"operation_key", "conclusion"}:
        raise CIResponseInvalid("CI source response must contain only operation_key and conclusion")
    if payload.get("operation_key") != op_key:
        raise CIResponseInvalid("CI source response operation_key does not match the request")
    conclusion = payload.get("conclusion")
    if conclusion in PENDING_CONCLUSIONS:
        return None
    if conclusion not in TERMINAL_CONCLUSIONS:
        raise CIResponseInvalid("CI source conclusion must be pending, success, or failure")
    return {"operation_key": op_key, "conclusion": conclusion}


class CIConclusionAdapter:
    """Callable ``ConclusionSource`` backed by one HTTPS CI endpoint."""

    def __init__(self, endpoint: str, token: str, *, timeout: float = 10.0, transport: Transport | None = None):
        endpoint = endpoint.strip()
        token = token.strip()
        parts = urlsplit(endpoint)
        if parts.scheme != "https" or not parts.netloc:
            raise ValueError("CI conclusion endpoint must be an HTTPS URL")
        if not token:
            raise CICredentialsMissing("CI conclusion token is required")
        if timeout <= 0:
            raise ValueError("CI conclusion timeout must be positive")
        self.endpoint = endpoint
        self._token = token
        self.timeout = timeout
        self._transport = transport or _http_transport

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "CIConclusionAdapter":
        values = os.environ if environ is None else environ
        endpoint = values.get("LH_CI_CONCLUSION_URL", "").strip()
        token = values.get("LH_CI_TOKEN", "").strip()
        if not endpoint or not token:
            raise CICredentialsMissing("LH_CI_CONCLUSION_URL and LH_CI_TOKEN are required")
        raw_timeout = values.get("LH_CI_TIMEOUT_SECONDS", "10")
        try:
            timeout = float(raw_timeout)
        except ValueError as exc:
            raise ValueError("LH_CI_TIMEOUT_SECONDS must be numeric") from exc
        return cls(endpoint, token, timeout=timeout)

    def __call__(self, op_key: str) -> dict[str, str] | None:
        if not isinstance(op_key, str) or not op_key.strip():
            raise CIResponseInvalid("operation_key must be a non-empty string")
        url = _endpoint_with_operation_key(self.endpoint, op_key)
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self._token}",
            "User-Agent": "loop-hybrid-ci-conclusion/1",
        }
        try:
            raw = self._transport(url, headers, self.timeout)
        except CIAdapterError:
            raise
        except (TimeoutError, OSError) as exc:
            raise CIAdapterError("CI source is unavailable") from exc
        return _normalize_response(raw, op_key)
