"""Prometheus query helpers and a node-local TTL cache for co-located fetches."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile
import threading
import time
from typing import Any, Callable, Dict, Optional, Tuple, Union

import requests

logger = logging.getLogger(__name__)

try:
    import fcntl as _fcntl
except ImportError:  # pragma: no cover
    _fcntl = None

DEFAULT_PROMETHEUS_QUERY_CACHE_TTL_S = float(
    os.environ.get("RAY_SERVE_PROMETHEUS_QUERY_CACHE_TTL_S", "1.0")
)


def normalize_prometheus_address(address: str) -> str:
    """Return host:port, stripping an optional http(s):// scheme and path."""
    if not address:
        return address
    addr = address.strip()
    for prefix in ("https://", "http://"):
        if addr.lower().startswith(prefix):
            addr = addr[len(prefix) :]
            break
    return addr.split("/")[0]


def fetch_from_prom_server(
    address: str,
    query: str,
    *,
    time: Union[str, int, float, None] = None,
    timeout: Optional[float] = None,
) -> Dict[str, Any]:
    """Instant-query Prometheus at address (host:port or URL)."""
    if not address:
        raise ValueError("Prometheus address must be non-empty")
    scheme = "https" if address.strip().lower().startswith("https://") else "http"
    hostport = normalize_prometheus_address(address)
    params: Dict[str, Any] = {"query": query}
    if time is not None:
        params["time"] = time
    if timeout is not None:
        params["timeout"] = str(timeout)
    request_timeout = timeout if timeout is not None else 10.0
    response = requests.get(
        f"{scheme}://{hostport}/api/v1/query",
        params=params,
        timeout=request_timeout,
    )
    response.raise_for_status()
    body = response.json()
    # Prometheus may return HTTP 200 with {"status":"error", ...} for bad PromQL.
    if isinstance(body, dict) and body.get("status") == "error":
        raise ValueError(
            "Prometheus query error: "
            f"{body.get('errorType', 'unknown')}: {body.get('error', body)}"
        )
    return body


def extract_instant_query_value(response: Dict[str, Any]) -> Optional[float]:
    """First sample from a PromQL instant query (vector or scalar), else None."""
    try:
        if not response or response.get("status") == "error":
            return None
        data = response.get("data") or {}
        result = data.get("result")
        if result is None:
            return None
        result_type = data.get("resultType")
        if result_type == "scalar" or (
            isinstance(result, (list, tuple))
            and len(result) == 2
            and not isinstance(result[0], dict)
        ):
            value = result[1]
            return float(value) if value is not None else None
        if not result:
            return None
        value = result[0].get("value", [None, None])[1]
        return float(value) if value is not None else None
    except (TypeError, ValueError, IndexError, AttributeError, KeyError):
        return None


class NodeLocalPrometheusQueryCache:
    """TTL cache shared via memory + on-disk files so co-located processes coalesce."""

    def __init__(
        self,
        ttl_s: float = DEFAULT_PROMETHEUS_QUERY_CACHE_TTL_S,
        cache_dir: Optional[str] = None,
    ):
        self._ttl_s = max(0.0, float(ttl_s))
        self._mem: Dict[Tuple[str, str], Tuple[float, Any]] = {}
        self._lock = threading.Lock()
        self._cache_dir = cache_dir or os.path.join(
            tempfile.gettempdir(), "ray_serve_prometheus_query_cache"
        )
        self.hits = 0
        self.misses = 0
        self.fetches = 0

    def _key(self, address: str, query: str) -> Tuple[str, str]:
        return (normalize_prometheus_address(address), query)

    def _disk_path(self, key: Tuple[str, str]) -> str:
        digest = hashlib.sha256(f"{key[0]}\n{key[1]}".encode("utf-8")).hexdigest()
        return os.path.join(self._cache_dir, f"{digest}.json")

    def _is_fresh(self, ts: float, now: float) -> bool:
        # ttl_s <= 0 disables caching (entries never treated as fresh).
        if self._ttl_s <= 0:
            return False
        return (now - ts) <= self._ttl_s

    def _flock(self, fd: int, flags: int) -> None:
        if _fcntl is not None:
            _fcntl.flock(fd, flags)

    def _read_mem(self, key: Tuple[str, str], now: float) -> Optional[Any]:
        entry = self._mem.get(key)
        if entry is None:
            return None
        ts, payload = entry
        return payload if self._is_fresh(ts, now) else None

    def _write_mem(self, key: Tuple[str, str], payload: Any, now: float) -> None:
        self._mem[key] = (now, payload)

    def _read_disk(self, key: Tuple[str, str], now: float) -> Optional[Any]:
        path = self._disk_path(key)
        if not os.path.isfile(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                self._flock(f.fileno(), _fcntl.LOCK_SH if _fcntl else 0)
                try:
                    data = json.load(f)
                finally:
                    self._flock(f.fileno(), _fcntl.LOCK_UN if _fcntl else 0)
            ts = float(data["ts"])
            return data["payload"] if self._is_fresh(ts, now) else None
        except Exception as e:
            logger.debug("Prometheus disk cache read failed: %s", e)
            return None

    def _write_disk(self, key: Tuple[str, str], payload: Any, now: float) -> None:
        path = self._disk_path(key)
        try:
            os.makedirs(self._cache_dir, exist_ok=True)
            fd, tmp_path = tempfile.mkstemp(prefix="promcache_", dir=self._cache_dir)
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    self._flock(f.fileno(), _fcntl.LOCK_EX if _fcntl else 0)
                    json.dump({"ts": now, "payload": payload}, f)
                    f.flush()
                    os.fsync(f.fileno())
                    self._flock(f.fileno(), _fcntl.LOCK_UN if _fcntl else 0)
                os.replace(tmp_path, path)
            except Exception:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
        except Exception as e:
            logger.debug("Prometheus disk cache write failed: %s", e)

    def get_or_fetch(
        self,
        address: str,
        query: str,
        fetch_fn: Optional[Callable[..., Dict[str, Any]]] = None,
        *,
        timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Return cached JSON for (address, query), or fetch and store it.

        Args:
            address: Prometheus host:port or URL.
            query: PromQL instant query string.
            fetch_fn: Optional fetcher; defaults to ``fetch_from_prom_server``.
            timeout: Optional request timeout in seconds.

        Returns:
            Prometheus ``/api/v1/query`` JSON body.
        """
        if fetch_fn is None:
            fetch_fn = fetch_from_prom_server
        key = self._key(address, query)
        now = time.time()

        with self._lock:
            mem_hit = self._read_mem(key, now)
            if mem_hit is not None:
                self.hits += 1
                return mem_hit

        disk_hit = self._read_disk(key, now)
        if disk_hit is not None:
            with self._lock:
                self._write_mem(key, disk_hit, now)
                self.hits += 1
            return disk_hit

        os.makedirs(self._cache_dir, exist_ok=True)
        lock_path = self._disk_path(key) + ".lock"
        lock_f = open(lock_path, "a+", encoding="utf-8")
        try:
            self._flock(lock_f.fileno(), _fcntl.LOCK_EX if _fcntl else 0)
            try:
                now = time.time()
                disk_hit = self._read_disk(key, now)
                if disk_hit is not None:
                    with self._lock:
                        self._write_mem(key, disk_hit, now)
                        self.hits += 1
                    return disk_hit
                with self._lock:
                    self.misses += 1
                    self.fetches += 1
                payload = fetch_fn(address, query, timeout=timeout)
                now = time.time()
                self._write_disk(key, payload, now)
                with self._lock:
                    self._write_mem(key, payload, now)
                return payload
            finally:
                self._flock(lock_f.fileno(), _fcntl.LOCK_UN if _fcntl else 0)
        finally:
            lock_f.close()

    def clear(self) -> None:
        with self._lock:
            self._mem.clear()
            self.hits = self.misses = self.fetches = 0


_DEFAULT_CACHE: Optional[NodeLocalPrometheusQueryCache] = None
_DEFAULT_CACHE_LOCK = threading.Lock()


def get_default_prometheus_query_cache() -> NodeLocalPrometheusQueryCache:
    global _DEFAULT_CACHE
    with _DEFAULT_CACHE_LOCK:
        if _DEFAULT_CACHE is None:
            _DEFAULT_CACHE = NodeLocalPrometheusQueryCache()
        return _DEFAULT_CACHE


def reset_default_prometheus_query_cache_for_tests(
    cache: Optional[NodeLocalPrometheusQueryCache] = None,
) -> NodeLocalPrometheusQueryCache:
    global _DEFAULT_CACHE
    with _DEFAULT_CACHE_LOCK:
        _DEFAULT_CACHE = cache if cache is not None else NodeLocalPrometheusQueryCache()
        return _DEFAULT_CACHE


def cached_fetch_from_prom_server(
    address: str,
    query: str,
    *,
    timeout: Optional[float] = None,
    cache: Optional[NodeLocalPrometheusQueryCache] = None,
    fetch_fn: Optional[Callable[..., Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    c = cache if cache is not None else get_default_prometheus_query_cache()
    return c.get_or_fetch(address, query, fetch_fn=fetch_fn, timeout=timeout)
