"""RayCluster client, JobResult handle, and top-level run helper."""

import copy
import inspect
import json
import math
import os
import threading
import time
import urllib.parse
import uuid
import warnings
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

try:
    import requests
except ImportError:  # pragma: no cover - allows `import ray_client.packaging` without requests
    requests = None  # type: ignore[assignment]

from ray_client.actors import ActorHandle
from ray_client.exceptions import (
    RayClientError,
    RayJobExecutionError,
    RayJobTimeoutError,
)
from ray_client.packaging import (
    build_working_dir_package,
    extract_class_source_and_globals,
    extract_function_source_and_globals,
    to_json,
    unwrap_actor_class,
)

_RETRYABLE_HTTP_STATUS = frozenset({502, 503, 504})
_TERMINAL_JOB_STATUSES = frozenset({"SUCCEEDED", "FAILED", "STOPPED"})
_MAX_RETRIES = 5
_UNSET: Any = object()
_AUTH_HINT = (
    " The cluster requires token authentication: set RAY_AUTH_TOKEN or "
    "RAY_AUTH_TOKEN_PATH, or pass headers={'Authorization': 'Bearer <token>'}."
)


def _resolve_address(address: Optional[str]) -> str:
    if address is not None:
        if not isinstance(address, str) or not address.strip():
            raise RayClientError(f"Invalid Ray Dashboard HTTP address: {address!r}.")
        addr_stripped = address.strip()
        explicit = addr_stripped if addr_stripped != "auto" else None
    else:
        explicit = None

    api_env_raw = os.environ.get("RAY_API_SERVER_ADDRESS")
    api_env = (
        api_env_raw.strip()
        if api_env_raw and api_env_raw.strip() and api_env_raw.strip() != "auto"
        else None
    )
    ray_env_raw = os.environ.get("RAY_ADDRESS")
    ray_env = (
        ray_env_raw.strip()
        if ray_env_raw and ray_env_raw.strip() and ray_env_raw.strip() != "auto"
        else None
    )

    if explicit is not None:
        raw, from_ray_address_env = explicit, False
    elif api_env is not None:
        raw, from_ray_address_env = api_env, False
    elif ray_env is not None:
        raw, from_ray_address_env = ray_env, True
    else:
        raw, from_ray_address_env = "http://127.0.0.1:8265", False

    raw = raw.strip()
    lower = raw.lower()
    if lower.startswith("ray://"):
        raise RayClientError(
            f"Address '{raw}' uses the legacy Ray Client gRPC scheme ('ray://'). "
            "Pass the Ray Dashboard HTTP address, e.g. 'http://<head>:8265'."
        )
    if lower == "local":
        raise RayClientError(
            "Address 'local' is not a Ray Dashboard HTTP address. "
            "Pass 'http://<head>:8265' or set RAY_API_SERVER_ADDRESS."
        )
    if from_ray_address_env and not lower.startswith(("http://", "https://")):
        if "RAY_JOB_CONFIG_JSON_ENV_VAR" in os.environ or raw.endswith(":6379"):
            raise RayClientError(
                f"RAY_ADDRESS={raw!r} points to the Ray GCS gRPC port rather than "
                "the Ray Dashboard HTTP server. Pass address='http://<head>:8265' or "
                "set RAY_API_SERVER_ADDRESS."
            )
    if lower.startswith("http://"):
        raw = "http://" + raw[7:]
    elif lower.startswith("https://"):
        raw = "https://" + raw[8:]
    elif "://" in raw:
        raise RayClientError(
            f"Unsupported scheme in address '{raw}'; expected 'http://' or 'https://'."
        )
    else:
        raw = f"http://{raw}"
    normalized = raw.rstrip("/")
    parsed = urllib.parse.urlsplit(normalized)
    if not parsed.netloc:
        raise RayClientError(f"Invalid Ray Dashboard HTTP address: {raw!r}.")
    return normalized


def _load_auth_token() -> Optional[str]:
    """Same precedence as Ray: RAY_AUTH_TOKEN, RAY_AUTH_TOKEN_PATH, then ~/.ray/auth_token."""
    if os.environ.get("RAY_AUTH_MODE", "").strip().lower() == "disabled":
        return None
    token = os.environ.get("RAY_AUTH_TOKEN")
    if token:
        cleaned = token.strip()
        if "\n" in cleaned or "\r" in cleaned:
            raise RayClientError("RAY_AUTH_TOKEN must not contain newline characters.")
        return cleaned

    explicit_path = os.environ.get("RAY_AUTH_TOKEN_PATH")
    if explicit_path:
        token_path = Path(explicit_path)
        if not token_path.is_file():
            raise RayClientError(
                f"RAY_AUTH_TOKEN_PATH={explicit_path!r} does not exist or is not a file."
            )
    else:
        try:
            token_path = Path.home() / ".ray" / "auth_token"
        except RuntimeError:
            return None
        if not token_path.is_file():
            return None

    content = token_path.read_text(encoding="utf-8").strip()
    if not content:
        return None
    if "\n" in content or "\r" in content:
        raise RayClientError(
            f"Auth token file '{token_path}' must contain a single-line token."
        )
    return content


def _exclude_patterns(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)) and all(isinstance(p, str) for p in value):
        return list(value)
    raise TypeError(
        "runtime_env['excludes'] must be a list of gitignore-syntax strings, "
        f"got {value!r}."
    )


def _parse_entrypoint_str(target: str, kind: str) -> Tuple[str, str]:
    if ":" in target:
        mod_path, attr_name = target.split(":", 1)
    elif "." in target:
        mod_path, attr_name = target.rsplit(".", 1)
    else:
        raise ValueError(
            f"{kind} string entrypoint must be 'module:name' or 'module.name', got {target!r}."
        )
    if not mod_path.strip() or not attr_name.strip():
        raise ValueError(
            f"{kind} string entrypoint must be 'module:name' or 'module.name', got {target!r}."
        )
    return mod_path.strip(), attr_name.strip()


def _validate_ttl_seconds(ttl_seconds: Any) -> Optional[float]:
    if ttl_seconds is None:
        return None
    if isinstance(ttl_seconds, bool) or not isinstance(ttl_seconds, (int, float)):
        raise ValueError(f"ttl_seconds must be > 0 or None, got {ttl_seconds!r}.")
    val = float(ttl_seconds)
    if not math.isfinite(val) or val <= 0:
        raise ValueError(f"ttl_seconds must be > 0 or None, got {ttl_seconds!r}.")
    return val


def _validate_timeout(timeout: Any, name: str = "timeout") -> Optional[float]:
    if timeout is None:
        return None
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, (int, float))
        or not math.isfinite(timeout)
        or timeout <= 0
    ):
        raise ValueError(
            f"{name} must be a positive finite number or None, got {timeout!r}."
        )
    return float(timeout)


def _validate_max_concurrency(max_concurrency: Any) -> int:
    if (
        isinstance(max_concurrency, bool)
        or not isinstance(max_concurrency, int)
        or max_concurrency < 1
    ):
        raise ValueError(
            f"max_concurrency must be a positive integer, got {max_concurrency!r}."
        )
    return max_concurrency


def _validate_label_selector(
    label_selector: Any, name: str = "label_selector"
) -> Optional[Dict[str, str]]:
    if label_selector is None:
        return None
    if not isinstance(label_selector, dict):
        raise ValueError(f"{name} must be a dict[str, str], got {label_selector!r}.")
    out: Dict[str, str] = {}
    for k, v in label_selector.items():
        if not isinstance(k, str) or not k.strip() or not isinstance(v, str):
            raise ValueError(
                f"{name} keys must be non-empty strings and values must be strings, got {k!r}: {v!r}."
            )
        out[k] = v
    return out


def _validate_get_if_exists(get_if_exists: Any) -> Union[bool, str]:
    if isinstance(get_if_exists, bool) or get_if_exists == "recreate":
        return get_if_exists
    raise ValueError(
        f"get_if_exists must be True, False, or 'recreate', got {get_if_exists!r}."
    )


def _is_timeout_exception(exc: BaseException) -> bool:
    timeout_types: Tuple[type, ...] = (
        (TimeoutError, requests.exceptions.Timeout)
        if requests is not None
        else (TimeoutError,)
    )
    curr: Optional[BaseException] = exc
    seen: set[int] = set()
    while curr is not None and id(curr) not in seen:
        seen.add(id(curr))
        if isinstance(curr, timeout_types):
            return True
        curr = curr.__cause__ or curr.__context__
    return "timed out" in str(exc).lower()


def _validate_resource_num(
    val: Any, name: str, *, integer: bool = False
) -> Union[int, float]:
    if (
        isinstance(val, bool)
        or not isinstance(val, (int, float))
        or not math.isfinite(val)
        or val < 0
    ):
        raise ValueError(
            f"Invalid {name} value {val!r}; must be a non-negative finite number."
        )
    if integer:
        if isinstance(val, float) and not val.is_integer():
            raise ValueError(
                f"Invalid {name} value {val!r}; must be an integer number of bytes."
            )
        return int(val)
    return val


def _normalize_resources(
    num_cpus: Optional[Union[int, float]],
    num_gpus: Optional[Union[int, float]],
    memory: Optional[Union[int, float]],
    resources: Optional[Dict[str, float]],
    *,
    default_cpus: Optional[Union[int, float]] = None,
) -> Tuple[
    Optional[Union[int, float]],
    Optional[Union[int, float]],
    Optional[int],
    Optional[Dict[str, float]],
]:
    """Normalizes Ray 2.x (`num_cpus`, `num_gpus`, `memory`) and Ray 3.0
    (`resources={"CPU": ..., "GPU": ..., "memory": ...}`) resource specifications."""
    if num_cpus is not None:
        num_cpus = _validate_resource_num(num_cpus, "num_cpus")
    if num_gpus is not None:
        num_gpus = _validate_resource_num(num_gpus, "num_gpus")
    if memory is not None:
        memory = int(_validate_resource_num(memory, "memory", integer=True))
    custom: Dict[str, float] = {}
    if resources is not None:
        if not isinstance(resources, dict):
            raise TypeError(f"resources must be a dict[str, float], got {resources!r}.")
        for k, v in resources.items():
            if not isinstance(k, str):
                raise ValueError(
                    f"Invalid resource key {k!r}; resource keys must be strings."
                )
            v_num = _validate_resource_num(
                v, f"resources[{k!r}]", integer=(k == "memory")
            )
            if k == "CPU":
                if num_cpus is not None and float(num_cpus) != float(v_num):
                    raise ValueError(
                        f"Conflicting CPU requirements: num_cpus={num_cpus!r} vs "
                        f"resources['CPU']={v!r}."
                    )
                num_cpus = v_num
            elif k == "GPU":
                if num_gpus is not None and float(num_gpus) != float(v_num):
                    raise ValueError(
                        f"Conflicting GPU requirements: num_gpus={num_gpus!r} vs "
                        f"resources['GPU']={v!r}."
                    )
                num_gpus = v_num
            elif k == "memory":
                if memory is not None and int(memory) != int(v_num):
                    raise ValueError(
                        f"Conflicting memory requirements: memory={memory!r} vs "
                        f"resources['memory']={v!r}."
                    )
                memory = int(v_num)
            elif v_num > 0:
                custom[k] = float(v_num)
    if num_cpus is None and default_cpus is not None:
        num_cpus = default_cpus
    return num_cpus, num_gpus, memory, (custom or None)


def _validate_node_id_and_label_selector(
    node_id: Any, label_selector: Any
) -> Tuple[Optional[str], Optional[Dict[str, str]]]:
    validated_selector = _validate_label_selector(label_selector, "label_selector")
    if node_id is not None:
        if not isinstance(node_id, str) or not node_id.strip():
            raise ValueError(f"node_id must be a non-empty string, got {node_id!r}.")
        node_id = node_id.strip()
    if node_id is not None and validated_selector:
        raise ValueError("Cannot specify both 'node_id' and 'label_selector'.")
    return node_id, validated_selector


class JobResult:
    """Handle to a submitted CLI job on `/api/jobs/`.

    `JobResult` polls job status and logs on the Ray Dashboard REST API and caches
    metadata and logs once the job reaches a terminal status (`SUCCEEDED`, `FAILED`,
    or `STOPPED`).

    Args:
        submission_id: Unique job submission ID on `/api/jobs/`.
        cluster: Owning `RayCluster` client instance.
        package_uploaded: Whether submitting this job uploaded a new `working_dir`
            package to the cluster.
        initial_info: Optional initial job status payload fetched on reattach.
    """

    def __init__(
        self,
        submission_id: str,
        cluster: "RayCluster",
        *,
        package_uploaded: bool = False,
        initial_info: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not isinstance(submission_id, str) or not submission_id.strip():
            raise ValueError("submission_id must be a non-empty string.")
        self._submission_id = submission_id
        self._cluster = cluster
        self._package_uploaded = package_uploaded
        self._terminal_info: Optional[Dict[str, Any]] = None
        self._terminal_logs: Optional[str] = None
        if (
            isinstance(initial_info, dict)
            and initial_info.get("status") in _TERMINAL_JOB_STATUSES
        ):
            self._terminal_info = dict(initial_info)

    def _quoted_id(self) -> str:
        return urllib.parse.quote(self._submission_id, safe="")

    def __repr__(self) -> str:
        return (
            f"JobResult(submission_id={self._submission_id!r}, "
            f"address={self._cluster.address!r})"
        )

    @property
    def submission_id(self) -> str:
        """Job submission ID on `/api/jobs/`."""
        return self._submission_id

    @property
    def package_uploaded(self) -> bool:
        """Whether submitting this job uploaded a new `working_dir` package."""
        return self._package_uploaded

    def info(self) -> Dict[str, Any]:
        """Fetches job status and metadata from `GET /api/jobs/{submission_id}`.

        Returns:
            Dictionary of job metadata, including `status`, `entrypoint`, and `message`.

        Raises:
            RayClientError: If the job does not exist or the HTTP request fails.
        """
        if self._terminal_info is not None:
            return dict(self._terminal_info)
        data = self._cluster._json("GET", f"/api/jobs/{self._quoted_id()}")
        if isinstance(data, dict) and data.get("status") in _TERMINAL_JOB_STATUSES:
            self._terminal_info = dict(data)
        return data

    def status(self) -> str:
        """Returns the current job status string (e.g. `PENDING`, `RUNNING`, `SUCCEEDED`).

        Returns:
            Current job status string.

        Raises:
            RayClientError: If the job does not exist or the HTTP request fails.
        """
        return str(self.info().get("status", "UNKNOWN"))

    def done(self) -> bool:
        """Returns whether the job has reached a terminal status (`SUCCEEDED`, `FAILED`, `STOPPED`).

        Returns:
            `True` if the job is in a terminal state, `False` otherwise.

        Raises:
            RayClientError: If the job does not exist or the HTTP request fails.
        """
        return self.status() in _TERMINAL_JOB_STATUSES

    def logs(self) -> str:
        """Fetches aggregated stdout and stderr logs for the job.

        Returns:
            Log text for the job from `GET /api/jobs/{submission_id}/logs`.

        Raises:
            RayClientError: If the job does not exist or the HTTP request fails.
        """
        if self._terminal_logs is not None:
            return self._terminal_logs
        is_terminal = self._terminal_info is not None
        data = self._cluster._json("GET", f"/api/jobs/{self._quoted_id()}/logs")
        text = str(data.get("logs", "")) if isinstance(data, dict) else str(data)
        if is_terminal:
            self._terminal_logs = text
        return text

    def wait(
        self,
        timeout: Optional[float] = None,
        on_log_line: Optional[Callable[[str], None]] = None,
    ) -> str:
        """Polls until the job reaches a terminal status.

        Args:
            timeout: Optional maximum number of seconds to wait before raising
                `RayJobTimeoutError` (leaving the job running on the cluster).
            on_log_line: Optional callback invoked for each newly emitted log line while
                waiting.

        Returns:
            Terminal status string (`SUCCEEDED`, `FAILED`, or `STOPPED`).

        Raises:
            RayJobTimeoutError: If the job does not reach a terminal state within
                `timeout` seconds.
            RayClientError: If polling the job status or logs fails.
        """
        timeout = _validate_timeout(timeout, "timeout")
        deadline = (time.monotonic() + float(timeout)) if timeout is not None else None
        delay = 0.1
        log_offset = 0
        pending_buf = ""

        def _drain_logs(final: bool = False) -> None:
            nonlocal log_offset, pending_buf
            if on_log_line is None:
                return
            current = self.logs()
            if len(current) > log_offset:
                pending_buf += current[log_offset:]
                log_offset = len(current)
            while "\n" in pending_buf:
                line, pending_buf = pending_buf.split("\n", 1)
                on_log_line(line.rstrip("\r"))
            if final and pending_buf:
                on_log_line(pending_buf.rstrip("\r"))
                pending_buf = ""

        while True:
            info = self.info()
            st = str(info.get("status", "UNKNOWN"))
            if st in _TERMINAL_JOB_STATUSES:
                _drain_logs(final=True)
                return st
            _drain_logs(final=False)
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise RayJobTimeoutError(
                        f"Job '{self._submission_id}' did not finish within "
                        f"{timeout}s (last status: {st}).",
                        job=self,
                        submission_id=self._submission_id,
                    )
                sleep_for = min(delay, remaining)
            else:
                sleep_for = delay
            time.sleep(sleep_for)
            delay = min(delay * 1.5, 2.0)

    def stop(self) -> bool:
        """Requests the cluster to stop the running job (`POST /api/jobs/{id}/stop`).

        Returns:
            `True` if the job was stopped, or `False` if the job was not found or
            already stopped.

        Raises:
            RayClientError: If the HTTP request fails with a non-404 status.
        """
        stop_endpoint = f"/api/jobs/{self._quoted_id()}/stop"
        resp = self._cluster.request(
            "POST",
            stop_endpoint,
            idempotent=False,
        )
        if resp.status_code == 404:
            return False
        if resp.status_code != 200:
            raise self._cluster._http_error("POST", stop_endpoint, resp)
        self._terminal_info = None
        return bool(resp.json().get("stopped", False))

    def delete(self) -> bool:
        """Deletes the job record (`DELETE /api/jobs/{id}`).

        Returns:
            `True` if the job was deleted, or `False` if the job was not found.

        Raises:
            RayClientError: If the HTTP request fails with a non-404 status.
        """
        del_endpoint = f"/api/jobs/{self._quoted_id()}"
        resp = self._cluster.request("DELETE", del_endpoint)
        if resp.status_code == 404:
            return False
        if resp.status_code != 200:
            raise self._cluster._http_error("DELETE", del_endpoint, resp)
        return bool(resp.json().get("deleted", False))


class _SSEStreamIterator(Iterator[Any]):
    """Iterator over an SSE response stream that guarantees cleanup even if never advanced."""

    def __init__(
        self,
        inner_gen: Iterator[Any],
        resp: "requests.Response",
        on_close: Optional[Callable[[], None]] = None,
    ) -> None:
        self._inner_gen = inner_gen
        self._resp = resp
        self._callbacks: List[Callable[[], None]] = []
        if on_close is not None:
            self._callbacks.append(on_close)
        self._closed = False

    def _add_close_callback(self, cb: Callable[[], None]) -> None:
        if self._closed:
            cb()
        else:
            self._callbacks.append(cb)

    def __iter__(self) -> "_SSEStreamIterator":
        return self

    def __next__(self) -> Any:
        if self._closed:
            raise StopIteration
        try:
            return next(self._inner_gen)
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            close_fn = getattr(self._inner_gen, "close", None)
            if callable(close_fn):
                close_fn()
        finally:
            try:
                self._resp.close()
            finally:
                callbacks = list(self._callbacks)
                self._callbacks.clear()
                for cb in callbacks:
                    try:
                        cb()
                    except Exception:
                        pass

    def __enter__(self) -> "_SSEStreamIterator":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass


class RayCluster:
    """Unified HTTP client for submitting CLI jobs, driver functions, stateful actors, and cluster node management.

    Args:
        address: Ray Dashboard HTTP address (e.g. `"http://127.0.0.1:8265"`). If `None`
            or `"auto"`, resolves from `RAY_API_SERVER_ADDRESS`, `RAY_ADDRESS`, or
            defaults to `"http://127.0.0.1:8265"`.
        headers: Optional HTTP headers included on every request.
        verify: TLS certificate verification boolean or path to a CA bundle.
        default_runtime_env: Default `runtime_env` merged into `run()`, `actor()`, and
            `submit_job()` calls.
        timeout: Default HTTP request timeout in seconds for control operations.

    Raises:
        ImportError: If `requests` is not installed.
        ValueError: If `timeout` is not a positive finite number.
        RayClientError: If `address` uses an unsupported scheme or auth token loading fails.
    """

    def __init__(
        self,
        address: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        verify: Union[bool, str] = True,
        default_runtime_env: Optional[Dict[str, Any]] = None,
        timeout: float = 30.0,
    ) -> None:
        if requests is None:
            raise ImportError(
                "RayCluster requires the 'requests' package: pip install requests"
            )
        if timeout is None:
            raise ValueError("timeout must be a positive finite number.")
        self._timeout = float(_validate_timeout(timeout, "timeout"))
        self._address = _resolve_address(address)
        self._headers = dict(headers or {})
        token = _load_auth_token()
        if token and not any(k.lower() == "authorization" for k in self._headers):
            self._headers["Authorization"] = f"Bearer {token}"
        self._verify = verify
        self._default_runtime_env = copy.deepcopy(default_runtime_env) or {}
        self._thread_local = threading.local()
        self._sessions_lock = threading.Lock()
        self._sessions: List["requests.Session"] = []
        self._upload_locks_guard = threading.Lock()
        self._upload_locks: Dict[str, threading.Lock] = {}

    def __enter__(self) -> "RayCluster":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def close(self) -> None:
        """Closes all open HTTP sessions held by this client."""
        with self._sessions_lock:
            sessions = list(self._sessions)
            self._sessions.clear()
        self._thread_local.session = None
        for sess in sessions:
            try:
                sess.close()
            except Exception:
                pass

    def __getstate__(self) -> Dict[str, Any]:
        state = dict(self.__dict__)
        state.pop("_thread_local", None)
        state.pop("_sessions_lock", None)
        state.pop("_sessions", None)
        state.pop("_upload_locks_guard", None)
        state.pop("_upload_locks", None)
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._thread_local = threading.local()
        self._sessions_lock = threading.Lock()
        self._sessions = []
        self._upload_locks_guard = threading.Lock()
        self._upload_locks = {}

    @property
    def address(self) -> str:
        """Resolved Ray Dashboard HTTP base URL without trailing slash."""
        return self._address

    @property
    def _session(self) -> "requests.Session":
        sess = getattr(self._thread_local, "session", None)
        if sess is None:
            sess = requests.Session()
            self._thread_local.session = sess
            with self._sessions_lock:
                self._sessions.append(sess)
        return sess

    def _uri_lock(self, package_uri: str) -> threading.Lock:
        with self._upload_locks_guard:
            lock = self._upload_locks.get(package_uri)
            if lock is None:
                lock = self._upload_locks[package_uri] = threading.Lock()
            return lock

    def request(
        self,
        method: str,
        endpoint: str,
        *,
        data: Optional[bytes] = None,
        json_data: Optional[Dict[str, Any]] = None,
        idempotent: bool = True,
        timeout: Optional[float] = None,
        stream: bool = False,
    ) -> "requests.Response":
        """Sends an HTTP request to the Ray Dashboard server.

        Args:
            method: HTTP method (e.g. `"GET"`, `"POST"`, `"PUT"`, `"DELETE"`).
            endpoint: API path beginning with `"/"` (e.g. `"/api/actors"`).
            data: Optional raw request body bytes.
            json_data: Optional JSON-serializable request body dictionary.
            idempotent: If `True`, automatically retries on transient network errors
                and HTTP 502/503/504 responses.
            timeout: Optional timeout in seconds (or `(connect, read)` tuple).
            stream: Whether to stream the HTTP response body.

        Returns:
            The `requests.Response` object.

        Raises:
            RayClientError: If the HTTP request fails after retries.
        """
        url = f"{self._address}{endpoint}"
        req_timeout = self._timeout if timeout is None else timeout
        attempts = _MAX_RETRIES if idempotent else 1
        delay = 0.2
        last_exc: Optional[BaseException] = None
        non_retryable = (
            requests.exceptions.InvalidURL,
            requests.exceptions.InvalidHeader,
            requests.exceptions.SSLError,
        )
        for attempt in range(attempts):
            try:
                resp = self._session.request(
                    method,
                    url,
                    data=data,
                    json=json_data,
                    headers=self._headers,
                    verify=self._verify,
                    timeout=req_timeout,
                    stream=stream,
                )
            except non_retryable as exc:
                raise RayClientError(f"HTTP {method} {url} failed: {exc}") from exc
            except requests.exceptions.RequestException as exc:
                last_exc = exc
            else:
                if resp.status_code not in _RETRYABLE_HTTP_STATUS or not idempotent:
                    return resp
                last_exc = RayClientError(
                    f"HTTP {resp.status_code} from {url}: {resp.text[:200]}"
                )
            if attempt + 1 < attempts:
                time.sleep(delay)
                delay = min(delay * 2, 3.0)
        raise RayClientError(
            f"HTTP {method} {url} failed after {attempts} attempt(s): {last_exc}"
        ) from last_exc

    def _json(self, method: str, endpoint: str, **kwargs: Any) -> Any:
        resp = self._ok(method, endpoint, **kwargs)
        try:
            return resp.json()
        except ValueError as exc:
            raise RayClientError(
                f"{method} {endpoint} returned a non-JSON response from "
                f"{self._address}; is this the Ray Dashboard HTTP address? Body starts "
                f"with: {resp.text[:200]!r}"
            ) from exc

    def _ok(self, method: str, endpoint: str, **kwargs: Any) -> "requests.Response":
        resp = self.request(method, endpoint, **kwargs)
        if resp.status_code != 200:
            raise self._http_error(method, endpoint, resp)
        return resp

    @staticmethod
    def _http_error(
        method: str, endpoint: str, resp: "requests.Response", hint: str = ""
    ) -> RayClientError:
        if resp.status_code in (401, 403):
            hint = _AUTH_HINT + hint
        return RayClientError(
            f"{method} {endpoint} failed ({resp.status_code}): {resp.text}{hint}"
        )

    @staticmethod
    def _raise_remote_error(
        target: str,
        err: Dict[str, Any],
        logs: str = "",
    ) -> None:
        exc_type = err.get("exc_type")
        exc_message = err.get("exc_message") or f"Remote execution '{target}' failed"
        if err.get("timed_out"):
            raise RayJobTimeoutError(exc_message, submission_id=target)
        raise RayJobExecutionError(
            submission_id=target,
            status="FAILED",
            message=exc_message,
            exc_type=exc_type,
            exc_message=exc_message,
            remote_traceback=err.get("traceback"),
            logs=logs,
        )

    def _iter_sse_stream_from_resp(
        self,
        resp: "requests.Response",
        *,
        target: str,
        submission_id: Optional[str] = None,
        timeout: Optional[float] = None,
        on_close: Optional[Callable[[], None]] = None,
    ) -> Iterator[Any]:
        sub_id = submission_id or target
        deadline = (time.monotonic() + float(timeout)) if timeout is not None else None
        close_called = False
        completed_normally = False

        def _run_on_close() -> None:
            nonlocal close_called
            if not close_called:
                close_called = True
                if on_close is not None and not completed_normally:
                    on_close()

        def _gen() -> Iterator[Any]:
            nonlocal completed_normally
            try:
                try:
                    for raw_line in resp.iter_lines():
                        if deadline is not None and time.monotonic() >= deadline:
                            raise RayJobTimeoutError(
                                f"Stream '{target}' timed out after {timeout}s.",
                                submission_id=sub_id,
                            )
                        if not raw_line:
                            continue
                        line = raw_line.decode("utf-8").strip()
                        if not line or line.startswith(":"):
                            continue
                        if line.startswith("data:"):
                            line = line[5:].strip()
                            if not line:
                                continue
                        payload = json.loads(line)
                        if not payload.get("ok", True):
                            self._raise_remote_error(
                                target,
                                payload.get("error") or {},
                                logs=str(payload.get("logs") or ""),
                            )
                        if payload.get("done"):
                            completed_normally = True
                            break
                        if "item" in payload:
                            yield payload["item"]
                    if not completed_normally:
                        raise RayClientError(
                            f"SSE stream '{target}' closed prematurely before receiving completion frame."
                        )
                except requests.exceptions.RequestException as exc:
                    if timeout is not None and (
                        _is_timeout_exception(exc)
                        or (deadline is not None and time.monotonic() >= deadline)
                    ):
                        raise RayJobTimeoutError(
                            f"Stream '{target}' timed out after {timeout}s.",
                            submission_id=sub_id,
                        ) from exc
                    raise RayClientError(f"Stream '{target}' failed: {exc}") from exc
            finally:
                resp.close()
                _run_on_close()

        return _SSEStreamIterator(_gen(), resp, on_close=_run_on_close)

    def _read_json_stream_response(
        self,
        resp: "requests.Response",
        *,
        endpoint: str,
        timeout: Optional[float],
        timeout_message: str,
        submission_id: str,
    ) -> Any:
        raw_text: Optional[str] = None
        try:
            if timeout is not None:
                grace_deadline = time.monotonic() + float(timeout) + 5.0
                chunks: List[bytes] = []
                for chunk in resp.iter_content(chunk_size=None):
                    if chunk:
                        chunks.append(chunk)
                    if time.monotonic() >= grace_deadline:
                        raise RayJobTimeoutError(
                            timeout_message,
                            submission_id=submission_id,
                        )
                raw_text = b"".join(chunks).decode("utf-8", errors="replace")
                return json.loads(raw_text)
            return resp.json()
        except ValueError as exc:
            preview = raw_text[:200] if raw_text is not None else resp.text[:200]
            raise RayClientError(
                f"POST {endpoint} returned a non-JSON response from "
                f"{self._address}; is this the Ray Dashboard HTTP address? Body starts "
                f"with: {preview!r}"
            ) from exc
        except requests.exceptions.RequestException as exc:
            if timeout is not None and _is_timeout_exception(exc):
                raise RayJobTimeoutError(
                    timeout_message,
                    submission_id=submission_id,
                ) from exc
            raise RayClientError(f"POST {endpoint} failed: {exc}") from exc

    def _call_or_stream_actor_method(
        self,
        endpoint: str,
        body: Dict[str, Any],
        *,
        target: str,
        submission_id: Optional[str] = None,
        timeout: Optional[float] = None,
        on_close: Optional[Callable[[], None]] = None,
        is_actor_method: bool = False,
        require_stream: bool = False,
    ) -> Any:
        timeout = _validate_timeout(timeout, "timeout")
        sub_id = submission_id or target
        req_body = dict(body)
        if timeout is not None:
            req_body["timeout"] = float(timeout)
        http_timeout: Any = (
            (min(self._timeout, float(timeout) + 5.0), float(timeout) + 5.0)
            if timeout is not None
            else (self._timeout, None)
        )
        kind_label = "Actor method" if is_actor_method else "Function"
        timeout_msg = f"{kind_label} '{target}' timed out after {timeout}s."
        try:
            resp = self.request(
                "POST",
                endpoint,
                json_data=req_body,
                idempotent=False,
                timeout=http_timeout,
                stream=True,
            )
        except BaseException as exc:
            if on_close is not None:
                on_close()
            if (
                isinstance(exc, RayClientError)
                and timeout is not None
                and _is_timeout_exception(exc)
            ):
                raise RayJobTimeoutError(
                    timeout_msg,
                    submission_id=sub_id,
                ) from exc
            raise

        if resp.status_code != 200:
            try:
                err_exc = self._http_error("POST", endpoint, resp)
            finally:
                resp.close()
                if on_close is not None:
                    on_close()
            raise err_exc

        content_type = resp.headers.get("Content-Type", "")
        if "text/event-stream" in content_type.lower():
            return self._iter_sse_stream_from_resp(
                resp,
                target=target,
                submission_id=sub_id,
                timeout=timeout,
                on_close=on_close,
            )

        reaped_by_server = False
        try:
            data = self._read_json_stream_response(
                resp,
                endpoint=endpoint,
                timeout=timeout,
                timeout_message=timeout_msg,
                submission_id=sub_id,
            )
            reaped_by_server = isinstance(data, dict)
            if not data.get("ok", True):
                self._raise_remote_error(
                    target,
                    data.get("error") or {},
                    logs=str(data.get("logs") or ""),
                )
            if require_stream:
                raise RayClientError(
                    f"Expected SSE stream from '{target}', but method returned a "
                    "non-streaming JSON response."
                )
            return data.get("result")
        finally:
            resp.close()
            if on_close is not None and not reaped_by_server:
                on_close()

    def _ensure_package_uploaded(self, package_uri: str, zip_bytes: bytes) -> bool:
        """Uploads `zip_bytes` unless `package_uri` is already cached on the cluster."""
        protocol, name = package_uri.split("://", 1)
        endpoint = f"/api/packages/{protocol}/{name}"
        with self._uri_lock(package_uri):
            check = self.request("GET", endpoint)
            if check.status_code == 200:
                return False
            if check.status_code != 404:
                raise self._http_error("GET", endpoint, check)
            put = self.request("PUT", endpoint, data=zip_bytes)
            if put.status_code != 200:
                hint = ""
                if put.status_code == 413:
                    hint = (
                        f" The {len(zip_bytes) / 1024**2:.1f} MiB package exceeds a "
                        "request body limit of the cluster or of a proxy in front of it; "
                        "shrink working_dir with runtime_env['excludes'] or raise the "
                        "proxy's limit."
                    )
                raise self._http_error("PUT", endpoint, put, hint)
            return True

    def _prepare_runtime_env(
        self,
        runtime_env: Optional[Dict[str, Any]],
        base_runtime_env: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Dict[str, Any], Optional[Union[str, Path]], List[str]]:
        merged: Dict[str, Any] = {}
        excludes: List[str] = []
        for env in (self._default_runtime_env, base_runtime_env, runtime_env):
            for key, value in (env or {}).items():
                if key == "env_vars":
                    if value is not None:
                        if not isinstance(value, dict):
                            raise TypeError(
                                f"runtime_env['env_vars'] must be a dict, got {value!r}."
                            )
                        merged.setdefault("env_vars", {}).update(value)
                elif key == "excludes":
                    excludes.extend(_exclude_patterns(value))
                else:
                    merged[key] = copy.deepcopy(value)

        for conflict_key in ("image_uri", "container"):
            if merged.get(conflict_key) is not None:
                raise RayClientError(
                    f"runtime_env[{conflict_key!r}] is not supported; "
                    "configure the container image on the cluster."
                )

        working_dir = merged.pop("working_dir", None)
        if working_dir is not None and "://" in str(working_dir):
            merged["working_dir"] = str(working_dir)
            working_dir = None
        py_modules = merged.get("py_modules")
        if py_modules is not None:
            if not isinstance(py_modules, (list, tuple)) or not all(
                isinstance(m, str) for m in py_modules
            ):
                raise TypeError(
                    "runtime_env['py_modules'] must be a list of remote URI strings, "
                    f"got {py_modules!r}."
                )
            merged["py_modules"] = list(py_modules)
            for module in merged["py_modules"]:
                if "://" not in str(module):
                    raise RayClientError(
                        f"runtime_env['py_modules'] entry {module!r} is a local path; "
                        "copy it into runtime_env['working_dir'] or reference a remote URI."
                    )
        return merged, working_dir, excludes

    def _prepare_and_upload_runtime_env(
        self,
        runtime_env: Optional[Dict[str, Any]],
        base_runtime_env: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Optional[Dict[str, Any]], bool]:
        merged_env, working_dir, excludes = self._prepare_runtime_env(
            runtime_env, base_runtime_env=base_runtime_env
        )
        uploaded = False
        if working_dir is not None:
            zip_bytes, pkg_uri = build_working_dir_package(working_dir, excludes)
            uploaded = self._ensure_package_uploaded(pkg_uri, zip_bytes)
            merged_env["working_dir"] = pkg_uri
        return (merged_env or None), uploaded

    def _build_driver_actor_spec(
        self,
        fn: Union[Callable[..., Any], str],
        args: Tuple[Any, ...] = (),
        kwargs: Optional[Dict[str, Any]] = None,
        runtime_env: Optional[Dict[str, Any]] = None,
        num_cpus: Optional[Union[int, float]] = None,
        num_gpus: Optional[Union[int, float]] = None,
        resources: Optional[Dict[str, float]] = None,
        *,
        node_id: Optional[str] = None,
        label_selector: Optional[Dict[str, str]] = None,
        allow_generator: bool = False,
    ) -> Tuple[Dict[str, Any], List[Any], Dict[str, Any]]:
        validated_node_id, validated_selector = _validate_node_id_and_label_selector(
            node_id, label_selector
        )
        (
            resolved_cpus,
            resolved_gpus,
            resolved_memory,
            resolved_resources,
        ) = _normalize_resources(num_cpus, num_gpus, None, resources, default_cpus=0)
        if isinstance(fn, str):
            module_path, fn_name = _parse_entrypoint_str(fn, "Function")
            module_source = None
            captured_globals: Optional[Dict[str, Any]] = None
            bound_args = list(args)
            bound_kwargs = dict(kwargs or {})
        else:
            module_path = None
            (
                fn_name,
                module_source,
                captured_globals,
            ) = extract_function_source_and_globals(fn, allow_generator=allow_generator)
            inspect.signature(fn).bind(*args, **(kwargs or {}))
            bound_args = list(args)
            bound_kwargs = dict(kwargs or {})

        to_json(bound_args, "Function positional arguments")
        to_json(bound_kwargs, "Function keyword arguments")
        if captured_globals is not None:
            to_json(captured_globals, "Captured globals")

        merged_env, _ = self._prepare_and_upload_runtime_env(runtime_env)

        actor_name = f"driver_{fn_name}_{uuid.uuid4().hex[:8]}"
        spec: Dict[str, Any] = {
            "name": actor_name,
            "kind": "function",
            "detached": False,
            "fn_name": fn_name,
            "module": module_path,
            "module_source": module_source,
            "captured_globals": captured_globals,
            "runtime_env": merged_env,
            "node_id": validated_node_id,
            "num_cpus": resolved_cpus,
            "num_gpus": resolved_gpus,
            "memory": resolved_memory,
            "resources": resolved_resources,
            "label_selector": validated_selector,
            "max_concurrency": 1,
            "ttl_seconds": 300.0,
            "get_if_exists": False,
        }
        spec = {k: v for k, v in spec.items() if v is not None}
        return spec, bound_args, bound_kwargs

    def _deploy_actor_spec(
        self,
        spec: Dict[str, Any],
        target_label: str,
        *,
        timeout: Optional[float] = None,
        on_unreaped_error: Optional[Callable[[], None]] = None,
    ) -> Dict[str, Any]:
        payload = dict(spec)
        if timeout is not None:
            payload["timeout"] = float(timeout)
        http_timeout: Any = (
            (min(self._timeout, float(timeout) + 5.0), float(timeout) + 5.0)
            if timeout is not None
            else (self._timeout, None)
        )
        sub_id = spec.get("name", target_label)
        timeout_msg = f"Actor '{target_label}' deployment timed out after {timeout}s."
        reaped_by_server = False
        resp: Optional["requests.Response"] = None
        try:
            try:
                resp = self.request(
                    "POST",
                    "/api/actors",
                    json_data=payload,
                    idempotent=False,
                    timeout=http_timeout,
                    stream=True,
                )
            except RayClientError as exc:
                if timeout is not None and _is_timeout_exception(exc):
                    raise RayJobTimeoutError(
                        timeout_msg,
                        submission_id=sub_id,
                    ) from exc
                raise
            if resp.status_code not in (200, 400):
                raise self._http_error("POST", "/api/actors", resp)
            data = self._read_json_stream_response(
                resp,
                endpoint="/api/actors",
                timeout=timeout,
                timeout_message=timeout_msg,
                submission_id=sub_id,
            )
            reaped_by_server = isinstance(data, dict)
            if not data.get("ok", True):
                self._raise_remote_error(
                    target_label,
                    data.get("error") or {},
                    logs=str(data.get("logs") or ""),
                )
            return data
        except BaseException:
            if resp is not None:
                resp.close()
            if on_unreaped_error is not None and not reaped_by_server:
                on_unreaped_error()
            raise
        finally:
            if resp is not None:
                resp.close()

    def _safe_kill_actor(self, actor_name: str) -> None:
        try:
            self.kill_actor(actor_name)
        except Exception:
            pass

    # 1. One-shot Execution (Functions & CLI Jobs)
    def run(
        self,
        fn: Union[Callable[..., Any], str],
        *args: Any,
        kwargs: Optional[Dict[str, Any]] = None,
        runtime_env: Optional[Dict[str, Any]] = None,
        node_id: Optional[str] = None,
        num_cpus: Optional[Union[int, float]] = None,
        num_gpus: Optional[Union[int, float]] = None,
        resources: Optional[Dict[str, float]] = None,
        label_selector: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
    ) -> Any:
        """Packages `fn` and runs it in a one-shot root Ray Actor on `/api/actors`.

        Args:
            fn: Python function or `"module:function"` import path to execute.
            *args: Positional arguments serialized as round-trip JSON.
            kwargs: Optional dictionary of keyword arguments serialized as JSON.
            runtime_env: Optional runtime environment overrides (e.g. `working_dir`,
                `env_vars`, `pip`).
            node_id: Optional Ray node ID to pin the driver actor to. Cannot be
                combined with `label_selector`.
            num_cpus: Optional CPU requirement for the root driver actor (defaults to 0).
            num_gpus: Optional GPU requirement for the root driver actor.
            resources: Optional Ray 3.0 unified resource map (e.g. `{"CPU": 1, "GPU": 1}`).
            label_selector: Optional node label selector dictionary. Cannot be combined
                with `node_id`.
            timeout: Optional timeout in seconds covering actor creation and execution.

        Returns:
            The decoded JSON return value for a synchronous or `async def` function, or
            an SSE iterator of yielded JSON items if `fn` is a generator or async
            generator function.

        Raises:
            RayClientSerializationError: If `fn`, `args`, `kwargs`, or referenced globals
                cannot be serialized as round-trip JSON.
            ValueError: If resource options or `node_id`/`label_selector` conflict.
            RayJobExecutionError: If `fn` raises an unhandled exception or returns a
                non-JSON result.
            RayJobTimeoutError: If execution exceeds `timeout`.
            RayClientError: If the HTTP request fails.
        """
        timeout = _validate_timeout(timeout, "timeout")
        spec, bound_args, bound_kwargs = self._build_driver_actor_spec(
            fn,
            args,
            kwargs,
            runtime_env,
            num_cpus,
            num_gpus,
            resources,
            node_id=node_id,
            label_selector=label_selector,
            allow_generator=True,
        )
        actor_name = spec["name"]
        fn_name = spec["fn_name"]
        t_start = time.monotonic()
        self._deploy_actor_spec(
            spec,
            fn_name,
            timeout=timeout,
            on_unreaped_error=lambda: self._safe_kill_actor(actor_name),
        )
        call_timeout: Optional[float] = None
        if timeout is not None:
            call_timeout = float(timeout) - (time.monotonic() - t_start)
            if call_timeout <= 0:
                self._safe_kill_actor(actor_name)
                raise RayJobTimeoutError(
                    f"Function '{fn_name}' timed out after {timeout}s.",
                    submission_id=actor_name,
                )
        call_body: Dict[str, Any] = {
            "args": bound_args,
            "kwargs": bound_kwargs,
        }
        q_actor_name = urllib.parse.quote(actor_name, safe="")
        endpoint = f"/api/actors/{q_actor_name}/methods/__call__"
        return self._call_or_stream_actor_method(
            endpoint,
            call_body,
            target=fn_name,
            submission_id=actor_name,
            timeout=call_timeout,
            on_close=lambda: self._safe_kill_actor(actor_name),
        )

    def submit_job(
        self,
        entrypoint: str,
        *,
        runtime_env: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, str]] = None,
        submission_id: Optional[str] = None,
        entrypoint_num_cpus: Optional[Union[int, float]] = None,
        entrypoint_num_gpus: Optional[Union[int, float]] = None,
        entrypoint_memory: Optional[int] = None,
        entrypoint_resources: Optional[Dict[str, float]] = None,
        entrypoint_label_selector: Optional[Dict[str, str]] = None,
        wait: bool = False,
        timeout: Optional[float] = None,
    ) -> JobResult:
        """Submits an entrypoint CLI script to `/api/jobs/` and returns a `JobResult`.

        Args:
            entrypoint: Non-empty shell command string to execute on the cluster.
            runtime_env: Optional runtime environment dictionary.
            metadata: Optional `dict[str, str]` job metadata tags.
            submission_id: Optional explicit job submission ID.
            entrypoint_num_cpus: Optional CPUs reserved for the entrypoint process.
            entrypoint_num_gpus: Optional GPUs reserved for the entrypoint process.
            entrypoint_memory: Optional memory in bytes for the entrypoint process.
            entrypoint_resources: Optional resource dictionary for the entrypoint.
            entrypoint_label_selector: Optional node label selector for the entrypoint.
            wait: If `True`, blocks until the job reaches a terminal state and raises
                `RayJobExecutionError` if it does not succeed.
            timeout: Optional timeout in seconds when `wait=True`.

        Returns:
            A `JobResult` handle for inspecting status, logs, or stopping the job.

        Raises:
            ValueError: If `entrypoint` is empty or resource values are invalid.
            TypeError: If `metadata` or `runtime_env` entries have invalid types.
            RayJobExecutionError: If `wait=True` and the job fails or is stopped.
            RayJobTimeoutError: If `wait=True` and the job does not finish within `timeout`.
            RayClientError: If the HTTP submission request fails.
        """
        timeout = _validate_timeout(timeout, "timeout")
        if not isinstance(entrypoint, str) or not entrypoint.strip():
            raise ValueError(
                f"entrypoint must be a non-empty command string, got {entrypoint!r}."
            )
        if submission_id is not None and (
            not isinstance(submission_id, str) or not submission_id.strip()
        ):
            raise ValueError("submission_id must be a non-empty string.")
        if metadata is not None:
            if not isinstance(metadata, dict) or not all(
                isinstance(k, str) and isinstance(v, str) for k, v in metadata.items()
            ):
                raise TypeError(f"metadata must be a dict[str, str], got {metadata!r}.")
        entrypoint_label_selector = _validate_label_selector(
            entrypoint_label_selector, "entrypoint_label_selector"
        )

        (
            entrypoint_num_cpus,
            entrypoint_num_gpus,
            entrypoint_memory,
            entrypoint_resources,
        ) = _normalize_resources(
            entrypoint_num_cpus,
            entrypoint_num_gpus,
            entrypoint_memory,
            entrypoint_resources,
        )
        merged_env, uploaded = self._prepare_and_upload_runtime_env(runtime_env)

        sub_id = submission_id or f"raysubmit_{uuid.uuid4().hex[:16]}"
        body: Dict[str, Any] = {
            "entrypoint": entrypoint,
            "submission_id": sub_id,
            "runtime_env": merged_env,
            "metadata": dict(metadata) if metadata else None,
            "entrypoint_num_cpus": entrypoint_num_cpus,
            "entrypoint_num_gpus": entrypoint_num_gpus,
            "entrypoint_memory": entrypoint_memory,
            "entrypoint_resources": entrypoint_resources,
            "entrypoint_label_selector": entrypoint_label_selector,
        }
        body = {k: v for k, v in body.items() if v is not None}

        actual_id = self._submit_job_with_recovery(sub_id, body)
        job = JobResult(
            submission_id=actual_id,
            cluster=self,
            package_uploaded=uploaded,
        )
        if wait:
            st = job.wait(timeout=timeout)
            if st != "SUCCEEDED":
                info = job.info()
                logs = job.logs()
                msg = (
                    info.get("message")
                    or f"Job '{actual_id}' finished with status {st}"
                )
                raise RayJobExecutionError(
                    submission_id=actual_id,
                    status=st,
                    message=str(msg),
                    logs=logs,
                    job=job,
                )
        return job

    def _submit_job_with_recovery(
        self, submission_id: str, body: Dict[str, Any]
    ) -> str:
        delay = 0.2
        last_exc: Optional[BaseException] = None
        for attempt in range(_MAX_RETRIES):
            try:
                resp = self.request(
                    "POST",
                    "/api/jobs/",
                    json_data=body,
                    idempotent=False,
                )
            except RayClientError as exc:
                last_exc = exc
                resp = None

            if resp is not None:
                if resp.status_code == 200:
                    data = resp.json()
                    return str(
                        data.get("submission_id") or data.get("job_id") or submission_id
                    )
                if resp.status_code == 400:
                    raise self._http_error("POST", "/api/jobs/", resp)
                if resp.status_code not in _RETRYABLE_HTTP_STATUS:
                    raise self._http_error("POST", "/api/jobs/", resp)
                last_exc = self._http_error("POST", "/api/jobs/", resp)

            # Check if the job was already registered before retrying POST /api/jobs/.
            q_id = urllib.parse.quote(submission_id, safe="")
            check = self.request("GET", f"/api/jobs/{q_id}")
            if check.status_code == 200:
                return submission_id
            if attempt + 1 < _MAX_RETRIES:
                time.sleep(delay)
                delay = min(delay * 2, 3.0)
        assert last_exc is not None
        raise last_exc

    def get_job(self, submission_id: str) -> JobResult:
        """Returns a `JobResult` handle for an existing job `submission_id`.

        Args:
            submission_id: Existing job submission ID on `/api/jobs/`.

        Returns:
            A `JobResult` bound to `submission_id`.

        Raises:
            RayClientError: If the job does not exist or the HTTP request fails.
        """
        if not isinstance(submission_id, str) or not submission_id.strip():
            raise ValueError("submission_id must be a non-empty string.")
        q_id = urllib.parse.quote(submission_id, safe="")
        info = self._json("GET", f"/api/jobs/{q_id}")
        return JobResult(
            submission_id=submission_id,
            cluster=self,
            initial_info=info if isinstance(info, dict) else None,
        )

    def list_jobs(self) -> List[Dict[str, Any]]:
        """Lists jobs recorded on `/api/jobs/`.

        Returns:
            List of job metadata dictionaries.

        Raises:
            RayClientError: If the HTTP request fails.
        """
        return list(self._json("GET", "/api/jobs/"))

    # 2. Stateful Actors
    def actor(
        self,
        cls_or_path: Union[type, str],
        *args: Any,
        kwargs: Optional[Dict[str, Any]] = None,
        name: Optional[str] = None,
        runtime_env: Optional[Dict[str, Any]] = None,
        node_id: Optional[str] = None,
        num_cpus: Optional[Union[int, float]] = None,
        num_gpus: Optional[Union[int, float]] = None,
        resources: Optional[Dict[str, float]] = None,
        label_selector: Optional[Dict[str, str]] = None,
        max_concurrency: Optional[int] = None,
        ttl_seconds: Optional[float] = _UNSET,
        get_if_exists: Union[bool, str] = True,
        detached: bool = True,
        timeout: Optional[float] = None,
    ) -> ActorHandle:
        """Deploys or reattaches to a root Ray Actor on `/api/actors`.

        Args:
            cls_or_path: Python class (plain or `@ray.remote` decorated) or
                `"module:ClassName"` import path.
            *args: Positional constructor arguments serialized as round-trip JSON.
            kwargs: Optional dictionary of constructor keyword arguments.
            name: Optional actor name for reattachment across sessions.
            runtime_env: Optional runtime environment overrides.
            node_id: Optional Ray node ID to pin the actor to. Cannot be combined with
                `label_selector`.
            num_cpus: Optional CPU requirement (defaults to `@ray.remote` option or 1).
            num_gpus: Optional GPU requirement.
            resources: Optional Ray 3.0 unified resource map (e.g. `{"CPU": 2, "GPU": 1}`).
            label_selector: Optional node label selector dictionary. Cannot be combined
                with `node_id`.
            max_concurrency: Optional maximum concurrent calls on the actor (defaults
                to `@ray.remote` option or 1).
            ttl_seconds: Optional idle TTL in seconds before the actor is automatically
                reaped (`None` disables idle reaping; omitting preserves an existing
                actor's TTL on reattachment).
            get_if_exists: Reattachment behavior when `name` already exists: `True`
                reattaches (emitting a `UserWarning` if the spec changed), `False` raises
                an error, and `"recreate"` replaces the existing actor if its spec changed.
            detached: Whether the actor is detached (`True`, default) or fate-shared with
                the dashboard agent (`False`).
            timeout: Optional timeout in seconds for actor creation and `__init__`.

        Returns:
            An `ActorHandle` connected to the remote actor.

        Raises:
            RayClientSerializationError: If constructor arguments or referenced globals
                are not round-trip JSON.
            ValueError: If options (`ttl_seconds`, `get_if_exists`, `node_id`,
                `label_selector`, `max_concurrency`, `timeout`, or resources) are invalid
                or conflict.
            RayJobExecutionError: If actor creation or `__init__` fails on the cluster.
            RayJobTimeoutError: If actor creation exceeds `timeout`.
            RayClientError: If the HTTP request fails.
        """
        timeout = _validate_timeout(timeout, "timeout")
        if name is not None and (not isinstance(name, str) or not name.strip()):
            raise ValueError("Actor name must be a non-empty string.")
        has_explicit_ttl = ttl_seconds is not _UNSET
        validated_ttl = _validate_ttl_seconds(ttl_seconds) if has_explicit_ttl else None
        validated_get_if_exists = _validate_get_if_exists(get_if_exists)
        if not isinstance(detached, bool):
            raise ValueError(f"detached must be a bool, got {detached!r}.")
        node_id, label_selector = _validate_node_id_and_label_selector(
            node_id, label_selector
        )
        raw_resources = resources
        num_cpus, num_gpus, memory, resources = _normalize_resources(
            num_cpus, num_gpus, None, resources
        )
        default_opts = (
            getattr(cls_or_path, "_default_options", None)
            if not isinstance(cls_or_path, str)
            else None
        )
        if isinstance(default_opts, dict):
            (def_cpus, def_gpus, def_memory, def_resources,) = _normalize_resources(
                default_opts.get("num_cpus"),
                default_opts.get("num_gpus"),
                default_opts.get("memory"),
                default_opts.get("resources"),
            )
            if num_cpus is None and def_cpus is not None:
                num_cpus = def_cpus
            if num_gpus is None and def_gpus is not None:
                num_gpus = def_gpus
            if memory is None and def_memory is not None:
                memory = def_memory
            has_custom_override = isinstance(raw_resources, dict) and (
                len(raw_resources) == 0
                or any(k not in ("CPU", "GPU", "memory") for k in raw_resources)
            )
            if not has_custom_override and def_resources is not None:
                resources = dict(def_resources)
            if (
                node_id is None
                and label_selector is None
                and default_opts.get("node_id") is not None
            ):
                node_id, _ = _validate_node_id_and_label_selector(
                    default_opts["node_id"], None
                )
            if (
                node_id is None
                and label_selector is None
                and default_opts.get("label_selector") is not None
            ):
                _, label_selector = _validate_node_id_and_label_selector(
                    None, default_opts["label_selector"]
                )
            if (
                max_concurrency is None
                and default_opts.get("max_concurrency") is not None
            ):
                max_concurrency = _validate_max_concurrency(
                    default_opts["max_concurrency"]
                )
        resolved_cpus: Union[int, float] = 1 if num_cpus is None else num_cpus
        resolved_concurrency: int = (
            1 if max_concurrency is None else _validate_max_concurrency(max_concurrency)
        )

        if isinstance(cls_or_path, str):
            module_path, class_name = _parse_entrypoint_str(cls_or_path, "Actor")
            module_source = None
            captured_globals: Optional[Dict[str, Any]] = None
            bound_args = list(args)
            bound_kwargs = dict(kwargs or {})
        else:
            module_path = None
            raw_cls = unwrap_actor_class(cls_or_path)
            init_fn = inspect.unwrap(getattr(raw_cls, "__init__", None))
            if inspect.isfunction(init_fn):
                sig = inspect.signature(init_fn)
                params = [
                    p
                    for p in list(sig.parameters.values())[1:]
                    if p.name != "_ray_trace_ctx"
                ]
                sig.replace(parameters=params).bind(*args, **(kwargs or {}))
                bound_args = list(args)
                bound_kwargs = dict(kwargs or {})
            else:
                bound_args = list(args)
                bound_kwargs = dict(kwargs or {})
            (
                class_name,
                module_source,
                captured_globals,
            ) = extract_class_source_and_globals(cls_or_path)

        to_json(bound_args, f"Actor '{class_name}' positional arguments")
        to_json(bound_kwargs, f"Actor '{class_name}' keyword arguments")
        if captured_globals is not None:
            to_json(captured_globals, f"Actor '{class_name}' captured globals")

        def_runtime_env = (
            default_opts.get("runtime_env") if isinstance(default_opts, dict) else None
        )
        merged_env, uploaded = self._prepare_and_upload_runtime_env(
            runtime_env, base_runtime_env=def_runtime_env
        )

        actor_name = name or f"{class_name}_{uuid.uuid4().hex[:8]}"
        spec: Dict[str, Any] = {
            "name": actor_name,
            "kind": "class",
            "detached": detached,
            "class_name": class_name,
            "module": module_path,
            "module_source": module_source,
            "captured_globals": captured_globals,
            "args": bound_args,
            "kwargs": bound_kwargs,
            "runtime_env": merged_env,
            "node_id": node_id,
            "num_cpus": resolved_cpus,
            "num_gpus": num_gpus,
            "memory": memory,
            "resources": resources,
            "label_selector": label_selector,
            "max_concurrency": resolved_concurrency,
            "ttl_seconds": validated_ttl,
            "get_if_exists": validated_get_if_exists,
        }
        spec = {
            k: v
            for k, v in spec.items()
            if v is not None or (k == "ttl_seconds" and has_explicit_ttl)
        }
        data = self._deploy_actor_spec(spec, actor_name, timeout=timeout)
        if data.get("spec_mismatch"):
            warnings.warn(
                f"Reattached to existing actor '{data['name']}', but its source, "
                "arguments, or resource configuration differ from the requested spec "
                "(the existing actor was not updated). Pass get_if_exists='recreate' "
                "to automatically replace stale actors, get_if_exists=False to require "
                f"a new actor, or call cluster.kill_actor({data['name']!r}) first.",
                UserWarning,
                stacklevel=2,
            )
        return ActorHandle(
            name=data["name"],
            cluster=self,
            created=bool(data.get("created", True)),
            package_uploaded=uploaded,
        )

    def get_actor(self, name: str) -> ActorHandle:
        """Returns an `ActorHandle` for an existing actor on the cluster.

        Args:
            name: Name of the existing actor on `/api/actors`.

        Returns:
            An `ActorHandle` with `created=False`.

        Raises:
            RayClientError: If the actor does not exist or the HTTP request fails.
        """
        if not isinstance(name, str) or not name.strip():
            raise ValueError("Actor name must be a non-empty string.")
        q_name = urllib.parse.quote(name, safe="")
        self._json("GET", f"/api/actors/{q_name}")
        return ActorHandle(name=name, cluster=self, created=False)

    def list_actors(self) -> List[Dict[str, Any]]:
        """Lists active client-managed actors on `/api/actors`.

        Returns:
            List of actor metadata dictionaries.

        Raises:
            RayClientError: If the HTTP request fails.
        """
        return list(self._json("GET", "/api/actors"))

    def kill_actor(self, name: str) -> bool:
        """Kills a named root actor and all child actors/objects it owns.

        Args:
            name: Name of the actor to terminate.

        Returns:
            `True` if the actor existed and was terminated, `False` otherwise.

        Raises:
            RayClientError: If the HTTP request fails with a non-404 status.
        """
        return ActorHandle(name=name, cluster=self).kill()

    # 3. Cluster Node Management
    def list_node_groups(self) -> List[Dict[str, Any]]:
        """Lists all node groups declared on the cluster (`GET /api/cluster/node_groups`).

        Returns:
            List of node group dictionaries (including the `"head"` group).

        Raises:
            RayClientError: If the HTTP request fails.
        """
        return list(self._json("GET", "/api/cluster/node_groups"))

    def get_node_group(self, group_name: str) -> Dict[str, Any]:
        """Inspects a node group by name (`GET /api/cluster/node_groups/{group_name}`).

        Args:
            group_name: Non-empty node group name (e.g. `"head"`, `"default"`, `"gb300"`).

        Returns:
            Dictionary describing the node group's `name`, `resources`, `labels`, and
            `max_nodes`.

        Raises:
            ValueError: If `group_name` is not a non-empty string.
            RayClientError: If the node group does not exist or the HTTP request fails.
        """
        if not isinstance(group_name, str) or not group_name.strip():
            raise ValueError("group_name must be a non-empty string.")
        q_group = urllib.parse.quote(group_name.strip(), safe="")
        return dict(self._json("GET", f"/api/cluster/node_groups/{q_group}"))

    def add_nodes(
        self,
        group_name: str,
        count: int = 1,
        *,
        wait_ready: bool = False,
        timeout: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        """Adds detached nodes to a cluster node group (`POST /api/cluster/node_groups/{group}/nodes`).

        Args:
            group_name: Non-empty target node group name.
            count: Number of nodes to add (must be an integer >= 1).
            wait_ready: If `True`, waits on the server until the new nodes transition to
                `ALIVE`.
            timeout: Optional timeout in seconds when waiting for nodes to become ready.

        Returns:
            List of node dictionaries for the newly requested nodes.

        Raises:
            ValueError: If `group_name`, `count`, `wait_ready`, or `timeout` is invalid.
            RayClientError: If the node group does not exist, exceeds `max_nodes`, or
                the HTTP request fails.
        """
        if not isinstance(group_name, str) or not group_name.strip():
            raise ValueError("group_name must be a non-empty string.")
        if isinstance(count, bool) or not isinstance(count, int) or count < 1:
            raise ValueError(f"count must be a positive integer, got {count!r}.")
        if not isinstance(wait_ready, bool):
            raise ValueError(f"wait_ready must be a bool, got {wait_ready!r}.")
        eff_timeout = _validate_timeout(timeout, "timeout")
        body: Dict[str, Any] = {
            "count": count,
            "wait_ready": wait_ready,
        }
        if eff_timeout is not None:
            body["timeout_s"] = float(eff_timeout)
        req_timeout: Any = (
            (min(self._timeout, float(eff_timeout) + 5.0), float(eff_timeout) + 5.0)
            if eff_timeout is not None
            else ((self._timeout, None) if wait_ready else None)
        )
        q_group = urllib.parse.quote(group_name.strip(), safe="")
        res = self._json(
            "POST",
            f"/api/cluster/node_groups/{q_group}/nodes",
            json_data=body,
            idempotent=False,
            timeout=req_timeout,
        )
        return list(res)

    def list_nodes(
        self,
        *,
        group_name: Optional[str] = None,
        states: Optional[Union[str, Sequence[str]]] = None,
        include_dead: bool = False,
    ) -> List[Dict[str, Any]]:
        """Lists cluster nodes (`GET /api/cluster/nodes`).

        Args:
            group_name: Optional node group name filter.
            states: Optional state filter string (e.g. `"ALIVE"`) or sequence of state
                strings (e.g. `["PENDING", "ALIVE"]`).
            include_dead: Whether to include `DEAD` nodes when `states` is not specified.

        Returns:
            List of node metadata dictionaries.

        Raises:
            ValueError: If `group_name`, `states`, or `include_dead` is invalid.
            RayClientError: If the HTTP request fails.
        """
        if not isinstance(include_dead, bool):
            raise ValueError(f"include_dead must be a bool, got {include_dead!r}.")
        params: List[Tuple[str, str]] = []
        if states is not None:
            if isinstance(states, str):
                parts = [s.strip().upper() for s in states.split(",") if s.strip()]
                if not parts:
                    raise ValueError("states must not be an empty string.")
            elif isinstance(states, Sequence):
                if not all(isinstance(s, str) for s in states):
                    raise TypeError(
                        f"states must be a string or sequence of strings, got {states!r}."
                    )
                parts = [s.strip().upper() for s in states if s.strip()]
                if not parts:
                    raise ValueError("states sequence must not be empty.")
            else:
                raise TypeError(
                    f"states must be a string or sequence of strings, got {states!r}."
                )
            params.append(("state", ",".join(parts)))
        if group_name is not None:
            if not isinstance(group_name, str) or not group_name.strip():
                raise ValueError("group_name must be a non-empty string.")
            params.append(("node_group", group_name.strip()))
        if include_dead:
            params.append(("include_dead", "true"))
        endpoint = "/api/cluster/nodes"
        if params:
            endpoint = f"{endpoint}?{urllib.parse.urlencode(params)}"
        return list(self._json("GET", endpoint))

    def get_node(self, node_id: str) -> Dict[str, Any]:
        """Inspects a specific cluster node (`GET /api/cluster/nodes/{node_id}`).

        Args:
            node_id: Non-empty Ray node ID.

        Returns:
            Dictionary describing the node's `id`, `group_name`, `state`, `resources`,
            and `labels`.

        Raises:
            ValueError: If `node_id` is not a non-empty string.
            RayClientError: If the node does not exist or the HTTP request fails.
        """
        if not isinstance(node_id, str) or not node_id.strip():
            raise ValueError("node_id must be a non-empty string.")
        q_node = urllib.parse.quote(node_id.strip(), safe="")
        return dict(self._json("GET", f"/api/cluster/nodes/{q_node}"))

    def destroy_node(
        self,
        node_id: str,
        reason: str,
        *,
        grace_period_s: float = 0.0,
    ) -> Dict[str, Any]:
        """Destroys a cluster node (`DELETE /api/cluster/nodes/{node_id}`).

        Args:
            node_id: Non-empty Ray node ID to destroy.
            reason: Non-empty human-readable reason for destroying the node.
            grace_period_s: Non-negative drain grace period in seconds before the node
                transitions to `DEAD` (defaults to `0.0`).

        Returns:
            Dictionary describing the updated node state.

        Raises:
            ValueError: If `node_id`, `reason`, or `grace_period_s` is invalid.
            RayClientError: If the node does not exist, cannot be destroyed, or the
                HTTP request fails.
        """
        if not isinstance(node_id, str) or not node_id.strip():
            raise ValueError("node_id must be a non-empty string.")
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("reason must be a non-empty string.")
        if (
            isinstance(grace_period_s, bool)
            or not isinstance(grace_period_s, (int, float))
            or not math.isfinite(grace_period_s)
            or grace_period_s < 0
        ):
            raise ValueError(
                f"grace_period_s must be a non-negative finite number, got {grace_period_s!r}."
            )
        body: Dict[str, Any] = {
            "reason": reason.strip(),
            "grace_period_s": float(grace_period_s),
        }
        q_node = urllib.parse.quote(node_id.strip(), safe="")
        return self._json(
            "DELETE",
            f"/api/cluster/nodes/{q_node}",
            json_data=body,
            idempotent=False,
        )


def run(
    fn: Union[Callable[..., Any], str],
    *args: Any,
    kwargs: Optional[Dict[str, Any]] = None,
    runtime_env: Optional[Dict[str, Any]] = None,
    node_id: Optional[str] = None,
    num_cpus: Optional[Union[int, float]] = None,
    num_gpus: Optional[Union[int, float]] = None,
    resources: Optional[Dict[str, float]] = None,
    label_selector: Optional[Dict[str, str]] = None,
    timeout: Optional[float] = None,
    address: Optional[str] = None,
    headers: Optional[Dict[str, str]] = None,
    verify: Union[bool, str] = True,
    default_runtime_env: Optional[Dict[str, Any]] = None,
    request_timeout: float = 30.0,
) -> Any:
    """Runs `fn(*args)` as a one-shot driver function on the Ray cluster at `address`.

    Args:
        fn: Python function or `"module:function"` import path to execute.
        *args: Positional arguments serialized as round-trip JSON.
        kwargs: Optional dictionary of keyword arguments serialized as JSON.
        runtime_env: Optional runtime environment overrides.
        node_id: Optional Ray node ID to pin the driver actor to.
        num_cpus: Optional CPU requirement for the root driver actor (defaults to 0).
        num_gpus: Optional GPU requirement for the root driver actor.
        resources: Optional Ray 3.0 unified resource map (e.g. `{"CPU": 1}`).
        label_selector: Optional node label selector dictionary.
        timeout: Optional timeout in seconds covering actor creation and execution.
        address: Ray Dashboard HTTP address (defaults to `RAY_API_SERVER_ADDRESS`,
            `RAY_ADDRESS`, or `"http://127.0.0.1:8265"`).
        headers: Optional HTTP headers for cluster requests.
        verify: TLS certificate verification boolean or CA bundle path.
        default_runtime_env: Default runtime environment merged with `runtime_env`.
        request_timeout: Default HTTP timeout in seconds for control operations.

    Returns:
        The decoded JSON return value for a regular or async function, or an SSE
        iterator of yielded JSON items if `fn` is a generator or async generator.

    Raises:
        RayClientSerializationError: If `fn`, `args`, `kwargs`, or referenced globals
            are not round-trip JSON.
        ValueError: If resource options or `node_id`/`label_selector` conflict.
        RayJobExecutionError: If `fn` raises an unhandled exception on the cluster.
        RayJobTimeoutError: If execution exceeds `timeout`.
        RayClientError: If the HTTP request fails.
    """
    cluster = RayCluster(
        address=address,
        headers=headers,
        verify=verify,
        default_runtime_env=default_runtime_env,
        timeout=request_timeout,
    )
    try:
        res = cluster.run(
            fn,
            *args,
            kwargs=kwargs,
            runtime_env=runtime_env,
            node_id=node_id,
            num_cpus=num_cpus,
            num_gpus=num_gpus,
            resources=resources,
            label_selector=label_selector,
            timeout=timeout,
        )
    except BaseException:
        cluster.close()
        raise
    if isinstance(res, _SSEStreamIterator):
        res._add_close_callback(cluster.close)
        return res
    cluster.close()
    return res
