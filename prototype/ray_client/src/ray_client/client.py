"""RayCluster client, JobResult handle, and top-level run helper."""

import copy
import inspect
import json
import os
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Union

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
_AUTH_HINT = (
    " The cluster requires token authentication: set RAY_AUTH_TOKEN or "
    "RAY_AUTH_TOKEN_PATH, or pass headers={'Authorization': 'Bearer <token>'}."
)


def _resolve_address(address: Optional[str]) -> str:
    explicit = address if address and address != "auto" else None
    api_env = os.environ.get("RAY_API_SERVER_ADDRESS")
    api_env = api_env if api_env and api_env != "auto" else None
    ray_env = os.environ.get("RAY_ADDRESS")
    ray_env = ray_env if ray_env and ray_env != "auto" else None

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
    return raw.rstrip("/")


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


class JobResult:
    """Handle to a submitted CLI job on `/api/jobs/`."""

    def __init__(
        self,
        submission_id: str,
        cluster: "RayCluster",
        *,
        package_uploaded: bool = False,
        initial_info: Optional[Dict[str, Any]] = None,
    ) -> None:
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

    def __repr__(self) -> str:
        return (
            f"JobResult(submission_id={self._submission_id!r}, "
            f"address={self._cluster.address!r})"
        )

    @property
    def submission_id(self) -> str:
        return self._submission_id

    @property
    def package_uploaded(self) -> bool:
        return self._package_uploaded

    def info(self) -> Dict[str, Any]:
        if self._terminal_info is not None:
            return dict(self._terminal_info)
        data = self._cluster._json("GET", f"/api/jobs/{self._submission_id}")
        if isinstance(data, dict) and data.get("status") in _TERMINAL_JOB_STATUSES:
            self._terminal_info = dict(data)
        return data

    def status(self) -> str:
        return str(self.info().get("status", "UNKNOWN"))

    def done(self) -> bool:
        return self.status() in _TERMINAL_JOB_STATUSES

    def logs(self) -> str:
        if self._terminal_logs is not None:
            return self._terminal_logs
        is_terminal = self._terminal_info is not None
        data = self._cluster._json("GET", f"/api/jobs/{self._submission_id}/logs")
        text = str(data.get("logs", "")) if isinstance(data, dict) else str(data)
        if is_terminal:
            self._terminal_logs = text
        return text

    def wait(
        self,
        timeout: Optional[float] = None,
        on_log_line: Optional[Callable[[str], None]] = None,
    ) -> str:
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
        resp = self._cluster.request(
            "POST",
            f"/api/jobs/{self._submission_id}/stop",
            idempotent=False,
        )
        if resp.status_code == 404:
            return False
        if resp.status_code != 200:
            raise self._cluster._http_error(
                "POST", f"/api/jobs/{self._submission_id}/stop", resp
            )
        self._terminal_info = None
        return bool(resp.json().get("stopped", False))

    def delete(self) -> bool:
        resp = self._cluster.request("DELETE", f"/api/jobs/{self._submission_id}")
        if resp.status_code == 404:
            return False
        if resp.status_code != 200:
            raise self._cluster._http_error(
                "DELETE", f"/api/jobs/{self._submission_id}", resp
            )
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
    """Unified HTTP client for submitting CLI jobs, driver functions, and stateful actors."""

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
        self._address = _resolve_address(address)
        self._headers = dict(headers or {})
        token = _load_auth_token()
        if token and not any(k.lower() == "authorization" for k in self._headers):
            self._headers["Authorization"] = f"Bearer {token}"
        self._verify = verify
        self._default_runtime_env = copy.deepcopy(default_runtime_env) or {}
        self._timeout = timeout
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
        """Sends an HTTP request to the Ray Dashboard server."""
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

        def _run_on_close() -> None:
            nonlocal close_called
            if not close_called:
                close_called = True
                if on_close is not None:
                    on_close()

        def _gen() -> Iterator[Any]:
            try:
                try:
                    for raw_line in resp.iter_lines(decode_unicode=True):
                        if deadline is not None and time.monotonic() >= deadline:
                            raise RayJobTimeoutError(
                                f"Stream '{target}' timed out after {timeout}s.",
                                submission_id=sub_id,
                            )
                        if not raw_line:
                            continue
                        line = (
                            raw_line.decode("utf-8")
                            if isinstance(raw_line, bytes)
                            else raw_line
                        ).strip()
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
                            break
                        if "item" in payload:
                            yield payload["item"]
                        elif "stream" in payload:
                            for item in payload["stream"]:
                                yield item
                except requests.exceptions.RequestException as exc:
                    if timeout is not None and (
                        "timed out" in str(exc).lower()
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
    ) -> Any:
        sub_id = submission_id or target
        req_body = dict(body)
        if timeout is not None:
            req_body["timeout"] = float(timeout)
        http_timeout: Any = (
            (self._timeout, max(float(timeout) + 5.0, self._timeout))
            if timeout is not None
            else (self._timeout, None)
        )
        kind_label = "Actor method" if is_actor_method else "Function"
        try:
            resp = self.request(
                "POST",
                endpoint,
                json_data=req_body,
                idempotent=False,
                timeout=http_timeout,
                stream=True,
            )
        except Exception as exc:
            if on_close is not None:
                on_close()
            if (
                isinstance(exc, RayClientError)
                and timeout is not None
                and "timed out" in str(exc).lower()
            ):
                raise RayJobTimeoutError(
                    f"{kind_label} '{target}' timed out after {timeout}s.",
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

        raw_text: Optional[str] = None
        try:
            try:
                if timeout is not None:
                    grace_deadline = time.monotonic() + float(timeout) + 5.0
                    chunks: List[bytes] = []
                    for chunk in resp.iter_content(chunk_size=65536):
                        if chunk:
                            chunks.append(chunk)
                        if time.monotonic() >= grace_deadline:
                            raise RayJobTimeoutError(
                                f"{kind_label} '{target}' timed out after {timeout}s.",
                                submission_id=sub_id,
                            )
                    raw_text = b"".join(chunks).decode("utf-8", errors="replace")
                    data = json.loads(raw_text)
                else:
                    data = resp.json()
            except ValueError as exc:
                preview = raw_text[:200] if raw_text is not None else resp.text[:200]
                raise RayClientError(
                    f"POST {endpoint} returned a non-JSON response from "
                    f"{self._address}; is this the Ray Dashboard HTTP address? Body starts "
                    f"with: {preview!r}"
                ) from exc
            except requests.exceptions.RequestException as exc:
                if timeout is not None and "timed out" in str(exc).lower():
                    raise RayJobTimeoutError(
                        f"{kind_label} '{target}' timed out after {timeout}s.",
                        submission_id=sub_id,
                    ) from exc
                raise RayClientError(f"POST {endpoint} failed: {exc}") from exc
            if not data.get("ok", True):
                self._raise_remote_error(
                    target,
                    data.get("error") or {},
                    logs=str(data.get("logs") or ""),
                )
            return data.get("result")
        finally:
            resp.close()
            if on_close is not None:
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
        self, runtime_env: Optional[Dict[str, Any]]
    ) -> Tuple[Dict[str, Any], Optional[Union[str, Path]], List[str]]:
        merged = copy.deepcopy(self._default_runtime_env)
        if merged.get("env_vars") is None and "env_vars" in merged:
            merged.pop("env_vars", None)
        excludes = _exclude_patterns(merged.pop("excludes", None))
        for key, value in (runtime_env or {}).items():
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
                    f"runtime_env[{conflict_key!r}] cannot be combined with "
                    "working_dir; configure the container image on the cluster."
                )

        working_dir = merged.pop("working_dir", None)
        if working_dir is not None and "://" in str(working_dir):
            raise RayClientError(
                f"runtime_env['working_dir']={working_dir!r} is a remote URI; "
                "place remote archives in runtime_env['py_modules'] instead."
            )
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

    def _build_driver_actor_spec(
        self,
        fn: Union[Callable[..., Any], str],
        args: Tuple[Any, ...] = (),
        kwargs: Optional[Dict[str, Any]] = None,
        runtime_env: Optional[Dict[str, Any]] = None,
        num_cpus: Union[int, float] = 0,
        num_gpus: Optional[Union[int, float]] = None,
        resources: Optional[Dict[str, float]] = None,
        *,
        allow_generator: bool = False,
    ) -> Tuple[Dict[str, Any], List[Any], Dict[str, Any]]:
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

        merged_env, working_dir, excludes = self._prepare_runtime_env(runtime_env)
        if working_dir is not None:
            zip_bytes, pkg_uri = build_working_dir_package(working_dir, excludes)
            self._ensure_package_uploaded(pkg_uri, zip_bytes)
            merged_env["working_dir"] = pkg_uri

        actor_name = f"driver_{fn_name}_{uuid.uuid4().hex[:8]}"
        spec: Dict[str, Any] = {
            "name": actor_name,
            "kind": "function",
            "ephemeral": True,
            "fn_name": fn_name,
            "module": module_path,
            "module_source": module_source,
            "captured_globals": captured_globals,
            "runtime_env": merged_env or None,
            "num_cpus": num_cpus,
            "num_gpus": num_gpus,
            "resources": resources,
            "max_concurrency": 10,
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
    ) -> Dict[str, Any]:
        http_timeout: Any = (
            (self._timeout, max(float(timeout) + 5.0, self._timeout))
            if timeout is not None
            else (self._timeout, None)
        )
        try:
            resp = self.request(
                "POST",
                "/api/actors",
                json_data=spec,
                idempotent=False,
                timeout=http_timeout,
            )
        except RayClientError as exc:
            if timeout is not None and "timed out" in str(exc).lower():
                raise RayJobTimeoutError(
                    f"Actor '{target_label}' deployment timed out after {timeout}s.",
                    submission_id=spec.get("name", target_label),
                ) from exc
            raise
        if resp.status_code not in (200, 400):
            raise self._http_error("POST", "/api/actors", resp)
        data = resp.json()
        if not data.get("ok", True):
            self._raise_remote_error(
                target_label,
                data.get("error") or {},
                logs=str(data.get("logs") or ""),
            )
        return data

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
        num_cpus: Union[int, float] = 0,
        num_gpus: Optional[Union[int, float]] = None,
        resources: Optional[Dict[str, float]] = None,
        timeout: Optional[float] = None,
    ) -> Any:
        """Packages `fn` and runs it in an ephemeral top-level Ray Actor.

        Returns the JSON result for a regular/async function, or an SSE iterator of
        yielded JSON items if `fn` is a generator or async generator function.
        """
        spec, bound_args, bound_kwargs = self._build_driver_actor_spec(
            fn,
            args,
            kwargs,
            runtime_env,
            num_cpus,
            num_gpus,
            resources,
            allow_generator=True,
        )
        actor_name = spec["name"]
        fn_name = spec["fn_name"]
        try:
            self._deploy_actor_spec(spec, fn_name, timeout=timeout)
        except Exception:
            self._safe_kill_actor(actor_name)
            raise
        call_body: Dict[str, Any] = {
            "args": bound_args,
            "kwargs": bound_kwargs,
        }
        endpoint = f"/api/actors/{actor_name}/methods/__call__"
        return self._call_or_stream_actor_method(
            endpoint,
            call_body,
            target=fn_name,
            submission_id=actor_name,
            timeout=timeout,
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
        """Submits an entrypoint CLI script to `/api/jobs/` and returns a `JobResult`."""
        if not isinstance(entrypoint, str) or not entrypoint.strip():
            raise ValueError(
                f"entrypoint must be a non-empty command string, got {entrypoint!r}."
            )
        if metadata is not None:
            if not isinstance(metadata, dict) or not all(
                isinstance(k, str) and isinstance(v, str) for k, v in metadata.items()
            ):
                raise TypeError(f"metadata must be a dict[str, str], got {metadata!r}.")

        merged_env, working_dir, excludes = self._prepare_runtime_env(runtime_env)
        uploaded = False
        if working_dir is not None:
            zip_bytes, pkg_uri = build_working_dir_package(working_dir, excludes)
            uploaded = self._ensure_package_uploaded(pkg_uri, zip_bytes)
            merged_env["working_dir"] = pkg_uri

        sub_id = submission_id or f"raysubmit_{uuid.uuid4().hex[:16]}"
        body: Dict[str, Any] = {
            "entrypoint": entrypoint,
            "submission_id": sub_id,
            "runtime_env": merged_env or None,
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
            check = self.request("GET", f"/api/jobs/{submission_id}")
            if check.status_code == 200:
                return submission_id
            if attempt + 1 < _MAX_RETRIES:
                time.sleep(delay)
                delay = min(delay * 2, 3.0)
        assert last_exc is not None
        raise last_exc

    def get_job(self, submission_id: str) -> JobResult:
        """Returns a `JobResult` handle for an existing job `submission_id`."""
        info = self._json("GET", f"/api/jobs/{submission_id}")
        return JobResult(
            submission_id=submission_id,
            cluster=self,
            initial_info=info if isinstance(info, dict) else None,
        )

    def list_jobs(self) -> List[Dict[str, Any]]:
        """Lists jobs recorded on `/api/jobs/`."""
        res = self._json("GET", "/api/jobs/")
        if isinstance(res, dict) and "jobs" in res:
            return res["jobs"]
        return list(res)

    # 2. Stateful Actors
    def actor(
        self,
        cls_or_path: Union[type, str],
        *args: Any,
        kwargs: Optional[Dict[str, Any]] = None,
        name: Optional[str] = None,
        runtime_env: Optional[Dict[str, Any]] = None,
        num_cpus: Optional[Union[int, float]] = None,
        num_gpus: Optional[Union[int, float]] = None,
        resources: Optional[Dict[str, float]] = None,
        max_concurrency: Optional[int] = None,
        ttl_seconds: Optional[float] = 1800.0,
        get_if_exists: bool = True,
        timeout: Optional[float] = None,
    ) -> ActorHandle:
        """Deploys or reattaches to a persistent top-level Ray Actor on `/api/actors`."""
        default_opts = (
            getattr(cls_or_path, "_default_options", None)
            if not isinstance(cls_or_path, str)
            else None
        )
        if isinstance(default_opts, dict):
            if num_cpus is None and default_opts.get("num_cpus") is not None:
                num_cpus = default_opts["num_cpus"]
            if num_gpus is None and default_opts.get("num_gpus") is not None:
                num_gpus = default_opts["num_gpus"]
            if resources is None and default_opts.get("resources") is not None:
                resources = dict(default_opts["resources"])
            if (
                max_concurrency is None
                and default_opts.get("max_concurrency") is not None
            ):
                max_concurrency = int(default_opts["max_concurrency"])
        resolved_cpus: Union[int, float] = 1 if num_cpus is None else num_cpus
        resolved_concurrency: int = 10 if max_concurrency is None else max_concurrency

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

        merged_env, working_dir, excludes = self._prepare_runtime_env(runtime_env)
        uploaded = False
        if working_dir is not None:
            zip_bytes, pkg_uri = build_working_dir_package(working_dir, excludes)
            uploaded = self._ensure_package_uploaded(pkg_uri, zip_bytes)
            merged_env["working_dir"] = pkg_uri

        actor_name = name or f"{class_name}_{uuid.uuid4().hex[:8]}"
        spec: Dict[str, Any] = {
            "name": actor_name,
            "kind": "class",
            "class_name": class_name,
            "module": module_path,
            "module_source": module_source,
            "captured_globals": captured_globals,
            "args": bound_args,
            "kwargs": bound_kwargs,
            "runtime_env": merged_env or None,
            "num_cpus": resolved_cpus,
            "num_gpus": num_gpus,
            "resources": resources,
            "max_concurrency": resolved_concurrency,
            "ttl_seconds": ttl_seconds,
            "get_if_exists": get_if_exists,
        }
        spec = {k: v for k, v in spec.items() if v is not None or k == "ttl_seconds"}
        data = self._deploy_actor_spec(spec, actor_name, timeout=timeout)
        return ActorHandle(
            name=data["name"],
            cluster=self,
            created=bool(data.get("created", True)),
            package_uploaded=uploaded,
        )

    def get_actor(self, name: str) -> ActorHandle:
        """Returns an `ActorHandle` for an existing actor on the cluster."""
        self._json("GET", f"/api/actors/{name}")
        return ActorHandle(name=name, cluster=self, created=False)

    def list_actors(self) -> List[Dict[str, Any]]:
        """Lists active client-managed actors on `/api/actors`."""
        res = self._json("GET", "/api/actors")
        if isinstance(res, dict) and "actors" in res:
            return res["actors"]
        return list(res)

    def kill_actor(self, name: str) -> bool:
        """Kills a named root actor and all child actors/objects it owns."""
        return ActorHandle(name=name, cluster=self).kill()

    # 3. Cluster Resource Management (planned KubeRay control-plane extension)
    def scale_worker_group(self, group: str, replicas: int) -> Dict[str, Any]:
        """Updates target replica count for a KubeRay worker group."""
        return self._json(
            "PATCH",
            f"/api/cluster/worker_groups/{group}",
            json_data={"replicas": int(replicas)},
            idempotent=False,
        )

    def add_worker_group(
        self,
        group: str,
        *,
        replicas: int = 1,
        num_cpus: Optional[Union[int, float]] = None,
        num_gpus: Optional[Union[int, float]] = None,
        resources: Optional[Dict[str, float]] = None,
        **extra_options: Any,
    ) -> Dict[str, Any]:
        """Adds a new worker group to a KubeRay-managed cluster."""
        payload: Dict[str, Any] = {
            "group": group,
            "replicas": int(replicas),
            "num_cpus": num_cpus,
            "num_gpus": num_gpus,
            "resources": resources,
            **extra_options,
        }
        payload = {k: v for k, v in payload.items() if v is not None}
        return self._json(
            "POST",
            "/api/cluster/worker_groups",
            json_data=payload,
            idempotent=False,
        )


RayClient = RayCluster


def run(
    fn: Union[Callable[..., Any], str],
    *args: Any,
    address: Optional[str] = None,
    headers: Optional[Dict[str, str]] = None,
    verify: Union[bool, str] = True,
    default_runtime_env: Optional[Dict[str, Any]] = None,
    request_timeout: float = 30.0,
    **options: Any,
) -> Any:
    """Runs `fn(*args)` as a driver function on the cluster at `address`."""
    cluster = RayCluster(
        address=address,
        headers=headers,
        verify=verify,
        default_runtime_env=default_runtime_env,
        timeout=request_timeout,
    )
    try:
        res = cluster.run(fn, *args, **options)
    except Exception:
        cluster.close()
        raise
    if isinstance(res, _SSEStreamIterator):
        res._add_close_callback(cluster.close)
        return res
    if isinstance(res, Iterator):

        def _gen() -> Iterator[Any]:
            try:
                yield from res
            finally:
                cluster.close()

        return _gen()
    cluster.close()
    return res
