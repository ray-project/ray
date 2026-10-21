"""REST-only RayClient, JobResult, and run_function_as_driver."""

import copy
import os
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

try:
    import requests
except ImportError:  # pragma: no cover - allows `import ray_client.packaging` without requests
    requests = None  # type: ignore[assignment]

from ray_client.exceptions import (
    RayClientError,
    RayJobExecutionError,
    RayJobTimeoutError,
)
from ray_client.packaging import (
    RESULT_LINE_PREFIX,
    build_driver_and_payload_packages,
    parse_logs_and_result,
)

DEFAULT_NAMESPACE: Optional[str] = None
TERMINAL_JOB_STATUSES = frozenset({"SUCCEEDED", "FAILED", "STOPPED"})
_RETRYABLE_HTTP_STATUS = frozenset({502, 503, 504})
_POLL_INTERVAL_S = 0.1
_MAX_POLL_INTERVAL_S = 2.0
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
            "Pass the Ray Job Submission HTTP address, e.g. 'http://<head>:8265'."
        )
    if lower == "local":
        raise RayClientError(
            "Address 'local' is not a Ray Jobs HTTP address. "
            "Pass 'http://<head>:8265' or set RAY_API_SERVER_ADDRESS."
        )
    if from_ray_address_env and not lower.startswith(("http://", "https://")):
        if "RAY_JOB_CONFIG_JSON_ENV_VAR" in os.environ or raw.endswith(":6379"):
            raise RayClientError(
                f"RAY_ADDRESS={raw!r} points to the Ray GCS gRPC port rather than "
                "the Ray Jobs HTTP server. Pass address='http://<head>:8265' or "
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
    """Validates a runtime_env['excludes'] value the way Ray does: a list of strings.

    A bare string would otherwise be unpacked into one-character patterns, and
    the `*` in "*.log" excludes the entire working_dir.
    """
    if value is None:
        return []
    if isinstance(value, (list, tuple)) and all(isinstance(p, str) for p in value):
        return list(value)
    raise TypeError(
        "runtime_env['excludes'] must be a list of gitignore-syntax strings, "
        f"got {value!r}."
    )


def _strip_marker_from_message(message: str) -> str:
    if RESULT_LINE_PREFIX not in message:
        return message
    kept = [
        ln for ln in message.splitlines(keepends=True) if RESULT_LINE_PREFIX not in ln
    ]
    return "".join(kept).rstrip()


class JobResult:
    """Handle to a function-as-driver job submitted to a Ray cluster."""

    def __init__(
        self,
        submission_id: str,
        client: "RayClient",
        package_uploaded: bool = False,
    ):
        self._submission_id = submission_id
        self._client = client
        self._package_uploaded = package_uploaded
        self._info: Optional[Dict[str, Any]] = None
        self._final_logs: Optional[str] = None

    def __repr__(self) -> str:
        return (
            f"JobResult(submission_id={self._submission_id!r}, "
            f"address={self._client.address!r})"
        )

    @property
    def submission_id(self) -> str:
        return self._submission_id

    @property
    def package_uploaded(self) -> bool:
        """True if this call uploaded the package; False if the cluster already had it cached."""
        return self._package_uploaded

    def _fetch_info(self, request_timeout: Optional[float] = None) -> Dict[str, Any]:
        if self._info is None or self._info.get("status") not in TERMINAL_JOB_STATUSES:
            info = dict(
                self._client._get_job_info(self._submission_id, timeout=request_timeout)
            )
            if isinstance(info.get("message"), str):
                info["message"] = _strip_marker_from_message(info["message"])
            self._info = info
        return self._info

    def info(self) -> Dict[str, Any]:
        """Returns a copy of the job's info dict from `GET /api/jobs/{id}` (cached once terminal)."""
        return copy.deepcopy(self._fetch_info())

    def status(self) -> str:
        return self._fetch_info()["status"]

    def done(self) -> bool:
        return self.status() in TERMINAL_JOB_STATUSES

    def _raw_logs(self, request_timeout: Optional[float] = None) -> str:
        if self._final_logs is not None:
            return self._final_logs
        done = (
            self._info is not None and self._info.get("status") in TERMINAL_JOB_STATUSES
        )
        raw = self._client._get_job_logs(self._submission_id, timeout=request_timeout)
        if done:
            self._final_logs = raw
        return raw

    def logs(self) -> str:
        """Returns driver stdout/stderr so far, with the internal result line removed."""
        return parse_logs_and_result(
            self._raw_logs(), final=False, submission_id=self._submission_id
        )[0]

    def stop(self) -> bool:
        return self._client._stop_job(self._submission_id)

    def _safe_stop(self) -> None:
        try:
            self._client._stop_job(
                self._submission_id, timeout=min(self._client._timeout, 5.0)
            )
        except BaseException:
            pass

    def delete(self) -> bool:
        return self._client._delete_job(self._submission_id)

    @staticmethod
    def _remaining_timeout(
        deadline: Optional[float], default_timeout: float
    ) -> Optional[float]:
        if deadline is None:
            return None
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return 0.0
        return min(default_timeout, remaining)

    def wait(
        self,
        timeout: Optional[float] = None,
        on_log_line: Optional[Callable[[str], None]] = None,
    ) -> str:
        """Blocks until the job is SUCCEEDED, FAILED, or STOPPED and returns that status.

        `on_log_line` receives each new driver log line while waiting. When called from
        a synchronous `run_function_as_driver(wait=True)` submission, Ctrl-C or timeout
        stops the owned remote job; on a detached (`wait=False`) or reattached
        (`get_job`) handle, the remote job is left running and `RayJobTimeoutError`
        carries this handle as `.job`.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        interval = _POLL_INTERVAL_S
        delivered = 0
        last_status = "PENDING"
        while True:
            req_timeout = self._remaining_timeout(deadline, self._client._timeout)
            if req_timeout is not None and req_timeout <= 0:
                raise RayJobTimeoutError(
                    f"Timed out after {timeout:.1f}s waiting for job "
                    f"'{self._submission_id}' (status: {last_status}).",
                    job=self,
                )
            try:
                status = self._fetch_info(request_timeout=req_timeout)["status"]
                last_status = status
                done = status in TERMINAL_JOB_STATUSES
                if on_log_line is not None:
                    log_timeout = self._remaining_timeout(
                        deadline, self._client._timeout
                    )
                    if log_timeout is not None and log_timeout <= 0 and not done:
                        raise RayJobTimeoutError(
                            f"Timed out after {timeout:.1f}s waiting for job "
                            f"'{self._submission_id}' (status: {last_status}).",
                            job=self,
                        )
                    raw = self._raw_logs(request_timeout=log_timeout)
                    text = parse_logs_and_result(
                        raw, final=False, submission_id=self._submission_id
                    )[0]
                    if not done:
                        # Only deliver complete lines while the driver is still writing.
                        text = text[: text.rfind("\n") + 1]
                    lines = text.splitlines()
                    for line in lines[delivered:]:
                        on_log_line(line)
                    delivered = len(lines)
            except RayClientError as exc:
                if isinstance(exc, RayJobTimeoutError):
                    raise
                if deadline is not None and time.monotonic() >= deadline:
                    raise RayJobTimeoutError(
                        f"Timed out after {timeout:.1f}s waiting for job "
                        f"'{self._submission_id}' (last error: {exc}).",
                        job=self,
                    ) from exc
                exc.job = self
                exc.submission_id = self._submission_id
                raise

            if done:
                return status
            if deadline is not None and time.monotonic() >= deadline:
                raise RayJobTimeoutError(
                    f"Timed out after {timeout:.1f}s waiting for job "
                    f"'{self._submission_id}' (status: {status}).",
                    job=self,
                )
            sleep_s = (
                interval
                if deadline is None
                else min(interval, max(0.0, deadline - time.monotonic()))
            )
            time.sleep(sleep_s)
            interval = min(interval * 1.5, _MAX_POLL_INTERVAL_S)

    def result(self, timeout: Optional[float] = None) -> Any:
        """Waits for the job and returns `fn`'s JSON return value.

        Raises RayJobExecutionError if the job failed, was stopped, or `fn` raised.
        """
        status = self.wait(timeout=timeout)
        user_logs, has_output, output, error = parse_logs_and_result(
            self._raw_logs(), final=True, submission_id=self._submission_id
        )
        if status == "SUCCEEDED" and has_output:
            return output
        message = self._fetch_info().get("message") or ""
        if status == "SUCCEEDED":
            message = (
                "Driver exited with code 0 but emitted no result line "
                "(it may have called os._exit or been killed)."
            )
        error = error or {}
        err = RayJobExecutionError(
            submission_id=self._submission_id,
            status=status,
            message=message,
            exc_type=error.get("exc_type"),
            exc_message=error.get("exc_message"),
            remote_traceback=error.get("traceback"),
            logs=user_logs,
        )
        err.job = self
        raise err


class RayClient:
    """Lightweight HTTP client for running Python functions as Ray driver jobs."""

    def __init__(
        self,
        address: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        verify: Union[bool, str] = True,
        namespace: Optional[str] = DEFAULT_NAMESPACE,
        default_runtime_env: Optional[Dict[str, Any]] = None,
        python_executable: str = "python",
        timeout: float = 30.0,
    ):
        if requests is None:
            raise ImportError(
                "RayClient requires the 'requests' package: pip install requests"
            )
        if namespace is not None and (
            not isinstance(namespace, str) or not namespace.strip()
        ):
            raise ValueError(
                f"namespace must be a non-empty string or None, got {namespace!r}."
            )
        self._address = _resolve_address(address)
        self._headers = dict(headers or {})
        token = _load_auth_token()
        if token and not any(k.lower() == "authorization" for k in self._headers):
            self._headers["Authorization"] = f"Bearer {token}"
        self._verify = verify
        self._namespace = namespace
        self._default_runtime_env = copy.deepcopy(default_runtime_env) or {}
        # Interpreter that runs the driver on the cluster. "python" matches the
        # `ray job submit` convention; override if the cluster image differs.
        self._python_executable = python_executable
        self._timeout = timeout
        # requests.Session is not thread-safe; one per thread keeps concurrent
        # submissions (e.g. from a ThreadPoolExecutor) safe with keep-alive.
        self._thread_local = threading.local()
        self._upload_locks_guard = threading.Lock()
        self._upload_locks: Dict[str, threading.Lock] = {}

    def __getstate__(self) -> Dict[str, Any]:
        state = dict(self.__dict__)
        state.pop("_thread_local", None)
        state.pop("_upload_locks_guard", None)
        state.pop("_upload_locks", None)
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._thread_local = threading.local()
        self._upload_locks_guard = threading.Lock()
        self._upload_locks = {}

    @property
    def address(self) -> str:
        return self._address

    @property
    def namespace(self) -> Optional[str]:
        return self._namespace

    @property
    def _session(self) -> "requests.Session":
        sess = getattr(self._thread_local, "session", None)
        if sess is None:
            sess = self._thread_local.session = requests.Session()
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
    ) -> "requests.Response":
        """Sends one HTTP request to the Ray Job Server.

        Idempotent requests are retried on connection errors and 502/503/504
        (common with port-forwards and ingress proxies); non-idempotent ones
        (job submission) are not retried inside `request`.
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
            # Typically an ingress login page or a non-dashboard port answering 200.
            raise RayClientError(
                f"{method} {endpoint} returned a non-JSON response from "
                f"{self._address}; is this the Ray Jobs HTTP address? Body starts "
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
        elif method == "POST" and endpoint == "/api/jobs/" and resp.status_code == 400:
            if "entrypoint_label_selector" in resp.text:
                hint += " Note: entrypoint_label_selector requires Ray >= 2.54 on the cluster."
            elif "entrypoint_memory" in resp.text:
                hint += " Note: entrypoint_memory requires Ray >= 2.8 on the cluster."
        return RayClientError(
            f"{method} {endpoint} failed ({resp.status_code}): {resp.text}{hint}"
        )

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
            put = self.request(
                "PUT", endpoint, data=zip_bytes
            )  # 200 with an empty body
            if put.status_code != 200:
                hint = ""
                if put.status_code == 413:
                    hint = (
                        f" The {len(zip_bytes) / 1024**2:.1f} MiB job package exceeds a "
                        "request body limit of the cluster or of a proxy in front of it; "
                        "shrink working_dir with runtime_env['excludes'] or raise the "
                        "proxy's limit."
                    )
                raise self._http_error("PUT", endpoint, put, hint)
            return True

    def get_job(self, submission_id: str) -> JobResult:
        """Returns a non-owning `JobResult` handle for an existing `submission_id`."""
        return JobResult(submission_id, self)

    def list_jobs(self) -> List[Dict[str, Any]]:
        return self._json("GET", "/api/jobs/")

    def _get_job_info(
        self, submission_id: str, timeout: Optional[float] = None
    ) -> Dict[str, Any]:
        return self._json("GET", f"/api/jobs/{submission_id}", timeout=timeout)

    def _get_job_logs(self, submission_id: str, timeout: Optional[float] = None) -> str:
        return self._json(
            "GET", f"/api/jobs/{submission_id}/logs", timeout=timeout
        ).get("logs", "")

    def _stop_job(self, submission_id: str, timeout: Optional[float] = None) -> bool:
        return bool(
            self._json("POST", f"/api/jobs/{submission_id}/stop", timeout=timeout).get(
                "stopped"
            )
        )

    def _delete_job(self, submission_id: str) -> bool:
        return bool(self._json("DELETE", f"/api/jobs/{submission_id}").get("deleted"))

    def _prepare_runtime_env(
        self, runtime_env: Optional[Dict[str, Any]]
    ) -> Tuple[Dict[str, Any], Optional[Union[str, Path]], List[str]]:
        """Merges the client default and per-call runtime_env and pulls out the
        local `working_dir` / `excludes` that this client packages itself."""
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
                    "working_dir in the Ray Jobs API; configure the container "
                    "image on the cluster or pass python_executable to RayClient."
                )

        working_dir = merged.pop("working_dir", None)
        if working_dir is not None and "://" in str(working_dir):
            raise RayClientError(
                f"runtime_env['working_dir']={working_dir!r} is a remote URI; "
                "ray_client must package the driver function into a local directory "
                "(place remote archives in runtime_env['py_modules'] instead)."
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
                # Only working_dir (and internal payload packages) are uploaded by
                # this client; the job server cannot resolve client-local paths.
                if "://" not in str(module):
                    raise RayClientError(
                        f"runtime_env['py_modules'] entry {module!r} is a local path; "
                        "copy it into runtime_env['working_dir'] or reference a remote URI."
                    )
        return merged, working_dir, excludes

    def run_function_as_driver(
        self,
        fn: Callable[..., Any],
        *args: Any,
        kwargs: Optional[Dict[str, Any]] = None,
        runtime_env: Optional[Dict[str, Any]] = None,
        namespace: Optional[str] = None,
        wait: bool = True,
        timeout: Optional[float] = None,
        stream_logs: bool = False,
        submission_id: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
        entrypoint_num_cpus: Optional[Union[int, float]] = None,
        entrypoint_num_gpus: Optional[Union[int, float]] = None,
        entrypoint_memory: Optional[int] = None,
        entrypoint_resources: Optional[Dict[str, float]] = None,
        entrypoint_label_selector: Optional[Dict[str, str]] = None,
    ) -> JobResult:
        """Runs `fn(*args, **kwargs)` as a driver job on the Ray cluster.

        `runtime_env` uses Ray's normal schema; a local `working_dir` is packaged
        together with `fn`, honoring its .gitignore/.rayignore files and the
        gitignore-syntax `excludes`, and must stay under 100 MiB compressed. With
        `wait=True` (default) this blocks until the job finishes and raises
        RayJobExecutionError on failure or RayJobTimeoutError (after stopping the
        job) on timeout. With `wait=False` it returns immediately; use the
        JobResult to poll, stream logs, or fetch the result later.
        """
        effective_namespace = self._namespace if namespace is None else namespace
        if effective_namespace is not None and (
            not isinstance(effective_namespace, str) or not effective_namespace.strip()
        ):
            raise ValueError(
                "namespace must be a non-empty string or None, "
                f"got {effective_namespace!r}."
            )

        merged_env, working_dir, excludes = self._prepare_runtime_env(runtime_env)
        (
            zip_bytes,
            package_uri,
            payload_b64,
            payload_pkg,
        ) = build_driver_and_payload_packages(
            fn, args, kwargs or {}, working_dir, excludes
        )
        uploaded = self._ensure_package_uploaded(package_uri, zip_bytes)

        merged_env["working_dir"] = package_uri
        env_vars = merged_env.setdefault("env_vars", {})
        if effective_namespace is not None:
            env_vars["RAY_CLIENT_NAMESPACE"] = effective_namespace
        if payload_b64 is not None:
            env_vars["RAY_CLIENT_PAYLOAD_B64"] = payload_b64
        elif payload_pkg is not None:
            payload_zip, payload_uri = payload_pkg
            self._ensure_package_uploaded(payload_uri, payload_zip)
            py_mods = list(merged_env.get("py_modules") or ())
            py_mods.append(payload_uri)
            merged_env["py_modules"] = py_mods
            env_vars["RAY_CLIENT_PAYLOAD_URI"] = payload_uri

        if submission_id is None:
            safe_fn = (
                "".join(c if c.isalnum() else "_" for c in fn.__name__)[:24].strip("_")
                or "fn"
            )
            submission_id = f"rayclient_{safe_fn}_{uuid.uuid4().hex[:12]}"
        env_vars["RAY_CLIENT_SUBMISSION_ID"] = submission_id

        body = {
            "entrypoint": f"{self._python_executable} _ray_client_driver_runner.py",
            "runtime_env": merged_env,
            "metadata": {"ray_client_fn": fn.__qualname__, **(metadata or {})},
            "submission_id": submission_id,
            "entrypoint_num_cpus": entrypoint_num_cpus,
            "entrypoint_num_gpus": entrypoint_num_gpus,
            "entrypoint_memory": entrypoint_memory,
            "entrypoint_resources": entrypoint_resources,
            "entrypoint_label_selector": entrypoint_label_selector,
        }
        body = {k: v for k, v in body.items() if v is not None}
        try:
            resp = self._json("POST", "/api/jobs/", json_data=body, idempotent=False)
            actual_id = resp.get("submission_id") or submission_id
        except RayClientError as post_exc:
            # If the proxy/connection dropped after the server created the job,
            # reconcile by probing the client-generated submission_id once.
            try:
                self._get_job_info(submission_id, timeout=min(self._timeout, 5.0))
                actual_id = submission_id
            except BaseException:
                raise post_exc

        job = JobResult(actual_id, self, package_uploaded=uploaded)
        if wait:
            try:
                job.wait(timeout=timeout, on_log_line=print if stream_logs else None)
                job.result()
            except BaseException as exc:
                if not isinstance(exc, RayJobExecutionError):
                    # A synchronous call owns its job: do not leave it running
                    # when the caller unwinds on timeout, Ctrl-C, or poll/stream failure.
                    job._safe_stop()
                if isinstance(exc, RayClientError):
                    exc.job = job
                    exc.submission_id = job.submission_id
                raise
        return job


def run_function_as_driver(
    fn: Callable[..., Any],
    *args: Any,
    address: Optional[str] = None,
    headers: Optional[Dict[str, str]] = None,
    verify: Union[bool, str] = True,
    namespace: Optional[str] = DEFAULT_NAMESPACE,
    default_runtime_env: Optional[Dict[str, Any]] = None,
    python_executable: str = "python",
    request_timeout: float = 30.0,
    **options: Any,
) -> JobResult:
    """Runs `fn(*args)` as a driver job on the cluster at `address` (see `RayClient.run_function_as_driver`)."""
    client = RayClient(
        address=address,
        headers=headers,
        verify=verify,
        namespace=namespace,
        default_runtime_env=default_runtime_env,
        python_executable=python_executable,
        timeout=request_timeout,
    )
    return client.run_function_as_driver(fn, *args, **options)
