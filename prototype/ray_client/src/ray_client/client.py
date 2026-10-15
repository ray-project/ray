"""REST-only RayClient, JobResult, and run_function_as_driver."""

import copy
import os
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import requests

from ray_client.exceptions import (
    RayClientError,
    RayJobExecutionError,
    RayJobTimeoutError,
)
from ray_client.packaging import build_driver_package, parse_logs_and_result

DEFAULT_NAMESPACE = "ray_client"
TERMINAL_JOB_STATUSES = frozenset({"SUCCEEDED", "FAILED", "STOPPED"})
_RETRYABLE_HTTP_STATUS = frozenset({502, 503, 504})
_POLL_INTERVAL_S = 0.1
_MAX_POLL_INTERVAL_S = 2.0
_MAX_RETRIES = 5


def _resolve_address(address: Optional[str]) -> str:
    candidates = (
        address,
        os.environ.get("RAY_API_SERVER_ADDRESS"),
        os.environ.get("RAY_ADDRESS"),
    )
    raw = next((c for c in candidates if c and c != "auto"), "http://127.0.0.1:8265")
    if raw.startswith("ray://"):
        raise RayClientError(
            f"Address '{raw}' uses the legacy Ray Client gRPC scheme ('ray://'). "
            "Pass the Ray Job Submission HTTP address, e.g. 'http://<head>:8265'."
        )
    if not raw.startswith(("http://", "https://")):
        raw = f"http://{raw}"
    return raw.rstrip("/")


def _load_auth_token() -> Optional[str]:
    """Same precedence as Ray: RAY_AUTH_TOKEN, RAY_AUTH_TOKEN_PATH, then ~/.ray/auth_token."""
    token = os.environ.get("RAY_AUTH_TOKEN")
    if token:
        return token.strip()
    token_path = Path(
        os.environ.get("RAY_AUTH_TOKEN_PATH") or Path.home() / ".ray" / "auth_token"
    )
    if token_path.is_file():
        return token_path.read_text(encoding="utf-8").strip() or None
    return None


class JobResult:
    """Handle to a function-as-driver job submitted to a Ray cluster."""

    def __init__(
        self, submission_id: str, client: "RayClient", package_uploaded: bool = False
    ):
        self._submission_id = submission_id
        self._client = client
        self._package_uploaded = package_uploaded
        self._info: Optional[Dict[str, Any]] = None
        self._final_logs: Optional[str] = None

    @property
    def submission_id(self) -> str:
        return self._submission_id

    @property
    def package_uploaded(self) -> bool:
        """True if this call uploaded the package; False if the cluster already had it cached."""
        return self._package_uploaded

    def info(self) -> Dict[str, Any]:
        """Returns the job's info dict from `GET /api/jobs/{id}` (cached once terminal)."""
        if self._info is None or self._info["status"] not in TERMINAL_JOB_STATUSES:
            self._info = self._client._get_job_info(self._submission_id)
        return self._info

    def status(self) -> str:
        return self.info()["status"]

    def done(self) -> bool:
        return self.status() in TERMINAL_JOB_STATUSES

    def _raw_logs(self) -> str:
        if self._final_logs is not None:
            return self._final_logs
        done = self._info is not None and self._info["status"] in TERMINAL_JOB_STATUSES
        raw = self._client._get_job_logs(self._submission_id)
        if done:
            self._final_logs = raw
        return raw

    def logs(self) -> str:
        """Returns driver stdout/stderr so far, with the internal result line removed."""
        return parse_logs_and_result(self._raw_logs(), final=False)[0]

    def stop(self) -> bool:
        return self._client._stop_job(self._submission_id)

    def delete(self) -> bool:
        return self._client._delete_job(self._submission_id)

    def wait(
        self,
        timeout: Optional[float] = None,
        on_log_line: Optional[Callable[[str], None]] = None,
    ) -> str:
        """Blocks until the job is SUCCEEDED, FAILED, or STOPPED and returns that status.

        `on_log_line` receives each new driver log line while waiting. Ctrl-C stops
        the remote job before re-raising KeyboardInterrupt. On timeout the job keeps
        running and the raised RayJobTimeoutError carries this handle as `.job`.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        interval = _POLL_INTERVAL_S
        delivered = 0
        try:
            while True:
                status = self.status()
                done = status in TERMINAL_JOB_STATUSES
                if on_log_line is not None:
                    text = self.logs()
                    if not done:
                        # Only deliver complete lines while the driver is still writing.
                        text = text[: text.rfind("\n") + 1]
                    lines = text.splitlines()
                    for line in lines[delivered:]:
                        on_log_line(line)
                    delivered = len(lines)
                if done:
                    return status
                if deadline is not None and time.monotonic() >= deadline:
                    raise RayJobTimeoutError(
                        f"Timed out after {timeout:.1f}s waiting for job "
                        f"'{self._submission_id}' (status: {status}).",
                        job=self,
                    )
                time.sleep(interval)
                interval = min(interval * 1.5, _MAX_POLL_INTERVAL_S)
        except KeyboardInterrupt:
            self.stop()
            raise

    def result(self, timeout: Optional[float] = None) -> Any:
        """Waits for the job and returns `fn`'s JSON return value.

        Raises RayJobExecutionError if the job failed, was stopped, or `fn` raised.
        """
        status = self.wait(timeout=timeout)
        user_logs, has_output, output, error = parse_logs_and_result(
            self._raw_logs(), final=True
        )
        if status == "SUCCEEDED" and has_output:
            return output
        message = self.info().get("message") or ""
        if status == "SUCCEEDED":
            message = (
                "Driver exited with code 0 but emitted no result line "
                "(it may have called os._exit or been killed)."
            )
        error = error or {}
        raise RayJobExecutionError(
            submission_id=self._submission_id,
            status=status,
            message=message,
            exc_type=error.get("exc_type"),
            exc_message=error.get("exc_message"),
            remote_traceback=error.get("traceback"),
            logs=user_logs,
        )


class RayClient:
    """Lightweight HTTP client for running Python functions as Ray driver jobs."""

    def __init__(
        self,
        address: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        verify: Union[bool, str] = True,
        namespace: str = DEFAULT_NAMESPACE,
        default_runtime_env: Optional[Dict[str, Any]] = None,
        python_executable: str = "python",
        timeout: float = 30.0,
    ):
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

    @property
    def address(self) -> str:
        return self._address

    @property
    def namespace(self) -> str:
        return self._namespace

    @property
    def _session(self) -> requests.Session:
        sess = getattr(self._thread_local, "session", None)
        if sess is None:
            sess = self._thread_local.session = requests.Session()
        return sess

    def request(
        self,
        method: str,
        endpoint: str,
        *,
        data: Optional[bytes] = None,
        json_data: Optional[Dict[str, Any]] = None,
        idempotent: bool = True,
    ) -> requests.Response:
        """Sends one HTTP request to the Ray Job Server.

        Idempotent requests are retried on connection errors and 502/503/504
        (common with port-forwards and ingress proxies); non-idempotent ones
        (job submission) are never retried to avoid duplicate jobs.
        """
        url = f"{self._address}{endpoint}"
        attempts = _MAX_RETRIES if idempotent else 1
        delay = 0.2
        last_exc: Optional[BaseException] = None
        for attempt in range(attempts):
            try:
                resp = self._session.request(
                    method,
                    url,
                    data=data,
                    json=json_data,
                    headers=self._headers,
                    verify=self._verify,
                    timeout=self._timeout,
                )
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
        return self._ok(method, endpoint, **kwargs).json()

    def _ok(self, method: str, endpoint: str, **kwargs: Any) -> requests.Response:
        resp = self.request(method, endpoint, **kwargs)
        if resp.status_code != 200:
            raise RayClientError(
                f"{method} {endpoint} failed ({resp.status_code}): {resp.text}"
            )
        return resp

    def _ensure_package_uploaded(self, package_uri: str, zip_bytes: bytes) -> bool:
        """Uploads `zip_bytes` unless `package_uri` is already cached on the cluster."""
        protocol, name = package_uri.split("://", 1)
        endpoint = f"/api/packages/{protocol}/{name}"
        check = self.request("GET", endpoint)
        if check.status_code == 200:
            return False
        if check.status_code != 404:
            raise RayClientError(
                f"GET {endpoint} failed ({check.status_code}): {check.text}"
            )
        self._ok("PUT", endpoint, data=zip_bytes)  # 200 with an empty body
        return True

    def get_job(self, submission_id: str) -> JobResult:
        """Returns a `JobResult` handle for an existing `submission_id`."""
        return JobResult(submission_id, self)

    def list_jobs(self) -> List[Dict[str, Any]]:
        return self._json("GET", "/api/jobs/")

    def _get_job_info(self, submission_id: str) -> Dict[str, Any]:
        return self._json("GET", f"/api/jobs/{submission_id}")

    def _get_job_logs(self, submission_id: str) -> str:
        return self._json("GET", f"/api/jobs/{submission_id}/logs").get("logs", "")

    def _stop_job(self, submission_id: str) -> bool:
        return bool(
            self._json("POST", f"/api/jobs/{submission_id}/stop").get("stopped")
        )

    def _delete_job(self, submission_id: str) -> bool:
        return bool(self._json("DELETE", f"/api/jobs/{submission_id}").get("deleted"))

    def _prepare_runtime_env(
        self, runtime_env: Optional[Dict[str, Any]]
    ) -> Tuple[Dict[str, Any], Optional[Union[str, Path]], List[str]]:
        """Merges the client default and per-call runtime_env and pulls out the
        local `working_dir` / `excludes` that this client packages itself."""
        merged = copy.deepcopy(self._default_runtime_env)
        for key, value in (runtime_env or {}).items():
            if key == "env_vars":
                merged.setdefault("env_vars", {}).update(value)
            elif key == "excludes":
                merged.setdefault("excludes", []).extend(value)
            else:
                merged[key] = copy.deepcopy(value)

        working_dir = merged.pop("working_dir", None)
        if working_dir is not None and "://" in str(working_dir):
            raise RayClientError(
                f"runtime_env['working_dir']={working_dir!r} is a remote URI; "
                "ray_client must package the driver function into a local directory "
                "(place remote archives in runtime_env['py_modules'] instead)."
            )
        return merged, working_dir, list(merged.pop("excludes", None) or [])

    def run_function_as_driver(
        self,
        fn: Callable[..., Any],
        *args: Any,
        kwargs: Optional[Dict[str, Any]] = None,
        runtime_env: Optional[Dict[str, Any]] = None,
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
        together with `fn` and `excludes` filters it. With `wait=True` (default)
        this blocks until the job finishes and raises RayJobExecutionError on
        failure or RayJobTimeoutError (after stopping the job) on timeout. With
        `wait=False` it returns immediately; use the JobResult to poll, stream
        logs, or fetch the result later.
        """
        merged_env, working_dir, excludes = self._prepare_runtime_env(runtime_env)
        zip_bytes, package_uri, payload_b64 = build_driver_package(
            fn, args, kwargs or {}, working_dir, excludes
        )
        uploaded = self._ensure_package_uploaded(package_uri, zip_bytes)

        merged_env["working_dir"] = package_uri
        env_vars = merged_env.setdefault("env_vars", {})
        env_vars["RAY_CLIENT_NAMESPACE"] = self._namespace
        if payload_b64 is not None:
            env_vars["RAY_CLIENT_PAYLOAD_B64"] = payload_b64

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
        resp = self._json("POST", "/api/jobs/", json_data=body, idempotent=False)
        job = JobResult(resp["submission_id"], self, package_uploaded=uploaded)

        if wait:
            try:
                job.wait(timeout=timeout, on_log_line=print if stream_logs else None)
            except RayJobTimeoutError:
                # A synchronous call owns its job: do not leave it running.
                job.stop()
                raise
            job.result()
        return job


def run_function_as_driver(
    fn: Callable[..., Any], *args: Any, address: Optional[str] = None, **options: Any
) -> JobResult:
    """Runs `fn(*args)` as a driver job on the cluster at `address` (see `RayClient.run_function_as_driver`)."""
    return RayClient(address=address).run_function_as_driver(fn, *args, **options)
