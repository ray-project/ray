"""REST-only RayClient, ActorProxy, RemoteRef, run, and stream."""

import atexit
import copy
import inspect
import json
import os
import threading
import time
import uuid
import weakref
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Union

try:
    import requests
except ImportError:  # pragma: no cover - allows `import ray_client.packaging` without requests
    requests = None  # type: ignore[assignment]

from ray_client.exceptions import (
    RayClientError,
    RayExecutionError,
    RayTimeoutError,
)
from ray_client.packaging import (
    build_working_dir_package,
    extract_class_source_and_globals,
    extract_function_source_and_globals,
    normalize_json_refs,
    to_json,
    unwrap_actor_class,
)

_RETRYABLE_HTTP_STATUS = frozenset({502, 503, 504})
_MAX_RETRIES = 5
_UNSET = object()
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


def _raise_remote_error(target: str, err: Dict[str, Any]) -> None:
    exc_type = err.get("exc_type")
    exc_message = err.get("exc_message") or f"Remote execution '{target}' failed"
    if exc_type == "TimeoutError":
        raise RayTimeoutError(exc_message)
    raise RayExecutionError(
        target=target,
        status="FAILED",
        message=exc_message,
        exc_type=exc_type,
        exc_message=exc_message,
        remote_traceback=err.get("traceback"),
    )


class RemoteRef:
    """JSON reference token (`{"$ray_ref": "<ref_id>"}`) to an ObjectRef pinned in a cluster workspace."""

    __ray_client_ref__ = True

    def __init__(
        self,
        ref_id: str,
        workspace: str,
        client: "RayClient",
        package_uploaded: bool = False,
    ) -> None:
        self._ref_id = ref_id
        self._workspace = workspace
        self._client = client
        self._package_uploaded = package_uploaded

    def __repr__(self) -> str:
        return f"RemoteRef(ref_id={self._ref_id!r}, workspace={self._workspace!r})"

    @property
    def ref_id(self) -> str:
        return self._ref_id

    @property
    def workspace(self) -> str:
        return self._workspace

    @property
    def package_uploaded(self) -> bool:
        return self._package_uploaded

    def to_json(self) -> Dict[str, str]:
        return {"$ray_ref": self._ref_id}

    def ready(self, timeout: float = 0.0) -> bool:
        """Returns True if the referenced ObjectRef has finished executing on the cluster."""
        q = "?ready_only=1"
        if timeout and timeout > 0:
            q += f"&timeout={float(timeout)}"
        resp = self._client._json(
            "GET",
            f"/api/workspaces/{self._workspace}/refs/{self._ref_id}{q}",
        )
        if not resp.get("ok", True):
            _raise_remote_error(self._ref_id, resp.get("error") or {})
        return bool(resp.get("ready", False))

    def get(self, timeout: Optional[float] = None) -> Any:
        """Waits for and materializes the referenced object as JSON from the cluster workspace."""
        q = f"?timeout={float(timeout)}" if timeout is not None else ""
        http_timeout = (
            max(float(timeout) + 5.0, self._client._timeout)
            if timeout is not None
            else self._client._timeout
        )
        try:
            resp = self._client._json(
                "GET",
                f"/api/workspaces/{self._workspace}/refs/{self._ref_id}{q}",
                timeout=http_timeout,
            )
        except RayClientError as exc:
            if timeout is not None and "timed out" in str(exc).lower():
                raise RayTimeoutError(
                    f"Timed out after {timeout}s waiting for {self._ref_id}",
                    ref=self,
                ) from exc
            raise
        if not resp.get("ok", True):
            err = resp.get("error") or {}
            if err.get("exc_type") == "TimeoutError":
                raise RayTimeoutError(
                    err.get("exc_message")
                    or f"Timed out after {timeout}s waiting for {self._ref_id}",
                    ref=self,
                )
            _raise_remote_error(self._ref_id, err)
        return resp.get("result")

    def cancel(self, force: bool = False) -> bool:
        """Cancels the in-flight task or actor method producing this reference."""
        resp = self._client.request(
            "POST",
            f"/api/workspaces/{self._workspace}/refs/{self._ref_id}/cancel",
            json_data={"force": bool(force)},
            idempotent=False,
        )
        if resp.status_code == 404:
            return False
        if resp.status_code != 200:
            raise self._client._http_error(
                "POST",
                f"/api/workspaces/{self._workspace}/refs/{self._ref_id}/cancel",
                resp,
            )
        return bool(resp.json().get("cancelled"))

    def delete(self) -> bool:
        """Releases the reference from the cluster workspace reference table."""
        resp = self._client.request(
            "DELETE", f"/api/workspaces/{self._workspace}/refs/{self._ref_id}"
        )
        if resp.status_code == 404:
            return False
        if resp.status_code != 200:
            raise self._client._http_error(
                "DELETE",
                f"/api/workspaces/{self._workspace}/refs/{self._ref_id}",
                resp,
            )
        return bool(resp.json().get("deleted"))


class ActorProxy:
    """Handle to a live actor running inside a cluster workspace."""

    def __init__(
        self,
        name: str,
        workspace: str,
        client: "RayClient",
        created: bool = False,
        package_uploaded: bool = False,
    ) -> None:
        self._name = name
        self._workspace = workspace
        self._client = client
        self._created = created
        self._package_uploaded = package_uploaded

    def __repr__(self) -> str:
        return (
            f"ActorProxy(name={self._name!r}, workspace={self._workspace!r}, "
            f"address={self._client.address!r})"
        )

    @property
    def name(self) -> str:
        return self._name

    @property
    def workspace(self) -> str:
        return self._workspace

    @property
    def created(self) -> bool:
        return self._created

    @property
    def package_uploaded(self) -> bool:
        return self._package_uploaded

    def info(self) -> Dict[str, Any]:
        return self._client._json(
            "GET", f"/api/workspaces/{self._workspace}/actors/{self._name}"
        )

    def call(
        self,
        method: str,
        *args: Any,
        kwargs: Optional[Dict[str, Any]] = None,
        wait: bool = True,
        return_ref: bool = False,
        timeout: Optional[float] = None,
    ) -> Any:
        """Invokes `method(*args, **kwargs)` on the remote actor over HTTP."""
        to_json(list(args), f"Actor '{self._name}.{method}' positional arguments")
        to_json(dict(kwargs or {}), f"Actor '{self._name}.{method}' keyword arguments")
        body: Dict[str, Any] = {
            "args": normalize_json_refs(list(args)),
            "kwargs": normalize_json_refs(dict(kwargs or {})),
            "wait": bool(wait),
            "return_ref": bool(return_ref),
        }
        if timeout is not None:
            body["timeout"] = float(timeout)
        endpoint = (
            f"/api/workspaces/{self._workspace}/actors/{self._name}/methods/{method}"
        )
        http_timeout = (
            max(float(timeout) + 5.0, self._client._timeout)
            if timeout is not None
            else self._client._timeout
        )
        resp = self._client._json(
            "POST",
            endpoint,
            json_data=body,
            idempotent=False,
            timeout=http_timeout,
        )
        if not resp.get("ok", True):
            _raise_remote_error(
                f"{self._workspace}:{self._name}.{method}",
                resp.get("error") or {},
            )
        ref_marker = resp.get("ref") or (
            resp.get("result")
            if isinstance(resp.get("result"), dict)
            and set(resp["result"].keys()) == {"$ray_ref"}
            else None
        )
        if (not wait or return_ref) and isinstance(ref_marker, dict):
            return RemoteRef(ref_marker["$ray_ref"], self._workspace, self._client)
        return resp.get("result")

    def stream(
        self,
        method: str,
        *args: Any,
        kwargs: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None,
    ) -> Iterator[Any]:
        """Invokes a generator method on the remote actor and yields JSON chunks in real time."""
        to_json(list(args), f"Actor '{self._name}.{method}' positional arguments")
        to_json(dict(kwargs or {}), f"Actor '{self._name}.{method}' keyword arguments")
        body = {
            "args": normalize_json_refs(list(args)),
            "kwargs": normalize_json_refs(dict(kwargs or {})),
            "stream": True,
        }
        endpoint = f"/api/workspaces/{self._workspace}/actors/{self._name}/methods/{method}?stream=1"
        yield from self._client._iter_ndjson_stream(
            endpoint,
            body,
            target=f"{self._workspace}:{self._name}.{method}",
            timeout=timeout,
        )

    def kill(self, no_restart: bool = True) -> bool:
        q = "?no_restart=1" if no_restart else "?no_restart=0"
        resp = self._client.request(
            "DELETE",
            f"/api/workspaces/{self._workspace}/actors/{self._name}{q}",
        )
        if resp.status_code == 404:
            return False
        if resp.status_code != 200:
            raise self._client._http_error(
                "DELETE",
                f"/api/workspaces/{self._workspace}/actors/{self._name}",
                resp,
            )
        return bool(resp.json().get("killed"))

    def __getattr__(self, method: str) -> "_ActorMethodProxy":
        if method.startswith("_"):
            raise AttributeError(method)
        return _ActorMethodProxy(self, method)


class _ActorMethodProxy:
    """Bound method proxy on `ActorProxy` supporting direct calls, `.ref()`, `.remote()`, and `.stream()`."""

    def __init__(self, actor: ActorProxy, method: str) -> None:
        self._actor = actor
        self._method = method
        self.__name__ = method

    def __repr__(self) -> str:
        return (
            f"ActorMethodProxy(actor={self._actor.name!r}, "
            f"method={self._method!r}, workspace={self._actor.workspace!r})"
        )

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self._actor.call(self._method, *args, kwargs=kwargs or None)

    def call(
        self,
        *args: Any,
        kwargs: Optional[Dict[str, Any]] = None,
        wait: bool = True,
        return_ref: bool = False,
        timeout: Optional[float] = None,
    ) -> Any:
        return self._actor.call(
            self._method,
            *args,
            kwargs=kwargs,
            wait=wait,
            return_ref=return_ref,
            timeout=timeout,
        )

    def ref(self, *args: Any, **kwargs: Any) -> RemoteRef:
        """Executes the method, pins the result in the workspace object store, and returns a `RemoteRef`."""
        return self._actor.call(
            self._method, *args, kwargs=kwargs or None, wait=True, return_ref=True
        )

    def remote(self, *args: Any, **kwargs: Any) -> RemoteRef:
        """Submits the method non-blockingly (`wait=False`) and returns an in-flight `RemoteRef`."""
        return self._actor.call(self._method, *args, kwargs=kwargs or None, wait=False)

    def stream(self, *args: Any, **kwargs: Any) -> Iterator[Any]:
        """Streams yielded JSON items from a generator method on the actor."""
        return self._actor.stream(self._method, *args, kwargs=kwargs or None)


class RayClient:
    """Lightweight HTTP client for running workspaces, live actors, and functions on Ray."""

    def __init__(
        self,
        address: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        verify: Union[bool, str] = True,
        default_runtime_env: Optional[Dict[str, Any]] = None,
        timeout: float = 30.0,
        *,
        workspace: Optional[str] = None,
        ttl_seconds: Any = _UNSET,
    ):
        if requests is None:
            raise ImportError(
                "RayClient requires the 'requests' package: pip install requests"
            )
        if workspace is not None and (
            not isinstance(workspace, str) or not workspace.strip()
        ):
            raise ValueError(
                f"workspace must be a non-empty string or None, got {workspace!r}."
            )
        self._address = _resolve_address(address)
        self._headers = dict(headers or {})
        token = _load_auth_token()
        if token and not any(k.lower() == "authorization" for k in self._headers):
            self._headers["Authorization"] = f"Bearer {token}"
        self._verify = verify
        if workspace is None:
            self._workspace = f"ws_{uuid.uuid4().hex[:12]}"
            self._ephemeral_workspace = True
            self._ttl_seconds: Optional[float] = (
                300.0 if ttl_seconds is _UNSET else ttl_seconds
            )
        else:
            self._workspace = workspace
            self._ephemeral_workspace = False
            self._ttl_seconds = 1800.0 if ttl_seconds is _UNSET else ttl_seconds
        self._default_runtime_env = copy.deepcopy(default_runtime_env) or {}
        self._timeout = timeout
        self._workspace_registered = False
        self._thread_local = threading.local()
        self._upload_locks_guard = threading.Lock()
        self._upload_locks: Dict[str, threading.Lock] = {}

        if self._ephemeral_workspace:
            weak_self = weakref.ref(self)

            def _cleanup_ephemeral() -> None:
                inst = weak_self()
                if inst is not None and inst._workspace_registered:
                    try:
                        inst.close_workspace()
                    except Exception:
                        pass

            atexit.register(_cleanup_ephemeral)

    def __enter__(self) -> "RayClient":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

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
    def workspace(self) -> str:
        return self._workspace

    @property
    def namespace(self) -> str:
        return f"workspace:{self._workspace}"

    @property
    def ephemeral_workspace(self) -> bool:
        return self._ephemeral_workspace

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
        stream: bool = False,
    ) -> "requests.Response":
        """Sends one HTTP request to the Ray Dashboard Server."""
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

    def _iter_ndjson_stream(
        self,
        endpoint: str,
        body: Dict[str, Any],
        *,
        target: str,
        timeout: Optional[float] = None,
    ) -> Iterator[Any]:
        resp = self.request(
            "POST",
            endpoint,
            json_data=body,
            idempotent=False,
            timeout=timeout,
            stream=True,
        )
        if resp.status_code != 200:
            raise self._http_error("POST", endpoint, resp)
        try:
            for raw_line in resp.iter_lines(decode_unicode=True):
                if not raw_line:
                    continue
                line = (
                    raw_line.decode("utf-8")
                    if isinstance(raw_line, bytes)
                    else raw_line
                ).strip()
                if not line:
                    continue
                payload = json.loads(line)
                if not payload.get("ok", True):
                    _raise_remote_error(target, payload.get("error") or {})
                if "item" in payload:
                    yield payload["item"]
                elif "stream" in payload:
                    for item in payload["stream"]:
                        yield item
        finally:
            resp.close()

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

    def _ensure_workspace(
        self, initial_runtime_env: Optional[Dict[str, Any]] = None
    ) -> Tuple[Dict[str, Any], bool]:
        ws = self._workspace
        merged_env, working_dir, excludes = self._prepare_runtime_env(
            initial_runtime_env
        )
        uploaded = False
        if working_dir is not None:
            zip_bytes, pkg_uri = build_working_dir_package(working_dir, excludes)
            uploaded = self._ensure_package_uploaded(pkg_uri, zip_bytes)
            merged_env["working_dir"] = pkg_uri
        info = self._json(
            "POST",
            "/api/workspaces",
            json_data={
                "workspace": ws,
                "ttl_seconds": self._ttl_seconds,
                "runtime_env": merged_env or None,
            },
            idempotent=False,
        )
        self._workspace_registered = True
        return info, uploaded

    def workspace_info(self) -> Dict[str, Any]:
        """Returns status and metadata for the current workspace."""
        if not self._workspace_registered:
            info, _ = self._ensure_workspace()
            return info
        return self._json("GET", f"/api/workspaces/{self._workspace}")

    def list_workspaces(self) -> List[Dict[str, Any]]:
        """Lists active workspaces on the cluster."""
        res = self._json("GET", "/api/workspaces")
        if isinstance(res, dict) and "workspaces" in res:
            return res["workspaces"]
        return res

    def heartbeat(self) -> Dict[str, Any]:
        """Refreshes the idle TTL lease for the current workspace."""
        if not self._workspace_registered:
            info, _ = self._ensure_workspace()
            return info
        return self._json(
            "POST",
            f"/api/workspaces/{self._workspace}/heartbeat",
            json_data={"ttl_seconds": self._ttl_seconds},
        )

    def close_workspace(self, kill_detached: bool = False) -> bool:
        """Closes the workspace and terminates all workspace-owned actors and refs."""
        q = "?kill_detached=1" if kill_detached else ""
        resp = self.request("DELETE", f"/api/workspaces/{self._workspace}{q}")
        self._workspace_registered = False
        if resp.status_code == 404:
            return False
        if resp.status_code != 200:
            raise self._http_error(
                "DELETE", f"/api/workspaces/{self._workspace}{q}", resp
            )
        data = resp.json()
        return bool(data.get("ok", True) or data.get("closed", True))

    def close(self) -> bool:
        """Closes ephemeral workspaces automatically; leaves named workspaces active."""
        if self._ephemeral_workspace and self._workspace_registered:
            return self.close_workspace()
        return False

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

    def _build_task_spec(
        self,
        fn: Union[Callable[..., Any], str],
        args: Tuple[Any, ...],
        kwargs: Optional[Dict[str, Any]],
        runtime_env: Optional[Dict[str, Any]],
        num_cpus: Optional[Union[int, float]],
        num_gpus: Optional[Union[int, float]],
        memory: Optional[int],
        resources: Optional[Dict[str, float]],
        label_selector: Optional[Dict[str, str]],
        *,
        allow_generator: bool = False,
    ) -> Tuple[Dict[str, Any], bool]:
        if isinstance(fn, str):
            module_path, fn_name = _parse_entrypoint_str(fn, "Task")
            module_source = None
            captured_globals = None
            bound_args = list(args)
            bound_kwargs = dict(kwargs or {})
        else:
            module_path = None
            (
                fn_name,
                module_source,
                captured_globals,
            ) = extract_function_source_and_globals(fn, allow_generator=allow_generator)
            bound = inspect.signature(fn).bind(*args, **(kwargs or {}))
            bound.apply_defaults()
            bound_args = list(bound.args)
            bound_kwargs = dict(bound.kwargs)

        to_json(bound_args, "Function positional arguments")
        to_json(bound_kwargs, "Function keyword arguments")

        ws_uploaded = False
        if not self._workspace_registered:
            _, ws_uploaded = self._ensure_workspace(runtime_env)

        merged_env, working_dir, excludes = self._prepare_runtime_env(runtime_env)
        call_uploaded = False
        if working_dir is not None:
            zip_bytes, pkg_uri = build_working_dir_package(working_dir, excludes)
            call_uploaded = self._ensure_package_uploaded(pkg_uri, zip_bytes)
            merged_env["working_dir"] = pkg_uri

        spec: Dict[str, Any] = {
            "fn_name": fn_name,
            "module": module_path,
            "module_source": module_source,
            "captured_globals": captured_globals,
            "args": normalize_json_refs(bound_args),
            "kwargs": normalize_json_refs(bound_kwargs),
            "runtime_env": merged_env or None,
            "num_cpus": num_cpus,
            "num_gpus": num_gpus,
            "memory": memory,
            "resources": resources,
            "label_selector": label_selector,
        }
        spec = {k: v for k, v in spec.items() if v is not None}
        return spec, (ws_uploaded or call_uploaded)

    def run(
        self,
        fn: Callable[..., Any],
        *args: Any,
        kwargs: Optional[Dict[str, Any]] = None,
        wait: bool = True,
        return_ref: bool = False,
        timeout: Optional[float] = None,
        runtime_env: Optional[Dict[str, Any]] = None,
        num_cpus: Optional[Union[int, float]] = None,
        num_gpus: Optional[Union[int, float]] = None,
        memory: Optional[int] = None,
        resources: Optional[Dict[str, float]] = None,
        label_selector: Optional[Dict[str, str]] = None,
    ) -> Any:
        """Runs `fn(*args, **kwargs)` as a task inside the cluster workspace.

        With `wait=True, return_ref=False` (default), blocks until completion and
        returns the decoded JSON result. With `return_ref=True`, pins the result in
        the workspace object store and returns a `RemoteRef`. With `wait=False`,
        returns an in-flight `RemoteRef` immediately without blocking.
        """
        spec, uploaded = self._build_task_spec(
            fn,
            args,
            kwargs,
            runtime_env,
            num_cpus,
            num_gpus,
            memory,
            resources,
            label_selector,
        )
        spec["wait"] = bool(wait)
        spec["return_ref"] = bool(return_ref)
        if timeout is not None:
            spec["timeout"] = float(timeout)

        endpoint = f"/api/workspaces/{self._workspace}/tasks"
        http_timeout = (
            max(float(timeout) + 5.0, self._timeout)
            if timeout is not None
            else self._timeout
        )
        resp = self.request(
            "POST",
            endpoint,
            json_data=spec,
            idempotent=False,
            timeout=http_timeout,
        )
        if resp.status_code not in (200, 400):
            raise self._http_error("POST", endpoint, resp)
        data = resp.json()
        if not data.get("ok", True):
            _raise_remote_error(
                f"{self._workspace}:{spec['fn_name']}",
                data.get("error") or {},
            )
        if not wait or return_ref:
            ref_dict = data.get("ref") or data.get("result")
            return RemoteRef(
                ref_dict["$ray_ref"],
                self._workspace,
                self,
                package_uploaded=uploaded,
            )
        return data.get("result")

    def stream(
        self,
        fn: Callable[..., Any],
        *args: Any,
        kwargs: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None,
        runtime_env: Optional[Dict[str, Any]] = None,
        num_cpus: Optional[Union[int, float]] = None,
        num_gpus: Optional[Union[int, float]] = None,
        memory: Optional[int] = None,
        resources: Optional[Dict[str, float]] = None,
        label_selector: Optional[Dict[str, str]] = None,
    ) -> Iterator[Any]:
        """Runs generator function `fn(*args, **kwargs)` in the workspace and yields JSON items."""
        spec, _ = self._build_task_spec(
            fn,
            args,
            kwargs,
            runtime_env,
            num_cpus,
            num_gpus,
            memory,
            resources,
            label_selector,
            allow_generator=True,
        )
        spec["stream"] = True
        endpoint = f"/api/workspaces/{self._workspace}/tasks?stream=1"
        yield from self._iter_ndjson_stream(
            endpoint,
            spec,
            target=f"{self._workspace}:{spec['fn_name']}",
            timeout=timeout,
        )

    def actor(
        self,
        cls: Any,
        *args: Any,
        kwargs: Optional[Dict[str, Any]] = None,
        name: Optional[str] = None,
        lifetime: str = "workspace",
        get_if_exists: bool = True,
        runtime_env: Optional[Dict[str, Any]] = None,
        num_cpus: Optional[Union[int, float]] = None,
        num_gpus: Optional[Union[int, float]] = None,
        memory: Optional[int] = None,
        resources: Optional[Dict[str, float]] = None,
        label_selector: Optional[Dict[str, str]] = None,
        max_concurrency: Optional[int] = None,
    ) -> ActorProxy:
        """Creates or reattaches to a live actor in the current workspace over HTTP."""
        if lifetime not in ("workspace", "detached"):
            raise ValueError(
                f"lifetime must be 'workspace' or 'detached', got {lifetime!r}."
            )
        ws = self._workspace
        ws_uploaded = False
        if not self._workspace_registered:
            _, ws_uploaded = self._ensure_workspace(runtime_env)

        if isinstance(cls, str):
            module_path, class_name = _parse_entrypoint_str(cls, "Actor")
            module_source = None
            captured_globals = None
            bound_args = list(args)
            bound_kwargs = dict(kwargs or {})
        else:
            module_path = None
            raw_cls = unwrap_actor_class(cls)
            init_fn = inspect.unwrap(getattr(raw_cls, "__init__", None))
            if inspect.isfunction(init_fn):
                sig = inspect.signature(init_fn)
                params = [
                    p
                    for p in list(sig.parameters.values())[1:]
                    if p.name != "_ray_trace_ctx"
                ]
                bound = sig.replace(parameters=params).bind(*args, **(kwargs or {}))
                bound.apply_defaults()
                bound_args = list(bound.args)
                bound_kwargs = dict(bound.kwargs)
            else:
                bound_args = list(args)
                bound_kwargs = dict(kwargs or {})
            (
                class_name,
                module_source,
                captured_globals,
            ) = extract_class_source_and_globals(cls)

        to_json(bound_args, f"Actor '{class_name}' positional arguments")
        to_json(bound_kwargs, f"Actor '{class_name}' keyword arguments")

        merged_env, working_dir, excludes = self._prepare_runtime_env(runtime_env)
        call_uploaded = False
        if working_dir is not None:
            zip_bytes, pkg_uri = build_working_dir_package(working_dir, excludes)
            call_uploaded = self._ensure_package_uploaded(pkg_uri, zip_bytes)
            merged_env["working_dir"] = pkg_uri

        spec = {
            "class_name": class_name,
            "module": module_path,
            "module_source": module_source,
            "captured_globals": captured_globals,
            "args": normalize_json_refs(bound_args),
            "kwargs": normalize_json_refs(bound_kwargs),
            "name": name or class_name,
            "lifetime": lifetime,
            "get_if_exists": get_if_exists,
            "runtime_env": merged_env or None,
            "num_cpus": num_cpus,
            "num_gpus": num_gpus,
            "memory": memory,
            "resources": resources,
            "label_selector": label_selector,
            "max_concurrency": max_concurrency,
        }
        spec = {k: v for k, v in spec.items() if v is not None}
        resp = self.request(
            "POST",
            f"/api/workspaces/{ws}/actors",
            json_data=spec,
            idempotent=False,
        )
        if resp.status_code not in (200, 400):
            raise self._http_error("POST", f"/api/workspaces/{ws}/actors", resp)
        data = resp.json()
        if not data.get("ok", True):
            _raise_remote_error(
                f"{ws}:{spec['name']}",
                data.get("error") or {},
            )
        return ActorProxy(
            name=data["name"],
            workspace=ws,
            client=self,
            created=bool(data.get("created", True)),
            package_uploaded=(ws_uploaded or call_uploaded),
        )

    def get_actor(self, name: str) -> ActorProxy:
        """Returns an `ActorProxy` for an existing actor in the current workspace."""
        ws = self._workspace
        if not self._workspace_registered:
            self._ensure_workspace()
        self._json("GET", f"/api/workspaces/{ws}/actors/{name}")
        return ActorProxy(name=name, workspace=ws, client=self, created=False)

    def list_actors(self) -> List[Dict[str, Any]]:
        """Lists live actors registered in the current workspace."""
        ws = self._workspace
        if not self._workspace_registered:
            self._ensure_workspace()
        res = self._json("GET", f"/api/workspaces/{ws}/actors")
        if isinstance(res, dict) and "actors" in res:
            return res["actors"]
        return res

    def kill_actor(self, name: str, no_restart: bool = True) -> bool:
        """Terminates a named actor in the current workspace."""
        return ActorProxy(name=name, workspace=self._workspace, client=self).kill(
            no_restart=no_restart
        )

    def get_ref(
        self, ref: Union[RemoteRef, str], timeout: Optional[float] = None
    ) -> Any:
        """Resolves a `RemoteRef` or `ref_id` to its JSON value."""
        ref_obj = (
            ref
            if isinstance(ref, RemoteRef)
            else RemoteRef(str(ref), self._workspace, self)
        )
        return ref_obj.get(timeout=timeout)

    def ready_ref(self, ref: Union[RemoteRef, str], timeout: float = 0.0) -> bool:
        """Returns True if `ref` has finished executing on the cluster."""
        ref_obj = (
            ref
            if isinstance(ref, RemoteRef)
            else RemoteRef(str(ref), self._workspace, self)
        )
        return ref_obj.ready(timeout=timeout)

    def cancel_ref(self, ref: Union[RemoteRef, str], force: bool = False) -> bool:
        """Cancels the task or actor method producing `ref`."""
        ref_obj = (
            ref
            if isinstance(ref, RemoteRef)
            else RemoteRef(str(ref), self._workspace, self)
        )
        return ref_obj.cancel(force=force)

    def delete_ref(self, ref: Union[RemoteRef, str]) -> bool:
        """Unpins `ref` from the current workspace."""
        ref_obj = (
            ref
            if isinstance(ref, RemoteRef)
            else RemoteRef(str(ref), self._workspace, self)
        )
        return ref_obj.delete()


def run(
    fn: Callable[..., Any],
    *args: Any,
    address: Optional[str] = None,
    headers: Optional[Dict[str, str]] = None,
    verify: Union[bool, str] = True,
    default_runtime_env: Optional[Dict[str, Any]] = None,
    request_timeout: float = 30.0,
    workspace: Optional[str] = None,
    ttl_seconds: Any = _UNSET,
    **options: Any,
) -> Any:
    """Runs `fn(*args)` in a cluster workspace at `address` (see `RayClient.run`)."""
    client = RayClient(
        address=address,
        headers=headers,
        verify=verify,
        default_runtime_env=default_runtime_env,
        timeout=request_timeout,
        workspace=workspace,
        ttl_seconds=ttl_seconds,
    )
    wait = options.get("wait", True)
    return_ref = options.get("return_ref", False)
    try:
        return client.run(fn, *args, **options)
    finally:
        if wait and not return_ref:
            client.close()


def stream(
    fn: Callable[..., Any],
    *args: Any,
    address: Optional[str] = None,
    headers: Optional[Dict[str, str]] = None,
    verify: Union[bool, str] = True,
    default_runtime_env: Optional[Dict[str, Any]] = None,
    request_timeout: float = 30.0,
    workspace: Optional[str] = None,
    ttl_seconds: Any = _UNSET,
    **options: Any,
) -> Iterator[Any]:
    """Streams generator `fn(*args)` in a cluster workspace at `address` (see `RayClient.stream`)."""
    client = RayClient(
        address=address,
        headers=headers,
        verify=verify,
        default_runtime_env=default_runtime_env,
        timeout=request_timeout,
        workspace=workspace,
        ttl_seconds=ttl_seconds,
    )
    try:
        yield from client.stream(fn, *args, **options)
    finally:
        client.close()
