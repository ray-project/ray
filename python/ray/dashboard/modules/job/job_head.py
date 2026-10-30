import asyncio
import contextlib
import dataclasses
import enum
import json
import logging
import os
import time
import traceback
import urllib.parse
from datetime import datetime
from typing import AsyncIterator, Dict, List, Optional, Tuple

import aiohttp.web
from aiohttp.client import ClientResponse
from aiohttp.web import Request, Response, StreamResponse

import ray
from ray import NodeID
from ray._common.network_utils import build_address
from ray._common.pydantic_compat import BaseModel, Extra, Field, validator
from ray._common.utils import get_or_create_event_loop, load_class
from ray._private.authentication.http_token_authentication import (
    get_auth_headers_if_auth_enabled,
)
from ray._private.ray_constants import KV_NAMESPACE_DASHBOARD
from ray._private.runtime_env.packaging import (
    package_exists,
    pin_runtime_env_uri,
    upload_package_to_gcs,
)
from ray._raylet import RAY_INTERNAL_DASHBOARD_NAMESPACE
from ray.dashboard.consts import (
    DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX,
    GCS_RPC_TIMEOUT_SECONDS,
    RAY_CLUSTER_ACTIVITY_HOOK,
    TRY_TO_GET_AGENT_INFO_INTERVAL_SECONDS,
    WAIT_AVAILABLE_AGENT_TIMEOUT,
)
from ray.dashboard.modules.job.common import (
    JobDeleteResponse,
    JobInfoStorageClient,
    JobLogsResponse,
    JobStopResponse,
    JobSubmitRequest,
    JobSubmitResponse,
    http_uri_components_to_uri,
)
from ray.dashboard.modules.job.pydantic_models import JobDetails, JobType
from ray.dashboard.modules.job.utils import (
    find_job_by_ids,
    get_driver_jobs,
    parse_and_validate_request,
)
from ray.dashboard.modules.version import CURRENT_VERSION, VersionResponse
from ray.dashboard.runtime_env_redaction import (
    redact_runtime_env,
    should_redact_runtime_env,
)
from ray.dashboard.subprocesses.module import SubprocessModule
from ray.dashboard.subprocesses.routes import SubprocessRouteTable as routes
from ray.dashboard.subprocesses.utils import ResponseType
from ray.dashboard.utils import get_head_node_id

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class RayActivityStatus(str, enum.Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    ERROR = "ERROR"


class RayActivityResponse(BaseModel, extra=Extra.allow):
    """
    Pydantic model used to inform if a particular Ray component can be considered
    active, and metadata about observation.
    """

    is_active: RayActivityStatus = Field(
        ...,
        description=(
            "Whether the corresponding Ray component is considered active or inactive, "
            "or if there was an error while collecting this observation."
        ),
    )
    reason: Optional[str] = Field(
        None, description="Reason if Ray component is considered active or errored."
    )
    timestamp: float = Field(
        ...,
        description=(
            "Timestamp of when this observation about the Ray component was made. "
            "This is in the format of seconds since unix epoch."
        ),
    )
    last_activity_at: Optional[float] = Field(
        None,
        description=(
            "Timestamp when last actvity of this Ray component finished in format of "
            "seconds since unix epoch. This field does not need to be populated "
            "for Ray components where it is not meaningful."
        ),
    )

    @validator("reason", always=True)
    def reason_required(cls, v, values, **kwargs):
        if "is_active" in values and values["is_active"] != RayActivityStatus.INACTIVE:
            if v is None:
                raise ValueError(
                    'Reason is required if is_active is "active" or "error"'
                )
        return v


class JobAgentSubmissionClient:
    """A local client for submitting and interacting with jobs on a specific node
    in the remote cluster.
    Submits requests over HTTP to the job agent on the specific node using the REST API.
    """

    def __init__(
        self,
        dashboard_agent_address: str,
    ):
        self._agent_address = dashboard_agent_address
        self._session = aiohttp.ClientSession()

    def _get_headers(self):
        """Get auth headers if token authentication is enabled."""
        return get_auth_headers_if_auth_enabled({})

    async def _raise_error(self, resp: ClientResponse):
        status = resp.status
        error_text = await resp.text()
        raise RuntimeError(f"Request failed with status code {status}: {error_text}.")

    async def submit_job_internal(self, req: JobSubmitRequest) -> JobSubmitResponse:
        logger.debug(f"Submitting job with submission_id={req.submission_id}.")

        async with self._session.post(
            f"{self._agent_address}/api/job_agent/jobs/",
            json=dataclasses.asdict(req),
            headers=self._get_headers(),
        ) as resp:
            if resp.status == 200:
                result_json = await resp.json()
                return JobSubmitResponse(**result_json)
            else:
                await self._raise_error(resp)

    async def stop_job_internal(self, job_id: str) -> JobStopResponse:
        logger.debug(f"Stopping job with job_id={job_id}.")

        async with self._session.post(
            f"{self._agent_address}/api/job_agent/jobs/{job_id}/stop",
            headers=self._get_headers(),
        ) as resp:
            if resp.status == 200:
                result_json = await resp.json()
                return JobStopResponse(**result_json)
            else:
                await self._raise_error(resp)

    async def delete_job_internal(self, job_id: str) -> JobDeleteResponse:
        logger.debug(f"Deleting job with job_id={job_id}.")

        async with self._session.delete(
            f"{self._agent_address}/api/job_agent/jobs/{job_id}",
            headers=self._get_headers(),
        ) as resp:
            if resp.status == 200:
                result_json = await resp.json()
                return JobDeleteResponse(**result_json)
            else:
                await self._raise_error(resp)

    async def get_job_logs_internal(self, job_id: str) -> JobLogsResponse:
        async with self._session.get(
            f"{self._agent_address}/api/job_agent/jobs/{job_id}/logs",
            headers=self._get_headers(),
        ) as resp:
            if resp.status == 200:
                result_json = await resp.json()
                return JobLogsResponse(**result_json)
            else:
                await self._raise_error(resp)

    async def tail_job_logs(self, job_id: str) -> AsyncIterator[str]:
        """Get an iterator that follows the logs of a job."""
        ws = await self._session.ws_connect(
            f"{self._agent_address}/api/job_agent/jobs/{job_id}/logs/tail",
            headers=self._get_headers(),
        )

        while True:
            msg = await ws.receive()

            if msg.type == aiohttp.WSMsgType.TEXT:
                yield msg.data
            elif msg.type == aiohttp.WSMsgType.CLOSED:
                logger.info(
                    f"WebSocket to job agent closed for job {job_id} "
                    f"with close code {ws.close_code}"
                )
                break
            elif msg.type == aiohttp.WSMsgType.ERROR:
                logger.warning(
                    f"WebSocket to job agent received an error message "
                    f"while tailing logs for job {job_id}: {ws.exception()!r}. "
                )
                pass

    async def close(self, ignore_error=True):
        try:
            await self._session.close()
        except Exception:
            if not ignore_error:
                raise

    async def forward_actor_request(
        self,
        method: str,
        subpath: str,
        json_body: Optional[Dict] = None,
        params: Optional[Dict[str, str]] = None,
        prefix: str = "/api/job_agent/actors",
        timeout: Optional[aiohttp.ClientTimeout] = None,
    ) -> Tuple[int, str, str]:
        url = f"{self._agent_address}{prefix}{subpath}"
        req_kwargs = {
            "json": json_body,
            "params": params,
            "headers": self._get_headers(),
        }
        if timeout is not None:
            req_kwargs["timeout"] = timeout
        async with self._session.request(
            method,
            url,
            **req_kwargs,
        ) as resp:
            text = await resp.text()
            content_type = (
                resp.headers.get("Content-Type", "application/json")
                .split(";")[0]
                .strip()
            )
            return resp.status, text, content_type

    async def stream_actor_request(
        self,
        method: str,
        subpath: str,
        json_body: Optional[Dict] = None,
        params: Optional[Dict[str, str]] = None,
        req: Optional[Request] = None,
    ) -> AsyncIterator[Tuple[int, str, bytes]]:
        """Yields `(status, content_type, chunk)` tuples for an agent actor request.

        A non-200 response is yielded once with its whole body. A 200 response is
        yielded with an empty chunk as soon as its headers arrive, then once per body
        chunk until the body ends or the client of `req` disconnects. Stopping early
        closes the connection to the agent, which cancels the call there.
        """
        url = f"{self._agent_address}/api/job_agent/actors{subpath}"
        async with self._session.request(
            method,
            url,
            json=json_body,
            params=params,
            headers=self._get_headers(),
            timeout=aiohttp.ClientTimeout(total=None),
        ) as resp:
            content_type = resp.headers.get("Content-Type", "application/json").strip()
            if resp.status != 200:
                yield resp.status, content_type, await resp.read()
                return
            yield resp.status, content_type, b""
            ait = resp.content.iter_chunks().__aiter__()
            next_chunk_task: Optional[asyncio.Task] = None
            try:
                while True:
                    if req is not None and (
                        req.transport is None or req.transport.is_closing()
                    ):
                        break
                    if next_chunk_task is None:
                        next_chunk_task = asyncio.ensure_future(ait.__anext__())
                    done, _ = await asyncio.wait({next_chunk_task}, timeout=0.05)
                    if not done:
                        continue
                    try:
                        chunk, _ = next_chunk_task.result()
                    except StopAsyncIteration:
                        break
                    finally:
                        next_chunk_task = None
                    if chunk:
                        yield resp.status, content_type, chunk
            finally:
                if next_chunk_task is not None and not next_chunk_task.done():
                    next_chunk_task.cancel()
                    with contextlib.suppress(BaseException):
                        await next_chunk_task


def _job_details_to_dict(job: JobDetails, redact: bool) -> Dict:
    """Serialize `job` for an HTTP response, optionally redacting its runtime env."""
    job_dict = job.dict()
    if redact:
        job_dict["runtime_env"] = redact_runtime_env(job_dict.get("runtime_env"))
    return job_dict


class JobHead(SubprocessModule):
    """Runs on the head node of a Ray cluster and handles Ray Jobs APIs.

    NOTE(architkulkarni): Please keep this class in sync with the OpenAPI spec at
    `doc/source/cluster/running-applications/job-submission/openapi.yml`.
    If any changes are made to the paths in the @route decorators or in the
    Responses returned by the methods (or any nested fields in the Responses),
    you will need to find the corresponding field of the OpenAPI yaml file and
    update it manually. Also, bump the version number in the yaml file and
    `CURRENT_VERSION` in `ray/dashboard/modules/version.py`.

    The OpenAPI tests in `tests/test_http_job_server.py` check that the spec's
    routes match this class's, that its version matches `CURRENT_VERSION`, and
    that the responses of the endpoints they exercise match the spec's schemas.
    """

    # Time that we sleep while tailing logs while waiting for
    # the supervisor actor to start. We don't know which node
    # to read the logs from until then.
    WAIT_FOR_SUPERVISOR_ACTOR_INTERVAL_S = 1

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._job_info_client = None

        # To make sure that the internal KV is initialized by getting the lazy property
        assert self.gcs_client is not None
        assert ray.experimental.internal_kv._internal_kv_initialized()

        # It contains all `JobAgentSubmissionClient` that
        # `JobHead` has ever used, and will not be deleted
        # from it unless `JobAgentSubmissionClient` is no
        # longer available (the corresponding agent process is dead)
        # {node_id: JobAgentSubmissionClient}
        self._agents: Dict[NodeID, JobAgentSubmissionClient] = dict()
        # Latest activity observed on an `/api/actors` client actor, kept so that
        # `last_activity_at` doesn't move backwards once the actor is gone.
        self._last_client_actor_activity_at = 0.0

    async def get_target_agent(
        self, timeout_s: float = WAIT_AVAILABLE_AGENT_TIMEOUT
    ) -> JobAgentSubmissionClient:
        """
        Get a `JobAgentSubmissionClient`, which is a client for interacting with jobs
        via an agent process.

        Args:
            timeout_s: The timeout for the operation.

        Returns:
            A `JobAgentSubmissionClient` for interacting with jobs via an agent process.

        Raises:
            TimeoutError: If the operation times out.
        """
        return await self._get_head_node_agent(timeout_s)

    async def _get_head_node_agent_once(self) -> JobAgentSubmissionClient:
        head_node_id_hex = await get_head_node_id(self.gcs_client)

        if not head_node_id_hex:
            raise Exception("Head node id has not yet been persisted in GCS")

        head_node_id = NodeID.from_hex(head_node_id_hex)

        if head_node_id not in self._agents:
            ip, http_port, _ = await self._fetch_agent_info(head_node_id)
            agent_http_address = f"http://{build_address(ip, http_port)}"
            self._agents[head_node_id] = JobAgentSubmissionClient(agent_http_address)

        return self._agents[head_node_id]

    async def _get_head_node_agent(self, timeout_s: float) -> JobAgentSubmissionClient:
        """Retrieves HTTP client for `JobAgent` running on the Head node. If the head
        node does not have an agent, it will retry every
        `TRY_TO_GET_AGENT_INFO_INTERVAL_SECONDS` seconds indefinitely.

        Args:
            timeout_s: The timeout for the operation.

        Returns:
            A `JobAgentSubmissionClient` for interacting with jobs via the head node's agent process.

        Raises:
            TimeoutError: If the operation times out.
        """
        timeout_point = time.time() + timeout_s
        exception = None
        while time.time() < timeout_point:
            try:
                return await self._get_head_node_agent_once()
            except Exception as e:
                exception = e
                logger.exception(
                    f"Failed to get head node agent, retrying in {TRY_TO_GET_AGENT_INFO_INTERVAL_SECONDS} seconds..."
                )
                await asyncio.sleep(TRY_TO_GET_AGENT_INFO_INTERVAL_SECONDS)
        raise TimeoutError(
            f"Failed to get head node agent within {timeout_s} seconds. The last exception is {exception}"
        )

    async def _fetch_agent_info(self, target_node_id: NodeID) -> Tuple[str, int, int]:
        """
        Fetches agent info by the Node ID. May raise exception if there's network error or the
        agent info is not found.

        Returns: (ip, http_port, grpc_port)
        """
        key = f"{DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX}{target_node_id.hex()}"
        value = await self.gcs_client.async_internal_kv_get(
            key,
            namespace=KV_NAMESPACE_DASHBOARD,
            timeout=GCS_RPC_TIMEOUT_SECONDS,
        )
        if not value:
            raise KeyError(
                f"Agent info not found in internal KV for node {target_node_id}. "
                "It's possible that the agent didn't launch successfully due to "
                "port conflicts or other issues. Please check `dashboard_agent.log` "
                "for more details."
            )
        return json.loads(value.decode())

    @routes.get("/api/version")
    async def get_version(self, req: Request) -> Response:
        # NOTE(edoakes): CURRENT_VERSION should be bumped and checked on the
        # client when we have backwards-incompatible changes.
        resp = VersionResponse(
            version=CURRENT_VERSION,
            ray_version=ray.__version__,
            ray_commit=ray.__commit__,
            session_name=self.session_name,
        )
        return Response(
            text=json.dumps(dataclasses.asdict(resp)),
            content_type="application/json",
            status=aiohttp.web.HTTPOk.status_code,
        )

    @routes.get("/api/packages/{protocol}/{package_name}")
    async def get_package(self, req: Request) -> Response:
        package_uri = http_uri_components_to_uri(
            protocol=req.match_info["protocol"],
            package_name=req.match_info["package_name"],
        )

        logger.debug(f"Adding temporary reference to package {package_uri}.")
        try:
            pin_runtime_env_uri(package_uri)
        except Exception:
            return Response(
                text=traceback.format_exc(),
                status=aiohttp.web.HTTPInternalServerError.status_code,
            )

        if not package_exists(package_uri):
            return Response(
                text=f"Package {package_uri} does not exist",
                status=aiohttp.web.HTTPNotFound.status_code,
            )

        return Response()

    @routes.put("/api/packages/{protocol}/{package_name}")
    async def upload_package(self, req: Request):
        package_uri = http_uri_components_to_uri(
            protocol=req.match_info["protocol"],
            package_name=req.match_info["package_name"],
        )
        logger.info(f"Uploading package {package_uri} to the GCS.")
        try:
            data = await req.read()
            await get_or_create_event_loop().run_in_executor(
                None,
                upload_package_to_gcs,
                package_uri,
                data,
            )
        except Exception:
            return Response(
                text=traceback.format_exc(),
                status=aiohttp.web.HTTPInternalServerError.status_code,
            )

        return Response(status=aiohttp.web.HTTPOk.status_code)

    @routes.post("/api/jobs/")
    async def submit_job(self, req: Request) -> Response:
        result = await parse_and_validate_request(req, JobSubmitRequest)
        # Request parsing failed, returned with Response object.
        if isinstance(result, Response):
            return result
        else:
            submit_request: JobSubmitRequest = result

        try:
            job_agent_client = await self.get_target_agent()
            resp = await job_agent_client.submit_job_internal(submit_request)
        except asyncio.TimeoutError:
            logger.warning(
                "Timed out waiting for an available job agent to submit the job."
            )
            return Response(
                text="No available agent to submit job, please try again later.",
                status=aiohttp.web.HTTPInternalServerError.status_code,
            )
        except (TypeError, ValueError):
            logger.warning("Failed to submit job due to an invalid request.")
            return Response(
                text=traceback.format_exc(),
                status=aiohttp.web.HTTPBadRequest.status_code,
            )
        except Exception:
            logger.exception("Failed to submit job due to unexpected exception.")
            return Response(
                text=traceback.format_exc(),
                status=aiohttp.web.HTTPInternalServerError.status_code,
            )

        return Response(
            text=json.dumps(dataclasses.asdict(resp)),
            content_type="application/json",
            status=aiohttp.web.HTTPOk.status_code,
        )

    @routes.post("/api/jobs/{job_or_submission_id}/stop")
    async def stop_job(self, req: Request) -> Response:
        job_or_submission_id = req.match_info["job_or_submission_id"]
        job = await find_job_by_ids(
            self.gcs_client,
            self._job_info_client,
            job_or_submission_id,
        )
        if not job:
            return Response(
                text=f"Job {job_or_submission_id} does not exist",
                status=aiohttp.web.HTTPNotFound.status_code,
            )
        if job.type is not JobType.SUBMISSION:
            return Response(
                text="Can only stop submission type jobs",
                status=aiohttp.web.HTTPBadRequest.status_code,
            )

        try:
            job_agent_client = await self.get_target_agent()
            resp = await job_agent_client.stop_job_internal(job.submission_id)
        except Exception:
            return Response(
                text=traceback.format_exc(),
                status=aiohttp.web.HTTPInternalServerError.status_code,
            )

        return Response(
            text=json.dumps(dataclasses.asdict(resp)), content_type="application/json"
        )

    @routes.delete("/api/jobs/{job_or_submission_id}")
    async def delete_job(self, req: Request) -> Response:
        job_or_submission_id = req.match_info["job_or_submission_id"]
        job = await find_job_by_ids(
            self.gcs_client,
            self._job_info_client,
            job_or_submission_id,
        )
        if not job:
            return Response(
                text=f"Job {job_or_submission_id} does not exist",
                status=aiohttp.web.HTTPNotFound.status_code,
            )
        if job.type is not JobType.SUBMISSION:
            return Response(
                text="Can only delete submission type jobs",
                status=aiohttp.web.HTTPBadRequest.status_code,
            )

        try:
            job_agent_client = await self.get_target_agent()
            resp = await job_agent_client.delete_job_internal(job.submission_id)
        except Exception:
            return Response(
                text=traceback.format_exc(),
                status=aiohttp.web.HTTPInternalServerError.status_code,
            )

        return Response(
            text=json.dumps(dataclasses.asdict(resp)), content_type="application/json"
        )

    @staticmethod
    async def _parse_json_object_body(
        req: Request,
    ) -> Tuple[Optional[Dict], Optional[Response]]:
        if not req.can_read_body:
            return None, None
        raw_body = await req.read()
        if not raw_body:
            return None, None
        try:
            body = json.loads(raw_body)
        except Exception as exc:
            return None, Response(
                text=json.dumps(
                    {
                        "ok": False,
                        "error": {
                            "exc_type": "ValueError",
                            "exc_message": f"Invalid JSON request body: {exc}",
                        },
                    }
                ),
                content_type="application/json",
                status=aiohttp.web.HTTPBadRequest.status_code,
            )
        if not isinstance(body, dict):
            return None, Response(
                text=json.dumps(
                    {
                        "ok": False,
                        "error": {
                            "exc_type": "ValueError",
                            "exc_message": "Request body must be a JSON object.",
                        },
                    }
                ),
                content_type="application/json",
                status=aiohttp.web.HTTPBadRequest.status_code,
            )
        return body, None

    async def _proxy_actor_request(self, req: Request, subpath: str) -> StreamResponse:
        body, err_resp = await self._parse_json_object_body(req)
        if err_resp is not None:
            return err_resp
        params = dict(req.query) if req.query else None
        is_mutating = req.method in ("POST", "DELETE")
        stream = req.method == "POST"
        sresp: Optional[StreamResponse] = None
        try:
            agent = await self.get_target_agent()
            if not stream:
                status, text, content_type = await agent.forward_actor_request(
                    req.method, subpath, json_body=body, params=params
                )
                if is_mutating and status == 200:
                    self._last_client_actor_activity_at = max(
                        self._last_client_actor_activity_at,
                        datetime.now().timestamp(),
                    )
                return Response(text=text, status=status, content_type=content_type)
            # Relay the agent's response as it arrives: SSE frames from generators,
            # or keepalive whitespace followed by the JSON result of other calls.
            # Closing the relay early (on a client disconnect) closes the connection
            # to the agent, which cancels the call there.
            relay = agent.stream_actor_request(
                req.method, subpath, json_body=body, params=params, req=req
            )
            try:
                async for status, content_type, chunk in relay:
                    if sresp is None:
                        if status != 200:
                            return Response(
                                body=chunk,
                                status=status,
                                content_type=content_type.split(";")[0].strip(),
                            )
                        sresp = StreamResponse(
                            status=status,
                            headers={
                                "Content-Type": content_type,
                                "Cache-Control": "no-cache",
                            },
                        )
                        await sresp.prepare(req)
                    if chunk:
                        await sresp.write(chunk)
            finally:
                if is_mutating and sresp is not None:
                    self._last_client_actor_activity_at = max(
                        self._last_client_actor_activity_at,
                        datetime.now().timestamp(),
                    )
                await relay.aclose()
            await sresp.write_eof()
            return sresp
        except Exception:
            if sresp is not None:
                # The response has started, so it can only end. This is normally
                # the client or the agent disconnecting mid-response.
                logger.debug(
                    f"Actor request {req.method} {subpath} ended early", exc_info=True
                )
                return sresp
            logger.exception(f"Failed actor request {req.method} {subpath}")
            return Response(
                text=traceback.format_exc(),
                status=aiohttp.web.HTTPInternalServerError.status_code,
            )

    @routes.post("/api/actors", resp_type=ResponseType.STREAM)
    async def create_actor(self, req: Request) -> StreamResponse:
        return await self._proxy_actor_request(req, "")

    @routes.get("/api/actors")
    async def list_actors(self, req: Request) -> StreamResponse:
        return await self._proxy_actor_request(req, "")

    @routes.get("/api/actors/{actor_name}")
    async def get_actor(self, req: Request) -> StreamResponse:
        name = urllib.parse.quote(req.match_info["actor_name"], safe="")
        return await self._proxy_actor_request(req, f"/{name}")

    @routes.get("/api/actors/{actor_name}/logs")
    async def get_actor_logs(self, req: Request) -> StreamResponse:
        name = urllib.parse.quote(req.match_info["actor_name"], safe="")
        return await self._proxy_actor_request(req, f"/{name}/logs")

    @routes.delete("/api/actors/{actor_name}")
    async def delete_actor(self, req: Request) -> StreamResponse:
        name = urllib.parse.quote(req.match_info["actor_name"], safe="")
        return await self._proxy_actor_request(req, f"/{name}")

    @routes.post(
        "/api/actors/{actor_name}/methods/{method}",
        resp_type=ResponseType.STREAM,
    )
    async def call_actor_method(self, req: Request) -> StreamResponse:
        name = urllib.parse.quote(req.match_info["actor_name"], safe="")
        method = urllib.parse.quote(req.match_info["method"], safe="")
        return await self._proxy_actor_request(req, f"/{name}/methods/{method}")

    async def _proxy_cluster_request(self, req: Request, subpath: str) -> Response:
        body, err_resp = await self._parse_json_object_body(req)
        if err_resp is not None:
            return err_resp
        params = dict(req.query) if req.query else None
        try:
            agent = await self.get_target_agent()
            status, text, content_type = await agent.forward_actor_request(
                req.method,
                subpath,
                json_body=body,
                params=params,
                prefix="/api/job_agent/cluster",
                timeout=(
                    aiohttp.ClientTimeout(total=None) if req.method == "POST" else None
                ),
            )
            return Response(text=text, status=status, content_type=content_type)
        except Exception:
            logger.exception(f"Failed cluster request {req.method} {subpath}")
            return Response(
                text=traceback.format_exc(),
                status=aiohttp.web.HTTPInternalServerError.status_code,
            )

    @routes.get("/api/cluster/node_groups")
    async def list_node_groups(self, req: Request) -> Response:
        return await self._proxy_cluster_request(req, "/node_groups")

    @routes.get("/api/cluster/node_groups/{group}")
    async def get_node_group(self, req: Request) -> Response:
        group = urllib.parse.quote(req.match_info["group"], safe="")
        return await self._proxy_cluster_request(req, f"/node_groups/{group}")

    @routes.post(
        "/api/cluster/node_groups/{group}/nodes",
        resp_type=ResponseType.STREAM,
    )
    async def add_nodes(self, req: Request) -> Response:
        group = urllib.parse.quote(req.match_info["group"], safe="")
        return await self._proxy_cluster_request(req, f"/node_groups/{group}/nodes")

    @routes.get("/api/cluster/nodes")
    async def list_nodes(self, req: Request) -> Response:
        return await self._proxy_cluster_request(req, "/nodes")

    @routes.get("/api/cluster/nodes/{node_id}")
    async def get_node(self, req: Request) -> Response:
        node_id = urllib.parse.quote(req.match_info["node_id"], safe="")
        return await self._proxy_cluster_request(req, f"/nodes/{node_id}")

    @routes.delete("/api/cluster/nodes/{node_id}")
    async def destroy_node(self, req: Request) -> Response:
        node_id = urllib.parse.quote(req.match_info["node_id"], safe="")
        return await self._proxy_cluster_request(req, f"/nodes/{node_id}")

    @routes.get("/api/jobs/{job_or_submission_id}")
    async def get_job_info(self, req: Request) -> Response:
        job_or_submission_id = req.match_info["job_or_submission_id"]
        job = await find_job_by_ids(
            self.gcs_client,
            self._job_info_client,
            job_or_submission_id,
        )
        if not job:
            return Response(
                text=f"Job {job_or_submission_id} does not exist",
                status=aiohttp.web.HTTPNotFound.status_code,
            )

        return Response(
            text=json.dumps(_job_details_to_dict(job, should_redact_runtime_env(req))),
            content_type="application/json",
        )

    # TODO(rickyx): This endpoint's logic is also mirrored in state API's endpoint.
    # We should eventually unify the backend logic (and keep the logic in sync before
    # that).
    @routes.get("/api/jobs/")
    async def list_jobs(self, req: Request) -> Response:
        (driver_jobs, submission_job_drivers), submission_jobs = await asyncio.gather(
            get_driver_jobs(self.gcs_client), self._job_info_client.get_all_jobs()
        )

        submission_jobs = [
            JobDetails(
                **dataclasses.asdict(job),
                submission_id=submission_id,
                job_id=submission_job_drivers.get(submission_id).id
                if submission_id in submission_job_drivers
                else None,
                driver_info=submission_job_drivers.get(submission_id),
                type=JobType.SUBMISSION,
            )
            for submission_id, job in submission_jobs.items()
        ]
        redact = should_redact_runtime_env(req)
        return Response(
            text=json.dumps(
                [
                    *[
                        _job_details_to_dict(submission_job, redact)
                        for submission_job in submission_jobs
                    ],
                    *[
                        _job_details_to_dict(job_info, redact)
                        for job_info in driver_jobs.values()
                    ],
                ]
            ),
            content_type="application/json",
        )

    @routes.get("/api/jobs/{job_or_submission_id}/logs")
    async def get_job_logs(self, req: Request) -> Response:
        job_or_submission_id = req.match_info["job_or_submission_id"]
        job = await find_job_by_ids(
            self.gcs_client,
            self._job_info_client,
            job_or_submission_id,
        )
        if not job:
            return Response(
                text=f"Job {job_or_submission_id} does not exist",
                status=aiohttp.web.HTTPNotFound.status_code,
            )

        if job.type is not JobType.SUBMISSION:
            return Response(
                text="Can only get logs of submission type jobs",
                status=aiohttp.web.HTTPBadRequest.status_code,
            )

        try:
            job_agent_client = self.get_job_driver_agent_client(job)
            payload = (
                await job_agent_client.get_job_logs_internal(job.submission_id)
                if job_agent_client
                else JobLogsResponse("")
            )
            return Response(
                text=json.dumps(dataclasses.asdict(payload)),
                content_type="application/json",
            )
        except Exception:
            return Response(
                text=traceback.format_exc(),
                status=aiohttp.web.HTTPInternalServerError.status_code,
            )

    @routes.get(
        "/api/jobs/{job_or_submission_id}/logs/tail", resp_type=ResponseType.WEBSOCKET
    )
    async def tail_job_logs(self, req: Request) -> StreamResponse:
        job_or_submission_id = req.match_info["job_or_submission_id"]
        job = await find_job_by_ids(
            self.gcs_client,
            self._job_info_client,
            job_or_submission_id,
        )
        if not job:
            return Response(
                text=f"Job {job_or_submission_id} does not exist",
                status=aiohttp.web.HTTPNotFound.status_code,
            )

        if job.type is not JobType.SUBMISSION:
            return Response(
                text="Can only get logs of submission type jobs",
                status=aiohttp.web.HTTPBadRequest.status_code,
            )

        ws = aiohttp.web.WebSocketResponse()
        await ws.prepare(req)

        driver_agent_http_address = None
        while driver_agent_http_address is None:
            job = await find_job_by_ids(
                self.gcs_client,
                self._job_info_client,
                job_or_submission_id,
            )
            driver_agent_http_address = job.driver_agent_http_address
            status = job.status
            if status.is_terminal() and driver_agent_http_address is None:
                # Job exited before supervisor actor started.
                return ws

            await asyncio.sleep(self.WAIT_FOR_SUPERVISOR_ACTOR_INTERVAL_S)

        job_agent_client = self.get_job_driver_agent_client(job)

        async for lines in job_agent_client.tail_job_logs(job.submission_id):
            await ws.send_str(lines)

        return ws

    def get_job_driver_agent_client(
        self, job: JobDetails
    ) -> Optional[JobAgentSubmissionClient]:
        if job.driver_agent_http_address is None:
            return None

        driver_node_id = job.driver_node_id
        if driver_node_id not in self._agents:
            self._agents[driver_node_id] = JobAgentSubmissionClient(
                job.driver_agent_http_address
            )

        return self._agents[driver_node_id]

    @routes.get("/api/component_activities")
    async def get_component_activities(
        self, req: aiohttp.web.Request
    ) -> aiohttp.web.Response:
        timeout = req.query.get("timeout", None)
        if timeout and timeout.isdigit():
            timeout = int(timeout)
        else:
            timeout = 30

        # Get activity information for driver
        driver_activity_info = await self._get_job_activity_info(timeout=timeout)
        resp = {"driver": dict(driver_activity_info)}

        if RAY_CLUSTER_ACTIVITY_HOOK in os.environ:
            try:
                cluster_activity_callable = load_class(
                    os.environ[RAY_CLUSTER_ACTIVITY_HOOK]
                )
                external_activity_output = cluster_activity_callable()
                assert isinstance(external_activity_output, dict), (
                    f"Output of hook {os.environ[RAY_CLUSTER_ACTIVITY_HOOK]} "
                    "should be Dict[str, RayActivityResponse]. Got "
                    f"output: {external_activity_output}"
                )
                for component_type in external_activity_output:
                    try:
                        component_activity_output = external_activity_output[
                            component_type
                        ]
                        # Parse and validate output to type RayActivityResponse
                        component_activity_output = RayActivityResponse(
                            **dict(component_activity_output)
                        )
                        resp[component_type] = dict(component_activity_output)
                    except Exception as e:
                        logger.exception(
                            f"Failed to get activity status of {component_type} "
                            f"from user hook {os.environ[RAY_CLUSTER_ACTIVITY_HOOK]}."
                        )
                        resp[component_type] = {
                            "is_active": RayActivityStatus.ERROR,
                            "reason": repr(e),
                            "timestamp": datetime.now().timestamp(),
                        }
            except Exception as e:
                logger.exception(
                    "Failed to get activity status from user "
                    f"hook {os.environ[RAY_CLUSTER_ACTIVITY_HOOK]}."
                )
                resp["external_component"] = {
                    "is_active": RayActivityStatus.ERROR,
                    "reason": repr(e),
                    "timestamp": datetime.now().timestamp(),
                }

        return aiohttp.web.Response(
            text=json.dumps(resp),
            content_type="application/json",
            status=aiohttp.web.HTTPOk.status_code,
        )

    async def _list_client_actors(self, timeout: float) -> List[Dict]:
        """Lists the live `/api/actors` actors, or returns [] if the agent can't."""

        async def _list() -> List[Dict]:
            agent = await self.get_target_agent(timeout)
            status, text, _ = await agent.forward_actor_request("GET", "")
            actors = json.loads(text) if status == 200 else []
            return [a for a in actors if isinstance(a, dict)]

        try:
            return await asyncio.wait_for(_list(), timeout=timeout)
        except Exception as e:
            logger.warning(f"Failed to list client actors for cluster activity: {e!r}")
            return []

    async def _get_job_activity_info(self, timeout: int) -> RayActivityResponse:
        # Returns if there is Ray activity from drivers (job) or from client actors
        # served by `/api/actors`.
        # Drivers in namespaces that start with _ray_internal_ are not
        # considered activity.
        # This includes the _ray_internal_dashboard job that gets automatically
        # created with every cluster
        try:
            reply = await self.gcs_client.async_get_all_job_info(
                skip_submission_job_info_field=True,
                skip_is_running_tasks_field=True,
                timeout=timeout,
            )

            num_active_drivers = 0
            latest_job_end_time = 0
            dashboard_driver_seen = False
            for job_table_entry in reply.values():
                is_dead = bool(job_table_entry.is_dead)
                namespace = job_table_entry.config.ray_namespace
                in_internal_namespace = namespace.startswith("_ray_internal_")
                dashboard_driver_seen |= namespace == RAY_INTERNAL_DASHBOARD_NAMESPACE
                latest_job_end_time = (
                    max(latest_job_end_time, job_table_entry.end_time)
                    if job_table_entry.end_time
                    else latest_job_end_time
                )
                if not is_dead and not in_internal_namespace:
                    num_active_drivers += 1

            current_timestamp = datetime.now().timestamp()
            # Latest job end time must be before or equal to the current timestamp.
            # Job end times may be provided in epoch milliseconds. Check if this
            # is true, and convert to seconds
            if latest_job_end_time > current_timestamp:
                latest_job_end_time = latest_job_end_time / 1000
                assert current_timestamp >= latest_job_end_time, (
                    f"Most recent job end time {latest_job_end_time} must be "
                    f"before or equal to the current timestamp {current_timestamp}"
                )

            # `/api/actors` root actors are created by the head node's dashboard
            # agent from its `_ray_internal_dashboard` driver, so they keep
            # the cluster busy without any user driver. Ask the agent only if such a
            # driver has existed, so that this probe never makes an idle agent
            # connect to Ray.
            client_actors = []
            if dashboard_driver_seen:
                client_actors = await self._list_client_actors(timeout)
            for actor in client_actors:
                # An actor with a call in flight is active right now.
                seen = (
                    current_timestamp
                    if actor.get("active_calls")
                    else actor.get("last_active")
                )
                if isinstance(seen, (int, float)):
                    self._last_client_actor_activity_at = max(
                        self._last_client_actor_activity_at,
                        min(seen, current_timestamp),
                    )

            reasons = []
            if num_active_drivers:
                reasons.append(f"Number of active drivers: {num_active_drivers}")
            if client_actors:
                names = sorted(str(a.get("name")) for a in client_actors)
                shown = ", ".join(names[:10])
                if len(names) > 10:
                    shown += f", and {len(names) - 10} more"
                reasons.append(f"Number of live client actors: {len(names)} ({shown})")
            is_active = (
                RayActivityStatus.ACTIVE
                if num_active_drivers > 0 or client_actors
                else RayActivityStatus.INACTIVE
            )
            last_activity_at = max(
                latest_job_end_time, self._last_client_actor_activity_at
            )
            return RayActivityResponse(
                is_active=is_active,
                reason="; ".join(reasons) or None,
                timestamp=current_timestamp,
                # If no job has finished and no client actor has been active yet,
                # don't populate last_activity_at.
                last_activity_at=last_activity_at if last_activity_at else None,
            )
        except Exception as e:
            logger.exception("Failed to get activity status of Ray drivers.")
            return RayActivityResponse(
                is_active=RayActivityStatus.ERROR,
                reason=repr(e),
                timestamp=datetime.now().timestamp(),
            )

    async def run(self):
        await super().run()
        if not self._job_info_client:
            self._job_info_client = JobInfoStorageClient(self.gcs_client)
