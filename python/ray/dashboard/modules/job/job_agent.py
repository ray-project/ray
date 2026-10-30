import asyncio
import contextlib
import dataclasses
import json
import logging
import traceback
from typing import Any, Dict, Optional, Tuple

import aiohttp
from aiohttp.web import Request, Response

import ray
import ray.dashboard.optional_utils as optional_utils
import ray.dashboard.utils as dashboard_utils
from ray.dashboard.modules.job.actor_manager import (
    ActorManager,
    MaxNodesExceededError,
)
from ray.dashboard.modules.job.common import (
    JobDeleteResponse,
    JobLogsResponse,
    JobStopResponse,
    JobSubmitRequest,
    JobSubmitResponse,
)
from ray.dashboard.modules.job.job_manager import JobManager
from ray.dashboard.modules.job.pydantic_models import JobType
from ray.dashboard.modules.job.utils import find_job_by_ids, parse_and_validate_request

routes = optional_utils.DashboardAgentRouteTable
logger = logging.getLogger(__name__)


def _actor_not_found(name: str) -> dict:
    return {
        "ok": False,
        "error": {
            "exc_type": "KeyError",
            "exc_message": f"Actor '{name}' does not exist.",
        },
    }


def _actor_not_found_response(name: str) -> Response:
    return Response(
        text=json.dumps(_actor_not_found(name)),
        content_type="application/json",
        status=aiohttp.web.HTTPNotFound.status_code,
    )


def _bad_request_json(exc_type: str, exc_message: str) -> Response:
    return Response(
        text=json.dumps(
            {
                "ok": False,
                "error": {
                    "exc_type": exc_type,
                    "exc_message": exc_message,
                },
            }
        ),
        content_type="application/json",
        status=aiohttp.web.HTTPBadRequest.status_code,
    )


async def _parse_json_object_body(
    req: Request, *, required: bool = False
) -> Tuple[Optional[Dict[str, Any]], Optional[Response]]:
    raw_body = (await req.read()) if req.can_read_body else b""
    if not raw_body:
        if required:
            return None, _bad_request_json(
                "ValueError", "Request body is required and must be a JSON object."
            )
        return {}, None
    try:
        body = json.loads(raw_body)
    except Exception as exc:
        return None, _bad_request_json(
            "ValueError", f"Invalid JSON request body: {exc}"
        )
    if not isinstance(body, dict):
        return None, _bad_request_json(
            "ValueError", "Request body must be a JSON object."
        )
    return body, None


def _cluster_response(fn: Any, *args: Any, **kwargs: Any) -> Response:
    try:
        res = fn(*args, **kwargs)
        return Response(text=json.dumps(res), content_type="application/json")
    except Exception as exc:
        exc_name = type(exc).__name__
        if isinstance(exc, NotImplementedError):
            return Response(
                text=json.dumps(
                    {
                        "ok": False,
                        "error": {"exc_type": exc_name, "exc_message": str(exc)},
                    }
                ),
                content_type="application/json",
                status=aiohttp.web.HTTPNotImplemented.status_code,
            )
        if isinstance(exc, KeyError) or exc_name.endswith("NotFoundError"):
            msg = exc.args[0] if isinstance(exc, KeyError) and exc.args else str(exc)
            return Response(
                text=json.dumps(
                    {
                        "ok": False,
                        "error": {"exc_type": exc_name, "exc_message": msg},
                    }
                ),
                content_type="application/json",
                status=aiohttp.web.HTTPNotFound.status_code,
            )
        if isinstance(exc, (TimeoutError, asyncio.TimeoutError)) or exc_name in (
            "TimeoutError",
            "FutureTimeoutError",
        ):
            return Response(
                text=json.dumps(
                    {
                        "ok": False,
                        "error": {"exc_type": exc_name, "exc_message": str(exc)},
                    }
                ),
                content_type="application/json",
                status=aiohttp.web.HTTPRequestTimeout.status_code,
            )
        if (
            isinstance(exc, MaxNodesExceededError)
            or exc_name == "MaxNodesExceededError"
        ):
            return Response(
                text=json.dumps(
                    {
                        "ok": False,
                        "error": {"exc_type": exc_name, "exc_message": str(exc)},
                    }
                ),
                content_type="application/json",
                status=aiohttp.web.HTTPConflict.status_code,
            )
        if isinstance(exc, ValueError) or exc_name.endswith("DiedError"):
            return _bad_request_json(exc_name, str(exc))
        raise


class JobAgent(dashboard_utils.DashboardAgentModule):
    def __init__(self, dashboard_agent):
        super().__init__(dashboard_agent)
        self._job_manager = None
        self._actor_manager = None

    def get_actor_manager(self) -> ActorManager:
        if not self._actor_manager:
            self._actor_manager = ActorManager()
        return self._actor_manager

    @routes.post("/api/job_agent/jobs/")
    @optional_utils.init_ray_and_catch_exceptions()
    async def submit_job(self, req: Request) -> Response:
        result = await parse_and_validate_request(req, JobSubmitRequest)
        # Request parsing failed, returned with Response object.
        if isinstance(result, Response):
            return result
        else:
            submit_request = result

        request_submission_id = submit_request.submission_id or submit_request.job_id
        try:
            ray._common.usage.usage_lib.record_library_usage("job_submission")
            submission_id = await self.get_job_manager().submit_job(
                entrypoint=submit_request.entrypoint,
                submission_id=request_submission_id,
                runtime_env=submit_request.runtime_env,
                metadata=submit_request.metadata,
                entrypoint_num_cpus=submit_request.entrypoint_num_cpus,
                entrypoint_num_gpus=submit_request.entrypoint_num_gpus,
                entrypoint_memory=submit_request.entrypoint_memory,
                entrypoint_resources=submit_request.entrypoint_resources,
                entrypoint_label_selector=submit_request.entrypoint_label_selector,
            )

            resp = JobSubmitResponse(job_id=submission_id, submission_id=submission_id)
        except (TypeError, ValueError):
            return Response(
                text=traceback.format_exc(),
                status=aiohttp.web.HTTPBadRequest.status_code,
            )
        except Exception:
            return Response(
                text=traceback.format_exc(),
                status=aiohttp.web.HTTPInternalServerError.status_code,
            )

        return Response(
            text=json.dumps(dataclasses.asdict(resp)),
            content_type="application/json",
            status=aiohttp.web.HTTPOk.status_code,
        )

    @routes.post("/api/job_agent/jobs/{job_or_submission_id}/stop")
    @optional_utils.init_ray_and_catch_exceptions()
    async def stop_job(self, req: Request) -> Response:
        job_or_submission_id = req.match_info["job_or_submission_id"]
        job = await find_job_by_ids(
            self._dashboard_agent.gcs_client,
            self.get_job_manager().job_info_client(),
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
            stopped = self.get_job_manager().stop_job(job.submission_id)
            resp = JobStopResponse(stopped=stopped)
        except Exception:
            return Response(
                text=traceback.format_exc(),
                status=aiohttp.web.HTTPInternalServerError.status_code,
            )

        return Response(
            text=json.dumps(dataclasses.asdict(resp)), content_type="application/json"
        )

    @routes.delete("/api/job_agent/jobs/{job_or_submission_id}")
    @optional_utils.init_ray_and_catch_exceptions()
    async def delete_job(self, req: Request) -> Response:
        job_or_submission_id = req.match_info["job_or_submission_id"]
        job = await find_job_by_ids(
            self._dashboard_agent.gcs_client,
            self.get_job_manager().job_info_client(),
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
            deleted = await self.get_job_manager().delete_job(job.submission_id)
            resp = JobDeleteResponse(deleted=deleted)
        except Exception:
            return Response(
                text=traceback.format_exc(),
                status=aiohttp.web.HTTPInternalServerError.status_code,
            )

        return Response(
            text=json.dumps(dataclasses.asdict(resp)), content_type="application/json"
        )

    @routes.get("/api/job_agent/jobs/{job_or_submission_id}/logs")
    @optional_utils.init_ray_and_catch_exceptions()
    async def get_job_logs(self, req: Request) -> Response:
        job_or_submission_id = req.match_info["job_or_submission_id"]
        job = await find_job_by_ids(
            self._dashboard_agent.gcs_client,
            self.get_job_manager().job_info_client(),
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

        resp = JobLogsResponse(
            logs=self.get_job_manager().get_job_logs(job.submission_id)
        )
        return Response(
            text=json.dumps(dataclasses.asdict(resp)), content_type="application/json"
        )

    @routes.get("/api/job_agent/jobs/{job_or_submission_id}/logs/tail")
    @optional_utils.init_ray_and_catch_exceptions()
    async def tail_job_logs(self, req: Request) -> Response:
        job_or_submission_id = req.match_info["job_or_submission_id"]
        job = await find_job_by_ids(
            self._dashboard_agent.gcs_client,
            self.get_job_manager().job_info_client(),
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

        async for lines in self._job_manager.tail_job_logs(job.submission_id):
            await ws.send_str(lines)

        return ws

    async def _stream_json_with_keepalive(
        self,
        req: Request,
        coro: Any,
        *,
        not_found_name: Optional[str] = None,
    ) -> aiohttp.web.StreamResponse:
        # Commit to a 200 JSON response up front, then write a single space every
        # second until the coroutine returns. JSON parsers skip leading whitespace,
        # and the bytes keep proxies and load balancers from timing out an idle
        # connection. Checking the transport notices a client disconnect, which
        # cancels the coroutine (cleaning up an in-progress creation or reaping a
        # one-shot run actor).
        sresp = aiohttp.web.StreamResponse(
            status=aiohttp.web.HTTPOk.status_code,
            headers={"Content-Type": "application/json", "Cache-Control": "no-cache"},
        )
        await sresp.prepare(req)
        call_task = asyncio.ensure_future(coro)
        ticks = 0
        try:
            await sresp.write(b" ")
            while True:
                done, _ = await asyncio.wait({call_task}, timeout=0.1)
                if done:
                    break
                if req.transport is None or req.transport.is_closing():
                    return sresp
                ticks += 1
                if ticks % 10 == 0:
                    await sresp.write(b" ")
            try:
                res = call_task.result()
            except Exception as exc:
                logger.exception("Unhandled error in streaming JSON actor request")
                res = {
                    "ok": False,
                    "error": {
                        "exc_type": type(exc).__name__,
                        "exc_message": str(exc),
                        "traceback": traceback.format_exc(),
                    },
                }
            if res is None and not_found_name is not None:
                res = _actor_not_found(not_found_name)
            await sresp.write(json.dumps(res).encode("utf-8"))
            if req.transport is not None and not req.transport.is_closing():
                await sresp.write_eof()
        except (ConnectionResetError, aiohttp.ClientConnectionError):
            pass
        finally:
            if not call_task.done():
                call_task.cancel()
                with contextlib.suppress(BaseException):
                    await call_task
        return sresp

    @routes.post("/api/job_agent/actors")
    @optional_utils.init_ray_and_catch_exceptions()
    async def create_actor(self, req: Request) -> aiohttp.web.StreamResponse:
        spec, err_resp = await _parse_json_object_body(req, required=True)
        if err_resp is not None:
            return err_resp
        actor_mgr = self.get_actor_manager()
        try:
            actor_mgr.validate_actor_spec(spec)
        except ValueError as exc:
            return _bad_request_json("ValueError", str(exc))
        return await self._stream_json_with_keepalive(req, actor_mgr.create_actor(spec))

    @routes.get("/api/job_agent/actors")
    @optional_utils.init_ray_and_catch_exceptions()
    async def list_actors(self, req: Request) -> Response:
        actors = await self.get_actor_manager().list_actors()
        return Response(text=json.dumps(actors), content_type="application/json")

    @routes.get("/api/job_agent/actors/{actor_name}")
    @optional_utils.init_ray_and_catch_exceptions()
    async def get_actor(self, req: Request) -> Response:
        name = req.match_info["actor_name"]
        info = await self.get_actor_manager().get_actor_info(name)
        if info is None or not info.get("ok", True):
            return Response(
                text=json.dumps(info or _actor_not_found(name)),
                content_type="application/json",
                status=aiohttp.web.HTTPNotFound.status_code,
            )
        return Response(text=json.dumps(info), content_type="application/json")

    @routes.get("/api/job_agent/actors/{actor_name}/logs")
    @optional_utils.init_ray_and_catch_exceptions()
    async def get_actor_logs(self, req: Request) -> Response:
        name = req.match_info["actor_name"]
        res = await self.get_actor_manager().get_actor_logs(name)
        if res is None:
            return _actor_not_found_response(name)
        return Response(text=json.dumps(res), content_type="application/json")

    @routes.delete("/api/job_agent/actors/{actor_name}")
    @optional_utils.init_ray_and_catch_exceptions()
    async def delete_actor(self, req: Request) -> Response:
        name = req.match_info["actor_name"]
        res = await self.get_actor_manager().kill_actor(name)
        if res is None or not res.get("killed"):
            return Response(
                text=json.dumps({"ok": False, "killed": False, "name": name}),
                content_type="application/json",
                status=aiohttp.web.HTTPNotFound.status_code,
            )
        return Response(text=json.dumps(res), content_type="application/json")

    @routes.post("/api/job_agent/actors/{actor_name}/methods/{method}")
    @optional_utils.init_ray_and_catch_exceptions()
    async def call_actor_method(self, req: Request) -> aiohttp.web.StreamResponse:
        name = req.match_info["actor_name"]
        method = req.match_info["method"]
        body, err_resp = await _parse_json_object_body(req)
        if err_resp is not None:
            return err_resp
        raw_query_stream = req.query.get("stream")
        if raw_query_stream is not None and raw_query_stream.lower() not in (
            "0",
            "1",
            "false",
            "true",
        ):
            return _bad_request_json(
                "ValueError",
                (
                    f"Invalid stream query parameter {raw_query_stream!r}; "
                    "expected '0', '1', 'true', or 'false'."
                ),
            )
        body_stream = body.get("stream", False)
        if body_stream is not None and not isinstance(body_stream, bool):
            return _bad_request_json(
                "ValueError",
                f"stream must be a bool, got {body_stream!r}.",
            )
        actor_mgr = self.get_actor_manager()
        meta = await actor_mgr._lookup_actor(name)
        if meta is None:
            return _actor_not_found_response(name)
        stream = (
            (raw_query_stream is not None and raw_query_stream.lower() in ("1", "true"))
            or bool(body_stream)
            or method in meta.get("generator_methods", ())
        )
        if stream:
            sresp = aiohttp.web.StreamResponse(
                status=aiohttp.web.HTTPOk.status_code,
                headers={
                    "Content-Type": "text/event-stream; charset=utf-8",
                    "Cache-Control": "no-cache",
                },
            )
            await sresp.prepare(req)
            agen = actor_mgr.stream_actor_method(name, method, body, meta=meta)
            ait = agen.__aiter__()
            next_item_task: Optional[asyncio.Task] = None
            try:
                while True:
                    if req.transport is None or req.transport.is_closing():
                        break
                    if next_item_task is None:
                        next_item_task = asyncio.ensure_future(ait.__anext__())
                    done, _ = await asyncio.wait({next_item_task}, timeout=0.05)
                    if not done:
                        continue
                    try:
                        item = next_item_task.result()
                    except StopAsyncIteration:
                        break
                    except Exception as exc:
                        logger.exception("Unhandled error in SSE actor stream")
                        err_item = {
                            "ok": False,
                            "error": {
                                "exc_type": type(exc).__name__,
                                "exc_message": str(exc),
                                "traceback": traceback.format_exc(),
                            },
                        }
                        frame = f"data: {json.dumps(err_item)}\n\n".encode("utf-8")
                        await sresp.write(frame)
                        break
                    finally:
                        next_item_task = None
                    if item is None:
                        await sresp.write(b": ping\n\n")
                    else:
                        frame = f"data: {json.dumps(item)}\n\n".encode("utf-8")
                        await sresp.write(frame)
                if req.transport is not None and not req.transport.is_closing():
                    await sresp.write_eof()
            except (ConnectionResetError, aiohttp.ClientConnectionError):
                pass
            finally:
                if next_item_task is not None and not next_item_task.done():
                    next_item_task.cancel()
                    with contextlib.suppress(BaseException):
                        await next_item_task
                with contextlib.suppress(Exception):
                    await agen.aclose()
            return sresp

        return await self._stream_json_with_keepalive(
            req,
            actor_mgr.call_actor_method(name, method, body, meta=meta),
            not_found_name=name,
        )

    @routes.get("/api/job_agent/cluster/node_groups")
    @optional_utils.init_ray_and_catch_exceptions()
    async def list_node_groups(self, req: Request) -> Response:
        return _cluster_response(self.get_actor_manager().list_node_groups)

    @routes.get("/api/job_agent/cluster/node_groups/{group}")
    @optional_utils.init_ray_and_catch_exceptions()
    async def get_node_group(self, req: Request) -> Response:
        group = req.match_info["group"]
        return _cluster_response(self.get_actor_manager().get_node_group, group)

    @routes.post("/api/job_agent/cluster/node_groups/{group}/nodes")
    @optional_utils.init_ray_and_catch_exceptions()
    async def add_nodes(self, req: Request) -> Response:
        group = req.match_info["group"]
        body, err_resp = await _parse_json_object_body(req)
        if err_resp is not None:
            return err_resp
        wait_ready = body.get("wait_ready", False)
        if wait_ready is True:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(
                None,
                lambda: _cluster_response(
                    self.get_actor_manager().add_nodes,
                    group,
                    count=body.get("count", 1),
                    wait_ready=True,
                    timeout_s=body.get("timeout_s"),
                ),
            )
        return _cluster_response(
            self.get_actor_manager().add_nodes,
            group,
            count=body.get("count", 1),
            wait_ready=wait_ready,
            timeout_s=body.get("timeout_s"),
        )

    @routes.get("/api/job_agent/cluster/nodes")
    @optional_utils.init_ray_and_catch_exceptions()
    async def list_nodes(self, req: Request) -> Response:
        raw_include_dead = req.query.get("include_dead", "false").lower()
        if raw_include_dead not in ("0", "1", "false", "true"):
            return _bad_request_json(
                "ValueError",
                (
                    f"Invalid include_dead query parameter "
                    f"{req.query.get('include_dead')!r}; expected 'true' or 'false'."
                ),
            )
        include_dead = raw_include_dead in ("1", "true")
        return _cluster_response(
            self.get_actor_manager().list_nodes,
            state=req.query.get("state"),
            node_group=req.query.get("node_group"),
            include_dead=include_dead,
        )

    @routes.get("/api/job_agent/cluster/nodes/{node_id}")
    @optional_utils.init_ray_and_catch_exceptions()
    async def get_node(self, req: Request) -> Response:
        node_id = req.match_info["node_id"]
        return _cluster_response(self.get_actor_manager().get_node, node_id)

    @routes.delete("/api/job_agent/cluster/nodes/{node_id}")
    @optional_utils.init_ray_and_catch_exceptions()
    async def destroy_node(self, req: Request) -> Response:
        node_id = req.match_info["node_id"]
        body, err_resp = await _parse_json_object_body(req)
        if err_resp is not None:
            return err_resp
        return _cluster_response(
            self.get_actor_manager().destroy_node,
            node_id,
            reason=body.get("reason"),
            grace_period_s=body.get("grace_period_s"),
        )

    def get_job_manager(self):
        if not self._job_manager:
            self._job_manager = JobManager(
                self._dashboard_agent.gcs_client, self._dashboard_agent.log_dir
            )
        return self._job_manager

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return False
