import asyncio
import contextlib
import dataclasses
import json
import logging
import traceback
from typing import Optional

import aiohttp
from aiohttp.web import Request, Response

import ray
import ray.dashboard.optional_utils as optional_utils
import ray.dashboard.utils as dashboard_utils
from ray.dashboard.modules.job.actor_manager import ActorManager
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

    @routes.post("/api/job_agent/actors")
    @optional_utils.init_ray_and_catch_exceptions()
    async def create_actor(self, req: Request) -> Response:
        spec = await req.json()
        res = await self.get_actor_manager().create_actor(spec)
        status = (
            aiohttp.web.HTTPOk.status_code
            if res.get("ok", True)
            else aiohttp.web.HTTPBadRequest.status_code
        )
        return Response(
            text=json.dumps(res), content_type="application/json", status=status
        )

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
        no_restart = req.query.get("no_restart", "1") in ("1", "true", "True")
        res = await self.get_actor_manager().kill_actor(name, no_restart=no_restart)
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
        body = await req.json() if req.can_read_body else {}
        actor_mgr = self.get_actor_manager()
        meta = await actor_mgr._lookup_actor(name)
        if meta is None:
            return _actor_not_found_response(name)
        stream = (
            req.query.get("stream", "0") in ("1", "true", "True")
            or bool(body.get("stream", False))
            or method in meta.get("generator_methods", ())
        )
        if stream:
            sresp = aiohttp.web.StreamResponse(
                status=aiohttp.web.HTTPOk.status_code,
                headers={
                    "Content-Type": "text/event-stream",
                    "Cache-Control": "no-cache",
                },
            )
            await sresp.prepare(req)
            agen = actor_mgr.stream_actor_method(name, method, body)
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
                    finally:
                        next_item_task = None
                    if item is None:
                        await sresp.write(b": ping\n\n")
                    else:
                        frame = f"data: {json.dumps(item)}\n\n".encode("utf-8")
                        await sresp.write(frame)
                if req.transport is not None and not req.transport.is_closing():
                    await sresp.write_eof()
            except (
                ConnectionResetError,
                aiohttp.ClientConnectionError,
                asyncio.TimeoutError,
                TimeoutError,
            ):
                pass
            finally:
                if next_item_task is not None and not next_item_task.done():
                    next_item_task.cancel()
                    with contextlib.suppress(Exception, asyncio.CancelledError):
                        await next_item_task
                with contextlib.suppress(Exception):
                    await agen.aclose()
            return sresp

        # Commit to a 200 JSON response up front, then write a single space every
        # second until the call returns. JSON parsers skip leading whitespace, and
        # the bytes keep proxies and load balancers from timing out an idle
        # connection. The same ticks notice a client disconnect, which cancels the
        # call (and reaps an ephemeral actor).
        sresp = aiohttp.web.StreamResponse(
            status=aiohttp.web.HTTPOk.status_code,
            headers={"Content-Type": "application/json", "Cache-Control": "no-cache"},
        )
        await sresp.prepare(req)
        call_task = asyncio.ensure_future(
            actor_mgr.call_actor_method(name, method, body)
        )
        try:
            while True:
                done, _ = await asyncio.wait({call_task}, timeout=1.0)
                if done:
                    break
                if req.transport is None or req.transport.is_closing():
                    return sresp
                await sresp.write(b" ")
            # None means a concurrent DELETE removed the actor after the lookup.
            res = call_task.result() or _actor_not_found(name)
            await sresp.write(json.dumps(res).encode("utf-8"))
            await sresp.write_eof()
        except (ConnectionResetError, aiohttp.ClientConnectionError):
            pass
        finally:
            if not call_task.done():
                call_task.cancel()
                with contextlib.suppress(Exception, asyncio.CancelledError):
                    await call_task
        return sresp

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
