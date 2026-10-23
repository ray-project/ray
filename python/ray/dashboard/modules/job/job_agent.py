import dataclasses
import json
import logging
import traceback

import aiohttp
from aiohttp.web import Request, Response

import ray
import ray.dashboard.optional_utils as optional_utils
import ray.dashboard.utils as dashboard_utils
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
from ray.dashboard.modules.job.workspace_manager import WorkspaceManager

routes = optional_utils.DashboardAgentRouteTable
logger = logging.getLogger(__name__)


class JobAgent(dashboard_utils.DashboardAgentModule):
    def __init__(self, dashboard_agent):
        super().__init__(dashboard_agent)
        self._job_manager = None
        self._workspace_manager = None

    def get_workspace_manager(self) -> WorkspaceManager:
        if not self._workspace_manager:
            self._workspace_manager = WorkspaceManager()
        return self._workspace_manager

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

    @routes.get("/api/job_agent/workspaces")
    @optional_utils.init_ray_and_catch_exceptions()
    async def list_workspaces(self, req: Request) -> Response:
        items = await self.get_workspace_manager().list_workspaces()
        return Response(text=json.dumps(items), content_type="application/json")

    @routes.post("/api/job_agent/workspaces")
    @optional_utils.init_ray_and_catch_exceptions()
    async def create_or_update_workspace(self, req: Request) -> Response:
        body = await req.json()
        ws = body.get("workspace")
        if not ws or not isinstance(ws, str):
            return Response(
                text="Field 'workspace' must be a non-empty string",
                status=aiohttp.web.HTTPBadRequest.status_code,
            )
        info = await self.get_workspace_manager().get_or_create_workspace(
            workspace=ws,
            ttl_seconds=body.get("ttl_seconds", 1800.0),
            runtime_env=body.get("runtime_env"),
            update_ttl="ttl_seconds" in body,
        )
        return Response(text=json.dumps(info), content_type="application/json")

    @routes.get("/api/job_agent/workspaces/{workspace}")
    @optional_utils.init_ray_and_catch_exceptions()
    async def get_workspace_info(self, req: Request) -> Response:
        ws = req.match_info["workspace"]
        info = await self.get_workspace_manager().get_workspace(ws)
        if info is None:
            return Response(
                text=f"Workspace '{ws}' does not exist",
                status=aiohttp.web.HTTPNotFound.status_code,
            )
        return Response(text=json.dumps(info), content_type="application/json")

    @routes.post("/api/job_agent/workspaces/{workspace}/heartbeat")
    @optional_utils.init_ray_and_catch_exceptions()
    async def workspace_heartbeat(self, req: Request) -> Response:
        ws = req.match_info["workspace"]
        body = await req.json() if req.can_read_body else {}
        info = await self.get_workspace_manager().heartbeat_workspace(
            ws,
            ttl_seconds=body.get("ttl_seconds"),
            update_ttl="ttl_seconds" in body,
        )
        if info is None:
            return Response(
                text=f"Workspace '{ws}' does not exist",
                status=aiohttp.web.HTTPNotFound.status_code,
            )
        return Response(text=json.dumps(info), content_type="application/json")

    @routes.delete("/api/job_agent/workspaces/{workspace}")
    @optional_utils.init_ray_and_catch_exceptions()
    async def close_workspace(self, req: Request) -> Response:
        ws = req.match_info["workspace"]
        kill_detached = req.query.get("kill_detached", "0") in ("1", "true", "True")
        summary = await self.get_workspace_manager().delete_workspace(
            ws, kill_detached=kill_detached
        )
        if summary is None:
            return Response(
                text=f"Workspace '{ws}' does not exist",
                status=aiohttp.web.HTTPNotFound.status_code,
            )
        return Response(text=json.dumps(summary), content_type="application/json")

    @routes.post("/api/job_agent/workspaces/{workspace}/tasks")
    @optional_utils.init_ray_and_catch_exceptions()
    async def run_workspace_task(self, req: Request) -> aiohttp.web.StreamResponse:
        ws = req.match_info["workspace"]
        spec = await req.json()
        stream = req.query.get("stream", "0") in ("1", "true", "True") or bool(
            spec.get("stream", False)
        )
        if stream:
            sresp = aiohttp.web.StreamResponse(
                status=aiohttp.web.HTTPOk.status_code,
                headers={"Content-Type": "application/x-ndjson"},
            )
            await sresp.prepare(req)
            async for item in self.get_workspace_manager().stream_task(ws, spec):
                await sresp.write((json.dumps(item) + "\n").encode("utf-8"))
                await sresp.drain()
            await sresp.write_eof()
            return sresp

        res = await self.get_workspace_manager().run_task(ws, spec)
        status = (
            aiohttp.web.HTTPOk.status_code
            if res.get("ok", True)
            else aiohttp.web.HTTPBadRequest.status_code
        )
        return Response(
            text=json.dumps(res), content_type="application/json", status=status
        )

    @routes.post("/api/job_agent/workspaces/{workspace}/actors")
    @optional_utils.init_ray_and_catch_exceptions()
    async def create_workspace_actor(self, req: Request) -> Response:
        ws = req.match_info["workspace"]
        spec = await req.json()
        res = await self.get_workspace_manager().create_actor(ws, spec)
        status = (
            aiohttp.web.HTTPOk.status_code
            if res.get("ok", True)
            else aiohttp.web.HTTPBadRequest.status_code
        )
        return Response(
            text=json.dumps(res), content_type="application/json", status=status
        )

    @routes.get("/api/job_agent/workspaces/{workspace}/actors")
    @optional_utils.init_ray_and_catch_exceptions()
    async def list_workspace_actors(self, req: Request) -> Response:
        ws = req.match_info["workspace"]
        actors = await self.get_workspace_manager().list_actors(ws)
        if actors is None:
            return Response(
                text=f"Workspace '{ws}' does not exist",
                status=aiohttp.web.HTTPNotFound.status_code,
            )
        return Response(text=json.dumps(actors), content_type="application/json")

    @routes.get("/api/job_agent/workspaces/{workspace}/actors/{actor_name}")
    @optional_utils.init_ray_and_catch_exceptions()
    async def get_workspace_actor(self, req: Request) -> Response:
        ws = req.match_info["workspace"]
        name = req.match_info["actor_name"]
        info = await self.get_workspace_manager().get_actor_info(ws, name)
        if info is None or not info.get("ok", True):
            return Response(
                text=json.dumps(
                    info
                    or {
                        "ok": False,
                        "error": {
                            "exc_message": (
                                f"Actor '{name}' does not exist in workspace '{ws}'"
                            )
                        },
                    }
                ),
                content_type="application/json",
                status=aiohttp.web.HTTPNotFound.status_code,
            )
        return Response(text=json.dumps(info), content_type="application/json")

    @routes.delete("/api/job_agent/workspaces/{workspace}/actors/{actor_name}")
    @optional_utils.init_ray_and_catch_exceptions()
    async def delete_workspace_actor(self, req: Request) -> Response:
        ws = req.match_info["workspace"]
        name = req.match_info["actor_name"]
        no_restart = req.query.get("no_restart", "1") in ("1", "true", "True")
        res = await self.get_workspace_manager().kill_actor(
            ws, name, no_restart=no_restart
        )
        if res is None or not res.get("killed"):
            return Response(
                text=f"Actor '{name}' does not exist in workspace '{ws}'",
                status=aiohttp.web.HTTPNotFound.status_code,
            )
        return Response(text=json.dumps(res), content_type="application/json")

    @routes.post(
        "/api/job_agent/workspaces/{workspace}/actors/{actor_name}/methods/{method}"
    )
    @optional_utils.init_ray_and_catch_exceptions()
    async def call_workspace_actor_method(
        self, req: Request
    ) -> aiohttp.web.StreamResponse:
        ws = req.match_info["workspace"]
        name = req.match_info["actor_name"]
        method = req.match_info["method"]
        body = await req.json() if req.can_read_body else {}
        stream = req.query.get("stream", "0") in ("1", "true", "True") or bool(
            body.get("stream", False)
        )
        if stream:
            sresp = aiohttp.web.StreamResponse(
                status=aiohttp.web.HTTPOk.status_code,
                headers={"Content-Type": "application/x-ndjson"},
            )
            await sresp.prepare(req)
            async for item in self.get_workspace_manager().stream_actor_method(
                ws, name, method, body
            ):
                await sresp.write((json.dumps(item) + "\n").encode("utf-8"))
                await sresp.drain()
            await sresp.write_eof()
            return sresp

        res = await self.get_workspace_manager().call_actor_method(
            ws, name, method, body
        )
        if res is None:
            return Response(
                text=json.dumps(
                    {
                        "ok": False,
                        "error": {
                            "exc_type": "KeyError",
                            "exc_message": f"Workspace '{ws}' does not exist.",
                        },
                    }
                ),
                content_type="application/json",
                status=aiohttp.web.HTTPNotFound.status_code,
            )
        return Response(text=json.dumps(res), content_type="application/json")

    @routes.get("/api/job_agent/workspaces/{workspace}/refs/{ref_id}")
    @optional_utils.init_ray_and_catch_exceptions()
    async def get_workspace_ref(self, req: Request) -> Response:
        ws = req.match_info["workspace"]
        ref_id = req.match_info["ref_id"]
        ready_only = req.query.get("ready_only", "0") in ("1", "true", "True")
        timeout_str = req.query.get("timeout")
        timeout = float(timeout_str) if timeout_str is not None else None
        res = await self.get_workspace_manager().get_ref(
            ws, ref_id, ready_only=ready_only, timeout=timeout
        )
        if res is None or (
            not res.get("ok", True)
            and (res.get("error") or {}).get("exc_type") == "KeyError"
        ):
            return Response(
                text=json.dumps(
                    res or {"ok": False, "error": {"exc_message": "Ref not found"}}
                ),
                content_type="application/json",
                status=aiohttp.web.HTTPNotFound.status_code,
            )
        return Response(text=json.dumps(res), content_type="application/json")

    @routes.post("/api/job_agent/workspaces/{workspace}/refs/{ref_id}/cancel")
    @optional_utils.init_ray_and_catch_exceptions()
    async def cancel_workspace_ref(self, req: Request) -> Response:
        ws = req.match_info["workspace"]
        ref_id = req.match_info["ref_id"]
        body = await req.json() if req.can_read_body else {}
        force = bool(body.get("force", False))
        res = await self.get_workspace_manager().cancel_ref(ws, ref_id, force=force)
        if res is None or not res.get("cancelled"):
            return Response(
                text=json.dumps({"ok": False, "cancelled": False, "ref_id": ref_id}),
                content_type="application/json",
                status=aiohttp.web.HTTPNotFound.status_code,
            )
        return Response(text=json.dumps(res), content_type="application/json")

    @routes.delete("/api/job_agent/workspaces/{workspace}/refs/{ref_id}")
    @optional_utils.init_ray_and_catch_exceptions()
    async def delete_workspace_ref(self, req: Request) -> Response:
        ws = req.match_info["workspace"]
        ref_id = req.match_info["ref_id"]
        res = await self.get_workspace_manager().delete_ref(ws, ref_id)
        if res is None or not res.get("deleted"):
            return Response(
                text=json.dumps({"deleted": False, "ref_id": ref_id}),
                content_type="application/json",
                status=aiohttp.web.HTTPNotFound.status_code,
            )
        return Response(text=json.dumps(res), content_type="application/json")

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
