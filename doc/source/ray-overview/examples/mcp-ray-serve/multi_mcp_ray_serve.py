import asyncio
import logging
import os
from contextlib import AsyncExitStack
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Request
from ray import serve
from ray.serve.handle import DeploymentHandle

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

logger = logging.getLogger("multi_mcp_serve")

def _podman_args(
    image: str,
    *,
    extra_args: Optional[List[str]] = None,
    env: Optional[Dict[str, str]] = None,
) -> List[str]:
    args = ["run", "-i", "--rm"]
    for key, value in (env or {}).items():
        if key.upper() == "PATH":
            continue
        args += ["-e", f"{key}={value}"]
    if extra_args:
        args += extra_args
    args.append(image)
    return args

class _BaseMCP:
    _PODMAN_ARGS: List[str] = []
    _ENV: Dict[str, str] = {}

    def __init__(self):
        self._ready = asyncio.create_task(self._startup())

    async def _startup(self):
        params = StdioServerParameters(
            command="podman",
            args=self._PODMAN_ARGS,
            env=self._ENV,
        )
        self._stack = AsyncExitStack()
        stdin, stdout = await self._stack.enter_async_context(stdio_client(params))
        self.session = await self._stack.enter_async_context(ClientSession(stdin, stdout))
        await self.session.initialize()
        logger.info("%s replica ready", type(self).__name__)

    async def _ensure_ready(self):
        await self._ready

    async def list_tools(self) -> List[Dict[str, Any]]:
        await self._ensure_ready()
        resp = await self.session.list_tools()
        return [
            {"name": t.name, "description": t.description, "input_schema": t.inputSchema}
            for t in resp.tools
        ]

    async def call_tool(self, tool_name: str, tool_args: Dict[str, Any]) -> Any:
        await self._ensure_ready()
        return await self.session.call_tool(tool_name, tool_args)

    async def __del__(self):
        if hasattr(self, "_stack"):
            await self._stack.aclose()

def build_mcp_deployment(
    *,
    name: str,
    docker_image: str,
    num_replicas: int = 3,
    num_cpus: float = 0.5,
    autoscaling_config: Optional[Dict[str, Any]] = None,
    server_command: Optional[str] = None,
    extra_podman_args: Optional[List[str]] = None,
    env: Optional[Dict[str, str]] = None,
) -> serve.Deployment:
    """
    - If autoscaling_config is provided, Ray Serve will autoscale between
      autoscaling_config['min_replicas'] and ['max_replicas'].
    - Otherwise it will launch `num_replicas` fixed replicas.
    """
    deployment_env = env or {}
    podman_args = _podman_args(docker_image, extra_args=extra_podman_args, env=deployment_env)
    if server_command:
        podman_args.append(server_command)

    # Build kwargs for the decorator:
    deploy_kwargs: Dict[str, Any] = {
        "name": name,
        "ray_actor_options": {"num_cpus": num_cpus},
    }
    if autoscaling_config:
        deploy_kwargs["autoscaling_config"] = autoscaling_config
    else:
        deploy_kwargs["num_replicas"] = num_replicas

    @serve.deployment(**deploy_kwargs)
    class MCP(_BaseMCP):
        _PODMAN_ARGS = podman_args
        _ENV = deployment_env

    return MCP

# -------------------------
# HTTP router code
# -------------------------

api = FastAPI()

@serve.deployment
@serve.ingress(api)
class Router:
    def __init__(self,
                 brave_search: DeploymentHandle,
                 fetch: DeploymentHandle) -> None:
        self._mcps = {"brave_search": brave_search, "fetch": fetch}

    @api.get("/{mcp_name}/tools")
    async def list_tools_http(self, mcp_name: str):
        handle = self._mcps.get(mcp_name)
        if not handle:
            raise HTTPException(404, f"MCP {mcp_name} not found")
        try:
            return {"tools": await handle.list_tools.remote()}
        except Exception as exc:
            logger.exception("Listing tools failed")
            raise HTTPException(500, str(exc))

    @api.post("/{mcp_name}/call")
    async def call_tool_http(self, mcp_name: str, request: Request):
        handle = self._mcps.get(mcp_name)
        if not handle:
            raise HTTPException(404, f"MCP {mcp_name} not found")
        body = await request.json()
        tool_name = body.get("tool_name")
        tool_args = body.get("tool_args")
        if tool_name is None or tool_args is None:
            raise HTTPException(400, "Missing 'tool_name' or 'tool_args'")
        try:
            result = await handle.call_tool.remote(tool_name, tool_args)
            return {"result": result}
        except Exception as exc:
            logger.exception("Tool call failed")
            raise HTTPException(500, str(exc))

# -------------------------
# Binding deployments
# -------------------------

if "BRAVE_API_KEY" not in os.environ:
    raise RuntimeError("BRAVE_API_KEY must be set before `serve run`.")

# Example: autoscaling BraveSearch between 1 and 5 replicas,
# targeting ~10 concurrent requests per replica.
BraveSearch = build_mcp_deployment(
    name="brave_search",
    docker_image="docker.io/mcp/brave-search",
    env={"BRAVE_API_KEY": os.environ["BRAVE_API_KEY"]},
    num_cpus=0.2,
    autoscaling_config={
        "min_replicas": 1,
        "max_replicas": 5,
        "target_num_ongoing_requests_per_replica": 10,
    },
)

# Example: keep Fetch at a fixed 2 replicas.
Fetch = build_mcp_deployment(
    name="fetch",
    docker_image="docker.io/mcp/fetch",
    num_replicas=2,
    num_cpus=0.2,
)

# entry-point object for `serve run …`
brave_search_handle = BraveSearch.bind()
fetch_handle = Fetch.bind()
app = Router.bind(brave_search_handle, fetch_handle)

## Run in terminal.
# serve run multi_mcp_ray_serve:app
