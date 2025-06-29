
import os
import asyncio
import logging
from contextlib import AsyncExitStack
from typing import Any, Dict, List

from fastapi import FastAPI, Request, HTTPException
from ray import serve

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

app = FastAPI()
logger = logging.getLogger("MCPDeployment")


@serve.deployment(num_replicas=3, ray_actor_options={"num_cpus": 0.5})
@serve.ingress(app)
class BraveSearchDeployment:
    """MCP deployment that exposes every tool provided by its server.

    * **GET  /tools** - list tools (name, description, and input schema)
    * **POST /call** - invoke a tool

      ```json
      {
        "tool_name": "<name>",   // optional - defaults to brave_web_search
        "tool_args": { ... }      // **required** - arguments for the tool
      }
      ```
    """

    DEFAULT_TOOL = "brave_web_search"

    def __init__(self) -> None:
        self._init_task = asyncio.create_task(self._initialize())

    # ------------------------------------------------------------------ #
    # 1. Start podman + MCP session
    # ------------------------------------------------------------------ #
    async def _initialize(self) -> None:
        params = StdioServerParameters(
            command="podman",
            args=[
                "run",
                "-i",
                "--rm",
                "-e",
                f"BRAVE_API_KEY={os.environ['BRAVE_API_KEY']}",
                "docker.io/mcp/brave-search",
            ],
            env=os.environ.copy(),
        )

        self._exit_stack = AsyncExitStack()

        stdin, stdout = await self._exit_stack.enter_async_context(stdio_client(params))

        self.session: ClientSession = await self._exit_stack.enter_async_context(ClientSession(stdin, stdout))
        await self.session.initialize()

        logger.info("BraveSearchDeployment replica ready.")

    async def _ensure_ready(self) -> None:
        """Block until _initialize finishes (and surface its errors)."""
        await self._init_task

    # ------------------------------------------------------------------ #
    # 2. Internal helper: list tools
    # ------------------------------------------------------------------ #
    async def _list_tools(self) -> List[Dict[str, Any]]:
        await self._ensure_ready()
        resp = await self.session.list_tools()
        return [
            {
                "name": tool.name,
                "description": tool.description,
                "input_schema": tool.inputSchema,
            }
            for tool in resp.tools
        ]

    # ------------------------------------------------------------------ #
    # 3. HTTP endpoints
    # ------------------------------------------------------------------ #
    @app.get("/tools")
    async def tools(self):
        """Return all tools exposed by the backing MCP server."""
        return {"tools": await self._list_tools()}

    @app.post("/call")
    async def call_tool(self, request: Request):
        """Generic endpoint to invoke any tool exposed by the server."""
        body = await request.json()

        tool_name: str = body.get("tool_name", self.DEFAULT_TOOL)
        tool_args: Dict[str, Any] | None = body.get("tool_args")

        if tool_args is None:
            raise HTTPException(400, "must include 'tool_args'")

        await self._ensure_ready()

        try:
            result = await self.session.call_tool(tool_name, tool_args)
            return {"result": result}
        except Exception as exc:
            logger.exception("MCP tool call failed")
            raise HTTPException(500, "Tool execution error") from exc

    # ------------------------------------------------------------------ #
    # 4. Tidy shutdown
    # ------------------------------------------------------------------ #
    async def __del__(self):
        if hasattr(self, "_exit_stack"):
            await self._exit_stack.aclose()


# Entry-point object for `serve run …`
brave_search_tool = BraveSearchDeployment.bind()

## Run in terminal.
# serve run brave_mcp_ray_serve:brave_search_tool