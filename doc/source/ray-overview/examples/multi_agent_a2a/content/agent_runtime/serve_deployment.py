"""
Ray Serve deployment factory for LangChain agents.

This module provides a unified way to create Ray Serve deployments for any
LangChain agent, eliminating duplicate FastAPI app and deployment code.
"""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Callable, Coroutine, Optional, Union
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from ray import serve
from starlette.responses import StreamingResponse


# Type for agent builder functions (sync or async)
AgentBuilder = Union[Callable[[], Any], Callable[[], Coroutine[Any, Any, Any]]]


def create_chat_app(
    build_agent_fn: AgentBuilder,
    *,
    title: str = "LangChain Agent",
    description: str = "LangChain agent with streaming SSE support",
) -> FastAPI:
    """
    Create a FastAPI app with a /chat endpoint that streams LangChain updates as SSE.

    This is the core FastAPI app used by all agent deployments. It provides:
    - Async lifespan management for agent initialization/cleanup
    - POST /chat endpoint with SSE streaming
    - Thread ID support for conversation continuity

    Args:
        build_agent_fn: Function that returns a LangChain agent (can be sync or async)
        title: FastAPI app title
        description: FastAPI app description

    Returns:
        Configured FastAPI application

    Example:
        >>> from weather_agent_with_mcp import build_agent
        >>> app = create_chat_app(build_agent, title="Weather Agent")
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """Initialize agent on startup, cleanup on shutdown."""
        # Support both sync and async build functions
        maybe_coro = build_agent_fn()
        agent = await maybe_coro if asyncio.iscoroutine(maybe_coro) else maybe_coro
        app.state.agent = agent
        try:
            yield
        finally:
            if hasattr(agent, "aclose"):
                await agent.aclose()

    fastapi_app = FastAPI(title=title, description=description, lifespan=lifespan)

    @fastapi_app.post("/chat")
    async def chat(request: Request):
        """
        POST /chat
        Body: {"user_request": "<text>", "thread_id": "<optional>", "checkpoint_ns": "<optional>"}

        Streams LangChain 'update' dicts as Server-Sent Events (one JSON object per event).
        SSE frames look like:
            data: {"some": "update"}

        Errors are emitted as:
            event: error
            data: {"error": "ErrorType", "detail": "..."}
        """
        body = await request.json()
        user_request: str = body.get("user_request") or ""

        # Threading and checkpoint identifiers
        thread_id = (
            body.get("thread_id")
            or request.headers.get("X-Thread-Id")
            or str(uuid4())  # New thread per request if none provided
        )
        checkpoint_ns = body.get("checkpoint_ns")  # Optional namespacing

        # Build config for LangChain
        config = {"configurable": {"thread_id": thread_id}}
        if checkpoint_ns:
            config["configurable"]["checkpoint_ns"] = checkpoint_ns

        async def event_stream() -> AsyncGenerator[str, None]:
            agent = request.app.state.agent
            inputs = {"messages": [{"role": "user", "content": user_request}]}

            try:
                # Stream updates from the agent
                async for update in agent.astream(
                    inputs, config=config, stream_mode="updates"
                ):
                    safe_update = jsonable_encoder(update)
                    chunk = json.dumps(safe_update, ensure_ascii=False)
                    # Proper SSE framing: "data: <json>\n\n"
                    yield f"data: {chunk}\n\n"
            except asyncio.CancelledError:
                # Client disconnected; exit quietly without sending an error frame
                return
            except Exception as e:
                # Surface one terminal error event and end
                err = {"error": type(e).__name__, "detail": str(e)}
                err_chunk = json.dumps(err, ensure_ascii=False)
                # SSE with a named event for clients that listen for "error"
                yield f"event: error\ndata: {err_chunk}\n\n"

        # Expose thread id so the client can reuse it on the next call
        # Also add headers commonly used for SSE behind proxies
        headers = {
            "X-Thread-Id": thread_id,
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # Disable buffering in nginx, if present
        }

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers=headers,
        )

    return fastapi_app


def create_agent_deployment(
    build_agent_fn: AgentBuilder,
    *,
    name: str = "LangChainAgent",
    title: str = "LangChain Agent",
    description: str = "LangChain agent with streaming SSE support",
    num_cpus: float = 1,
) -> Any:
    """
    Create a Ray Serve deployment for a LangChain agent.

    This is a factory function that creates both the FastAPI app and the
    Ray Serve deployment wrapper in one step.

    Args:
        build_agent_fn: Function that returns a LangChain agent (can be sync or async)
        name: Name for the Ray Serve deployment class
        title: FastAPI app title
        description: FastAPI app description
        num_cpus: Number of CPUs for the Ray actor

    Returns:
        Bound Ray Serve application ready for deployment

    Example:
        >>> from weather_agent_with_mcp import build_agent
        >>> app = create_agent_deployment(build_agent, name="WeatherAgent")
        >>> # Deploy: serve run module:app
    """
    fastapi_app = create_chat_app(
        build_agent_fn,
        title=title,
        description=description,
    )

    # IMPORTANT:
    # - Changing __name__ after @serve.deployment does NOT reliably change the Serve deployment name.
    # - Use an explicit deployment name via @serve.deployment(name=...).
    DeploymentCls = type(
        name,
        (),
        {"__doc__": "Ray Serve deployment that exposes the FastAPI app as ingress."},
    )
    Deployment = serve.ingress(fastapi_app)(DeploymentCls)
    Deployment = serve.deployment(
        name=name,
        ray_actor_options={"num_cpus": num_cpus},
    )(Deployment)
    return Deployment.bind()
