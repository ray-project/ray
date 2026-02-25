"""
A2A (Agent-to-Agent) deployment utilities for Ray Serve.

This module provides a factory function to wrap any LangChain agent with A2A
protocol endpoints:
- GET  /.well-known/agent-card.json  (discovery)
- POST /v1/message:send              (blocking call)
- POST /v1/message:stream            (SSE stream)
- GET  /v1/tasks/{id}                (poll / fetch history)
"""

from __future__ import annotations

import asyncio
import functools
import inspect
from contextlib import asynccontextmanager
from typing import Any, Callable, Coroutine, Optional, Union

from fastapi import FastAPI, HTTPException
from fastapi.routing import APIRoute, request_response
from ray import serve

from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.apps.rest.fastapi_app import A2ARESTFastAPIApplication
from a2a.server.events import EventQueue
from a2a.server.request_handlers.default_request_handler import DefaultRequestHandler
from a2a.server.tasks.inmemory_task_store import InMemoryTaskStore
from a2a.server.tasks.task_updater import TaskUpdater
from a2a.types import AgentCard, Part, TextPart


# Type for agent builder functions (sync or async)
AgentBuilder = Union[Callable[[], Any], Callable[[], Coroutine[Any, Any, Any]]]

def _ray_serve_patch_partial_endpoints(app: FastAPI) -> None:
    """
    Ray Serve's FastAPI ingress code assumes `route.endpoint` has `__qualname__`.

    Some third-party FastAPI builders (including A2A SDK) register endpoints as
    `functools.partial`, which are callable but do not define `__qualname__`.
    Ray Serve then crashes during deployment with:
        AttributeError: 'functools.partial' object has no attribute '__qualname__'

    This function replaces those `route.endpoint` objects with small wrapper
    callables that preserve behavior but have a real `__qualname__`.
    """
    # FastAPI keeps routes on both `app.routes` and `app.router.routes`.
    for route in list(getattr(app, "routes", ())):
        endpoint = getattr(route, "endpoint", None)
        if not isinstance(endpoint, functools.partial):
            continue

        original = endpoint
        original_func = original.func

        @functools.wraps(original_func)
        async def _endpoint_wrapper(*args: Any, __orig: functools.partial = original, **kwargs: Any) -> Any:
            result = __orig(*args, **kwargs)
            if inspect.isawaitable(result):
                return await result
            return result

        # Patch what Ray Serve inspects.
        try:
            route.endpoint = _endpoint_wrapper  # type: ignore[attr-defined]
        except Exception:
            # Best-effort: if we can't patch, Ray will still error.
            continue

        # Keep FastAPI internals consistent for runtime and OpenAPI.
        dependant = getattr(route, "dependant", None)
        if dependant is not None and getattr(dependant, "call", None) is original:
            try:
                dependant.call = _endpoint_wrapper  # type: ignore[attr-defined]
            except Exception:
                pass

        if isinstance(route, APIRoute):
            # Rebuild Starlette handler so the wrapper is used.
            route.app = request_response(route.get_route_handler())

def _to_pascal_case(name: str) -> str:
    """Convert 'weather-agent' / 'research_agent' / 'travel agent' -> 'WeatherAgent'."""
    parts: list[str] = []
    for chunk in (name or "").replace("_", "-").replace(" ", "-").split("-"):
        chunk = chunk.strip()
        if not chunk:
            continue
        parts.append(chunk[:1].upper() + chunk[1:])
    return "".join(parts) or "Agent"


async def run_langchain_agent_once(
    agent: Any,
    *,
    user_request: str,
    thread_id: str,
    max_updates: int = 200,
) -> str:
    """
    Run a LangChain agent using stream_mode="updates" and return the last assistant text.

    This matches how the repo's /chat endpoints stream, providing consistency
    across SSE and A2A interfaces.
    """
    config = {"configurable": {"thread_id": thread_id}}
    inputs = {"messages": [{"role": "user", "content": user_request}]}

    def _extract_text(obj: Any) -> str:
        """Best-effort extraction of assistant text from various LangChain shapes."""
        try:
            if isinstance(obj, str):
                return obj.strip()
            if isinstance(obj, dict):
                # Common keys across versions/wrappers
                for key in ("content", "output", "result"):
                    if isinstance(obj.get(key), str):
                        return obj[key].strip()
                # Sometimes the final state is nested
                for key in ("messages", "agent", "state", "final"):
                    val = obj.get(key)
                    if isinstance(val, dict):
                        txt = _extract_text(val)
                        if txt:
                            return txt
                    if isinstance(val, list):
                        for item in reversed(val):
                            txt = _extract_text(item)
                            if txt:
                                return txt
                return ""
            if isinstance(obj, list):
                for item in reversed(obj):
                    txt = _extract_text(item)
                    if txt:
                        return txt
            content = getattr(obj, "content", None)
            if isinstance(content, str):
                return content.strip()
        except Exception:
            return ""
        return ""

    last_text = ""
    n = 0
    async for update in agent.astream(inputs, config=config, stream_mode="updates"):
        n += 1
        if n > max_updates:
            break
        if isinstance(update, dict):
            txt = _extract_text(update)
            if txt:
                last_text = txt

    if last_text:
        return last_text

    # Fallback: read the committed checkpoint state instead of re-invoking,
    # which would duplicate the user message and re-run tool calls.
    try:
        if hasattr(agent, "aget_state"):
            state = await agent.aget_state(config)
        elif hasattr(agent, "get_state"):
            state = await asyncio.to_thread(agent.get_state, config)
        else:
            state = None

        if state and hasattr(state, "values"):
            txt = _extract_text(state.values)
            if txt:
                return txt
    except Exception:
        pass

    return "(no final text captured from stream updates)"


def create_a2a_app(
    build_agent_fn: AgentBuilder,
    card: AgentCard,
) -> FastAPI:
    """
    Create a FastAPI app that wraps a LangChain agent with A2A protocol endpoints.

    Args:
        build_agent_fn: Function that returns a LangChain agent (sync or async)
        card: Agent discovery card with name, description, version, skills

    Returns:
        Configured FastAPI application with A2A endpoints
    """
    agent_ready = False
    agent_obj: Optional[Any] = None
    agent_init_error: Optional[str] = None
    agent_init_lock = asyncio.Lock()

    async def _init_agent() -> Any:
        nonlocal agent_ready, agent_obj, agent_init_error
        agent_init_error = None
        agent_ready = False
        try:
            maybe = build_agent_fn()
            agent_obj = await maybe if asyncio.iscoroutine(maybe) else maybe
            agent_ready = True
            return agent_obj
        except Exception as e:
            agent_obj = None
            agent_ready = False
            agent_init_error = f"{type(e).__name__}: {e}"
            raise

    async def _ensure_agent() -> Any:
        nonlocal agent_ready, agent_obj, agent_init_error
        if agent_ready and agent_obj is not None:
            return agent_obj
        async with agent_init_lock:
            if agent_ready and agent_obj is not None:
                return agent_obj
            try:
                return await _init_agent()
            except Exception:
                detail = agent_init_error or "Agent initialization failed."
                raise HTTPException(status_code=503, detail=detail)

    class LangChainAgentExecutor(AgentExecutor):
        """A2A AgentExecutor adapter for a LangChain agent."""

        async def execute(self, context: RequestContext, event_queue: EventQueue) -> None:
            a = await _ensure_agent()
            user_text = context.get_user_input() or ""

            task_id = context.task_id or (context.message.task_id if context.message else None)
            context_id = context.context_id or (context.message.context_id if context.message else None)
            task_id = (task_id or "").strip() or "unknown-task"
            context_id = (context_id or "").strip() or task_id

            updater = TaskUpdater(event_queue=event_queue, task_id=task_id, context_id=context_id)
            await updater.submit()
            await updater.start_work()

            try:
                text = await run_langchain_agent_once(
                    a,
                    user_request=user_text,
                    thread_id=context_id,
                )
                msg = updater.new_agent_message(parts=[Part(root=TextPart(text=text))])
                await updater.complete(msg)
            except Exception as e:
                # Surface the exception in the task status message for debuggability.
                msg = updater.new_agent_message(
                    parts=[Part(root=TextPart(text=f"{type(e).__name__}: {e}"))]
                )
                await updater.failed(msg)
                raise

        async def cancel(self, context: RequestContext, event_queue: EventQueue) -> None:
            task_id = context.task_id or (context.message.task_id if context.message else None)
            context_id = context.context_id or (context.message.context_id if context.message else None)
            task_id = (task_id or "").strip() or "unknown-task"
            context_id = (context_id or "").strip() or task_id
            updater = TaskUpdater(event_queue=event_queue, task_id=task_id, context_id=context_id)
            msg = updater.new_agent_message(parts=[Part(root=TextPart(text="Canceled"))])
            await updater.cancel(msg)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        nonlocal agent_ready, agent_obj
        # Best-effort eager init
        try:
            await _init_agent()
        except Exception:
            pass  # Keep serving; execution will return 503
        try:
            yield
        finally:
            agent_ready = False
            try:
                if hasattr(agent_obj, "aclose"):
                    await agent_obj.aclose()
            except Exception:
                pass
            agent_obj = None

    # Build official A2A REST app (HTTP+JSON), and then attach repo-specific endpoints.
    task_store = InMemoryTaskStore()
    http_handler = DefaultRequestHandler(agent_executor=LangChainAgentExecutor(), task_store=task_store)
    a2a_app = A2ARESTFastAPIApplication(agent_card=card, http_handler=http_handler).build(
        title=card.name,
        description=card.description,
        lifespan=lifespan,
    )

    @a2a_app.get("/health")
    async def health():
        if agent_ready:
            return {"status": "healthy", "ready": True}
        try:
            await _ensure_agent()
            return {"status": "healthy", "ready": True}
        except HTTPException:
            payload = {"status": "starting", "ready": False}
            if agent_init_error:
                payload["status"] = "error"
                payload["error"] = agent_init_error
            return payload

    return a2a_app


def create_a2a_deployment(
    build_agent_fn: AgentBuilder,
    card: AgentCard,
    *,
    name: str | None = None,
    num_cpus: float = 1,
) -> Any:
    """
    Create a Ray Serve deployment with A2A protocol endpoints.

    Args:
        build_agent_fn: Function that returns a LangChain agent
        card: Agent discovery card
        num_cpus: Number of CPUs for the Ray actor

    Returns:
        Bound Ray Serve application ready for deployment
    """
    fastapi_app = create_a2a_app(build_agent_fn, card)
    _ray_serve_patch_partial_endpoints(fastapi_app)

    deployment_name = name or f"A2A{_to_pascal_case(card.name)}Deployment"
    DeploymentCls = type(deployment_name, (), {})
    Deployment = serve.ingress(fastapi_app)(DeploymentCls)
    Deployment = serve.deployment(
        name=deployment_name,
        ray_actor_options={"num_cpus": num_cpus},
    )(Deployment)
    return Deployment.bind()
