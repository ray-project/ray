import asyncio
import json
from contextlib import asynccontextmanager
from typing import AsyncGenerator
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from starlette.responses import StreamingResponse
from ray import serve

from agent_with_mcp import build_agent

# ----------------------------------------------------------------------
# FastAPI app with an async lifespan hook.
# ----------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    agent = await build_agent()
    app.state.agent = agent
    try:
        yield
    finally:
        if hasattr(agent, "aclose"):
            await agent.aclose()


fastapi_app = FastAPI(lifespan=lifespan)


@fastapi_app.post("/chat")
async def chat(request: Request):
    """
    POST /chat
    Body: {"user_request": "<text>", "thread_id": "<optional>", "checkpoint_ns": "<optional>"}

    Streams LangGraph 'update' dicts as Server-Sent Events (one JSON object per event).
    SSE frames look like:
        data: {"some": "update"}

    Errors are emitted as:
        event: error
        data: {"error": "ErrorType", "detail": "..."}
    """
    body = await request.json()
    user_request: str = body.get("user_request") or ""

    # Threading and checkpoint identifiers.
    thread_id = (
        body.get("thread_id")
        or request.headers.get("X-Thread-Id")
        or str(uuid4())  # New thread per request if none provided.
    )
    checkpoint_ns = body.get("checkpoint_ns")  # Optional namespacing.

    # Build config for LangGraph.
    config = {"configurable": {"thread_id": thread_id}}
    if checkpoint_ns:
        config["configurable"]["checkpoint_ns"] = checkpoint_ns

    async def event_stream() -> AsyncGenerator[str, None]:
        agent = request.app.state.agent
        inputs = {"messages": [{"role": "user", "content": user_request}]}

        try:
            # Stream updates from the agent.
            async for update in agent.astream(
                inputs, config=config, stream_mode="updates"
            ):
                safe_update = jsonable_encoder(update)
                chunk = json.dumps(safe_update, ensure_ascii=False)
                # Proper SSE framing: "data: <json>\n\n".
                yield f"data: {chunk}\n\n"
        except asyncio.CancelledError:
            # Client disconnected; exit quietly without sending an error frame.
            return
        except Exception as e:
            # Surface one terminal error event and end.
            err = {"error": type(e).__name__, "detail": str(e)}
            err_chunk = json.dumps(err, ensure_ascii=False)
            # SSE with a named event for clients that listen for "error".
            yield f"event: error\ndata: {err_chunk}\n\n"

    # Expose thread id so the client can reuse it on the next call.
    # Also add headers commonly used for SSE behind proxies.
    headers = {
        "X-Thread-Id": thread_id,
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",  # Disable buffering in nginx, if present.
    }

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers=headers,
    )


# ----------------------------------------------------------------------
# Ray Serve deployment wrapper.
# ----------------------------------------------------------------------
@serve.deployment(ray_actor_options={"num_cpus": 1})
@serve.ingress(fastapi_app)
class LangGraphServeDeployment:
    """Ray Serve deployment that exposes the FastAPI app as ingress."""

    pass


app = LangGraphServeDeployment.bind()

# Deploy the agent app locally:
#   serve run ray_serve_agent_deployment:app
#
# Deploy the agent using Anyscale service:
#   anyscale service deploy ray_serve_agent_deployment:app
