import asyncio
from fastapi import FastAPI
from mcp.server.fastmcp import FastMCP
from contextlib import asynccontextmanager
from ray import serve
from transformers import pipeline

# ---------------------------------------------------------------------
# 1. FastMCP business logic for translation
# ---------------------------------------------------------------------
mcp = FastMCP("translator", stateless_http=True)

# Pre-load the translation model (English → French).
translator_pipeline = pipeline("translation_en_to_fr", model="t5-small")

@mcp.tool()
async def translate(text: str) -> str:
    """Translate English text to French."""
    loop = asyncio.get_event_loop()
    # Offload the sync pipeline call to a thread to avoid blocking the event loop.
    result = await loop.run_in_executor(None, translator_pipeline, text)
    return result[0]["translation_text"]



## FastAPI app and Ray Serve setup.
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1) Mount the MCP app.
    app.mount("/", mcp.streamable_http_app())

    # 2) Enter the session_manager's context.
    async with mcp.session_manager.run():
        yield

fastapi_app = FastAPI(lifespan=lifespan)

@serve.deployment(
    autoscaling_config={
        "min_replicas": 2,
        "max_replicas": 20,
        "target_ongoing_requests": 10
    },
    ray_actor_options={"num_gpus": 0.5, 
    'runtime_env':{
        "pip": [
            "transformers",   
            "torch"              
        ]
    }}
)
@serve.ingress(fastapi_app)
class TranslatorMCP:
    def __init__(self):
        pass
       

# Ray Serve entry point.
app = TranslatorMCP.bind()


## Run in terminal.
# serve run translator_mcp_ray:app 