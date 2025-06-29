from contextlib import asynccontextmanager
import fastapi
from ray import serve
from mcp.server.fastmcp import FastMCP

# --------------------------------------------------------------------------
# 1.  Create FastMCP in stateless http (streamable) mode
# --------------------------------------------------------------------------
mcp = FastMCP("Image-N-Translate", stateless_http=True)

# --------------------------------------------------------------------------
# 2.  Register your tools BEFORE mounting the app
# --------------------------------------------------------------------------

@mcp.tool()
async def classify(image_url: str) -> str:
    """Return the top-1 label for an image URL."""
    # these remote calls are already async, so no extra thread executor needed
    clf = serve.get_deployment_handle("image_classifier", app_name="image_classifier_app")
    return await clf.classify.remote(image_url)

@mcp.tool()
async def translate(text: str) -> str:
    """Translate English → German."""

    tr  = serve.get_deployment_handle("text_translator", app_name="text_translator_app")
    return await tr.translate.remote(text)


# --------------------------------------------------------------------------
# 3.  Build FastAPI app with lifespan to mount the FastMCP streamable HTTP app
# --------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: fastapi.FastAPI):
    # after startup, mount the streamable-http MCP app
    app.mount("/", mcp.streamable_http_app())

    # keep MCP’s session manager running for the lifetime of this process
    async with mcp.session_manager.run():
        yield

api = fastapi.FastAPI(lifespan=lifespan)

# --------------------------------------------------------------------------
# 4.  Wrap in a Ray Serve deployment
# --------------------------------------------------------------------------
@serve.deployment(
    autoscaling_config={
        "min_replicas": 2,
        "max_replicas": 10,
        "target_ongoing_requests": 50,
    },
    ray_actor_options={
        "num_cpus": 0.5
    }
)
@serve.ingress(api)
class MCPGateway:

    def __init__(self):
        pass  


# --------------------------------------------------------------------------
# 5.  Expose the Serve application graph
# --------------------------------------------------------------------------
app = MCPGateway.bind()



