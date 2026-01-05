# Build a tool-using agent

This tutorial guides you through building and deploying a sophisticated, tool-using agent using LangChain, LangGraph, and Ray Serve on Anyscale.

Learn how to create a scalable microservices architecture where each component—the agent, the LLM, and the tools—runs as an independent, autoscaling service:

* The Agent (built with LangGraph) orchestrates tasks and manages conversation state.

* The LLM (Qwen 4&nbsp;B) runs in its own service for dedicated, high-speed inference.

* The Tools (a weather API) expose themselves through the Model Context Protocol (MCP), an open standard that allows the agent to discover and use them dynamically.

This decoupled design provides automatic scaling, fault isolation, and the flexibility to update or swap components such as LLMs or tools without changing your agent's code.


## Architecture overview

You can build the agentic system from three core components, each running as its own Ray Serve application.

### Components
* Agent Service ([LangChain agents](https://docs.langchain.com/oss/python/langchain/agents)): The "brain" of the operation. It orchestrates the multi-step reasoning and manages the conversation state. It's lightweight (CPU-only) and deployed with Ray Serve. **Note:**  [LangGraph v1 deprecates the `createReactAgent` prebuilt](https://docs.langchain.com/oss/javascript/migrate/langgraph-v1#deprecation:-createreactagent-%E2%86%92-createagent). Use LangChain's `create_agent`, which runs on LangGraph and adds a flexible middle-ware system. 


* LLM Service (Ray Serve LLM): The "language engine." It runs the `Qwen/Qwen3-4B-Instruct-2507-FP8` model, optimized for tool use. It's deployed with vLLM on a GPU (L4) for high-speed inference and provides an OpenAI-compatible API.

* Tool Service (MCP): The "hands." It exposes a weather API as a set of tools. The agent discovers these tools at runtime using the Model Context Protocol (MCP). It's also a stateless, CPU-only service.

<div align="center">
    <img src="https://anyscale-public-materials.s3.us-west-2.amazonaws.com/agent-template/overall-architecture.svg" alt="Overall Architecture" width="95%"/>
</div>

### Benefits of this architecture

This architecture allows each component to scale independently. Your GPU-intensive LLM service can scale up and down based on inference demand, separate from the lightweight, CPU-based agent orchestration.

### Key benefits using Ray and Anyscale include:

* Independent scaling: Scale GPUs for the LLM and CPUs for the agent/tools separately.

* High availability: Zero-downtime updates and automatic recovery from failures.

* Flexibility: Swap LLMs or add new tools simply by deploying a new service. The agent discovers them at runtime—no code changes needed.

* Enhanced observability: Anyscale provides comprehensive logs, metrics, and tracing for each service.

### Additional resources

For more information on LLM serving and Ray Serve, see the following:
- [Anyscale LLM Serving documentation](https://docs.anyscale.com/llm/serving)
- [Ray Serve LLM documentation](https://docs.ray.io/en/master/serve/llm/index.html)
- [Anyscale LLM Serving Template](https://console.anyscale.com/template-preview/deployment-serve-llm)




## Dependencies and compute resource requirements

**Docker Image:** For optimal compatibility and performance, use the Docker image `anyscale/ray-llm:2.52.0-py311-cu128`. This image includes Ray 2.52.0, Python 3.11, and CUDA 12.8 support, providing all necessary dependencies for LLM serving with GPU acceleration.

**GPU Requirements:** The deployment requires two compute resources: one L4 GPU (g6.2xlarge instance, 24 GB GPU memory) for the LLM service, and one m5d.xlarge (4 vCPU) for the MCP and agent services.

**Python Libraries**: This project uses `requirements.txt` for dependency management. Run the following command to install dependencies:



```bash
%%bash
# Install dependencies
pip install -r requirements.txt
```

## Implementation: Building the services

This project consists of several Python scripts that work together to create and serve the agent.

### Step 1: Create the LLM service

Check out the code in `llm_deploy_qwen.py`. This script deploys the Qwen LLM (`Qwen/Qwen3-4B-Instruct-2507-FP8`) as an OpenAI-compatible API endpoint using Ray Serve's `build_openai_app` utility. This allows you to use the Qwen model with any OpenAI-compatible client, including LangChain.

The following are key configurations in this script:

- **`accelerator_type="L4"`**: Specifies the GPU type. L4 GPUs (Ada Lovelace architecture) are optimized for FP8 precision, making them cost-effective for this quantized model. For higher throughput, use H100 GPUs. For GPU selection guidance, see the [GPU guidance documentation](https://docs.anyscale.com/llm/serving/gpu-guidance).


- **`enable_auto_tool_choice=True`**: Enables the model to automatically decide when to use tools based on the input. This is essential for agent workflows where the LLM needs to determine whether to call a tool or respond directly. For more information on tool calling, see the [tool and function calling documentation](https://docs.anyscale.com/llm/serving/tool-function-calling).

- **`tool_call_parser="hermes"`**: Specifies the parsing strategy for tool calls. The `hermes` parser is designed for models that follow the Hermes function-calling format, which Qwen models support.

- **`trust_remote_code=True`**: Required when loading Qwen models from Hugging Face, as they use custom chat templates and tokenization logic that aren't part of the standard transformers library.

**Additional LLM development resources:**
- [LLM serving basics](https://docs.anyscale.com/llm/serving/intro)
- [LLM serving examples and template](https://console.anyscale.com/template-preview/deployment-serve-llm): Comprehensive examples for deploying LLMs with Ray Serve
- [Performance optimization documentation](https://docs.anyscale.com/llm/serving/performance-optimization)
- [Configure structured output](https://docs.anyscale.com/llm/serving/structured-output): Ensure LLM responses match specific schemas


### Step 2: Create the MCP weather tool service

Check out the code in `weather_mcp_ray.py` to deploy weather tools as an MCP (Model Context Protocol) service.

**How the weather tool service works:**

The `weather_mcp_ray.py` script uses `FastMCP` to define and expose weather-related tools. This service is a FastAPI application deployed with Ray Serve, making the tools available over HTTP.

- **FastMCP framework**: The `FastMCP` class provides a way to define tools using Python decorators. Setting `stateless_http=True` makes it suitable for deployment as an HTTP service.

- **Tool registration**: Each function decorated with `@mcp.tool()` becomes an automatically discoverable tool:
  - `get_alerts(state: str)`: Fetches active weather alerts for a given U.S. state code.
  - `get_forecast(latitude: float, longitude: float)`: Retrieves a 5-period forecast for specific coordinates.

- **Tool metadata**: The docstrings for each tool function serve as descriptions that the agent uses to understand when and how to call each tool. This is crucial for the LLM to decide which tool to use.

- **Ray Serve deployment**: When deployed with Ray Serve, this becomes a scalable microservice that can handle multiple concurrent tool requests from agent instances.

:::{note}
Ray Serve only supports stateless HTTP mode in MCP. Set `stateless_http=True` to prevent "session not found" errors when multiple replicas are running:
:::

```python
mcp = FastMCP("weather", stateless_http=True)
```

**Additional resources:**
- [MCP quickstart guide](https://docs.anyscale.com/mcp/mcp-quickstart-guide)
- [Deploy scalable MCP servers](https://docs.anyscale.com/mcp/scalable-remote-mcp-deployment)
- [Anyscale MCP Deployment Template](https://console.anyscale.com/template-preview/mcp-ray-serve)


### Step 3: Create the agent logic

<div align="center">
  <img src="https://anyscale-public-materials.s3.us-west-2.amazonaws.com/agent-template/agent-architecture.svg" alt="Agent Architecture" width="75%"/>
</div>

Check out the code in `agent_with_mcp.py` to define the agent that orchestrates the LLM and tools.

The core function is `build_agent`:

```python
# ========== BUILD AGENT ==========
async def build_agent():
    """Instantiate an agent with MCP tools when available."""
    mcp_tools = await get_mcp_tools()

    tools = list(mcp_tools)
    print(f"\n[Agent] Using {len(tools)} tool(s).")

    memory = MemorySaver()
    agent = create_agent(
        llm,
        tools,
        system_prompt=PROMPT,
        checkpointer=memory,
    )
    return agent
```

**How the agent works:**

- **LLM configuration**: Connects to your deployed Qwen model using the OpenAI-compatible API.

- **Tool discovery**: The function `get_mcp_tools` uses `MultiServerMCPClient` to automatically discover available tools from the MCP service.

- **Agent creation**: Creates an agent with the LLM, tools, and system prompt using LangChain's `create_agent` function.

- **Memory management**: Uses `MemorySaver` to maintain conversation state across multiple turns.



### Step 4: Create the agent deployment script

The `ray_serve_agent_deployment.py` script deploys the agent as a Ray Serve application with a `/chat` endpoint.

```python
import json
from contextlib import asynccontextmanager
from typing import AsyncGenerator
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from starlette.responses import StreamingResponse
from ray import serve

from agent_with_mcp import build_agent  # Your factory that returns a LangChain / LangGraph agent.

# ----------------------------------------------------------------------
# FastAPI app with an async lifespan hook.
# ----------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    agent = await build_agent()  # Likely compiled with a checkpointer.
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

    Streams LangGraph 'update' dicts as SSE (one JSON object per event).
    """
    body = await request.json()
    user_request: str = body.get("user_request", "")

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
            async for update in agent.astream(inputs, config=config, stream_mode="updates"):
                safe_update = jsonable_encoder(update)
                # Proper SSE framing: "data: <json>\n\n".
                yield f"data: {json.dumps(safe_update)}\n\n"
        except Exception as e:
            # Don't crash the SSE; surface one terminal error event and end.
            err = {"error": type(e).__name__, "detail": str(e)}
            yield f"data: {json.dumps(err)}\n\n"

    # Expose thread id so the client can reuse it on the next call.
    headers = {"X-Thread-Id": thread_id}

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
    pass

app = LangGraphServeDeployment.bind()

# Deploy the agent app locally:
# serve run ray_serve_agent_deployment:app

# Deploy the agent using Anyscale service:
# anyscale service deploy ray_serve_agent_deployment:app
```

**How deployment works:**

- **FastAPI lifespan management**: Uses `@asynccontextmanager` to initialize the agent on startup and clean up on shutdown.

- **Streaming endpoint**: The `/chat` endpoint accepts POST requests and returns server-sent events (SSE):
  ```python
  {
    "user_request": "What's the weather?",
    "thread_id": "optional-thread-id",
    "checkpoint_ns": "optional-namespace"
  }
  ```

- **Thread management**: Each conversation can have a `thread_id` to maintain context across requests. If you don't provide a `thread_id`, the system generates a new UUID.

- **Event streaming**: Uses LangGraph's `astream` to emit real-time updates (tool calls, reasoning steps, final answers) as JSON objects.

- **Resource allocation**: The agent deployment is lightweight (0.2 CPUs per replica, no GPU) since heavy computation happens in the LLM service.


## Deploy the services

Now that you've reviewed the code, deploy each service to Anyscale.

### Step 5: Deploy the LLM service

Deploy the Qwen LLM service on Anyscale. This command creates a scalable endpoint for LLM inference:



```bash
%%bash
anyscale service deploy llm_deploy_qwen:app --name llm_deploy_qwen_service

```

After deployment completes, you receive:
- Service URL (for example, `https://llm-deploy-qwen-service-jgz99.cld-kvedzwag2qa8i5bj.s.anyscaleuserdata.com`)
- API token for authentication

**Save these values to configure the agent later.** Note: You don't need to add `/v1` to the URL manually; the code uses `urljoin` to append it automatically.


### Step 6: Deploy the weather MCP service

Deploy the weather tool service. This creates an endpoint for the agent to discover and call weather tools:



```bash
%%bash
anyscale service deploy weather_mcp_ray:app --name weather_mcp_service

```

After deployment completes, you receive:
- Service URL (for example, `https://weather-mcp-service-jgz99.cld-kvedzwag2qa8i5bj.s.anyscaleuserdata.com`)
- API token for authentication

**Note:** You don't need to add `/mcp` to the URL manually; the code uses `urljoin` to append it automatically.


### Step 7: Configure the agent

Update `agent_with_mcp.py` with the service endpoints you received from the deployments. Modify the following lines:

```python
from urllib.parse import urljoin

API_KEY = "<your-llm-service-token>"
OPENAI_COMPAT_BASE_URL = "<your-llm-service-url>"  
MODEL = "Qwen/Qwen3-4B-Instruct-2507-FP8"
TEMPERATURE = 0.01
WEATHER_MCP_BASE_URL = "<your-mcp-service-url>"  
WEATHER_MCP_TOKEN = "<your-mcp-service-token>"

# The code uses urljoin to automatically append /v1 and /mcp:
llm = ChatOpenAI(
    model=MODEL,
    base_url=urljoin(OPENAI_COMPAT_BASE_URL, "v1"),
    api_key=API_KEY,
    ...
)

mcp_client = MultiServerMCPClient({
    "weather": {
        "url": urljoin(WEATHER_MCP_BASE_URL, "mcp"),
        ...
    }
})
```




### Step 8: Deploy the agent service locally

Deploy the agent itself. For local testing, use `serve run` in your terminal. For production deployment on Anyscale, see next step.

**For local deployment:**

```bash
serve run ray_serve_agent_deployment:app  
```


## Test the agent

### Step 9: Send test requests

With the agent service running, send requests to the `/chat` endpoint. 
The script "helpers/agent_client_local.py" sends a request and streams the response:



```python
import json
import requests

SERVER_URL = "http://127.0.0.1:8000/chat"  # For local deployment.
HEADERS = {"Content-Type": "application/json"}

def chat(user_request: str, thread_id: str | None = None) -> None:
    """Send a chat request to the agent and stream the response."""
    payload = {"user_request": user_request}
    if thread_id:
        payload["thread_id"] = thread_id

    with requests.post(SERVER_URL, headers=HEADERS, json=payload, stream=True) as resp:
        resp.raise_for_status()
        # Capture thread_id for multi-turn conversations.
        server_thread = resp.headers.get("X-Thread-Id")
        if not thread_id and server_thread:
            print(f"[thread_id: {server_thread}]")
        # Stream SSE events.
        for line in resp.iter_lines():
            if not line:
                continue
            txt = line.decode("utf-8")
            if txt.startswith("data: "):
                txt = txt[len("data: "):]
            print(txt, flush=True)

# Test the agent.
chat("What's the weather in Palo Alto?")

```

### Terminate the local server
After testing the agent locally, you can shut down the local server by pressing `Ctrl + C` in the same terminal, or by running the following command in a different terminal:

```bash
serve shutdown -y
```

### Step 10: Deploy the agent to production on Anyscale

After testing the agent locally, deploy it to Anyscale for production use. This creates a scalable, managed endpoint with enterprise features.

#### Why deploy to Anyscale

**Production benefits:**
- **Auto-scaling**: Automatically scales replicas based on request volume (0 to N replicas)
- **High availability**: Zero-downtime deployments with automated handling of outages
- **Observability**: Built-in metrics, logs, and distributed tracing
- **Cost optimization**: Scale to zero when idle (with appropriate configuration)
- **Load balancing**: Distributes requests across multiple agent replicas
- **Fault isolation**: Agent, LLM, and tools run as separate services

#### Deploy the agent service

Run the following command to deploy your agent to Anyscale. This command packages your code and creates a production-ready service:




```bash
%%bash
anyscale service deploy ray_serve_agent_deployment:app --name agent_service_langchain
```

#### Understanding the deployment output

After running the deployment command, you receive:
- **Service URL**: The HTTPS endpoint for your agent (for example, `https://agent-service-langchain-jgz99.cld-kvedzwag2qa8i5bj.s.anyscaleuserdata.com`)
- **Authorization token**: Bearer token for authenticating requests
- **Service UI link**: Direct link to monitor your service in the Anyscale console

#### Test the production agent

Once deployed, test your production agent with authenticated requests. Update the following code with your deployment details.

**Note:** The repository also includes the script at "helpers/agent_client_anyscale.py" for your reference.


```python
import json
import requests

base_url = "https://agent-service-langchain-jgz99.cld-kvedzwag2qa8i5bj.s.anyscaleuserdata.com" ## replace with your service url
token = "nZp2BEjdloNlwGyxoWSpdalYGtkhfiHtfXhmV4BQuyk" ## replace with your service bearer token

SERVER_URL = f"{base_url}/chat"  # For Anyscale deployment.
HEADERS = {"Content-Type": "application/json",
"Authorization": f"Bearer {token}"
}

def chat(user_request: str, thread_id: str | None = None) -> None:
    """Send a chat request to the agent and stream the response."""
    payload = {"user_request": user_request}
    if thread_id:
        payload["thread_id"] = thread_id

    with requests.post(SERVER_URL, headers=HEADERS, json=payload, stream=True) as resp:
        resp.raise_for_status()
        # Capture thread_id for multi-turn conversations.
        server_thread = resp.headers.get("X-Thread-Id")
        if not thread_id and server_thread:
            print(f"[thread_id: {server_thread}]")
        # Stream SSE events.
        for line in resp.iter_lines():
            if not line:
                continue
            txt = line.decode("utf-8")
            if txt.startswith("data: "):
                txt = txt[len("data: "):]
            print(txt, flush=True)

# Test the agent.
chat("What's the weather in Palo Alto?")
```

### Terminate the production server
Right now there are three production services running: `llm_deploy_qwen_service`, `weather_mcp_service`, and `agent_service_langchain`. You can manually shutdown them on the service page, or you can run the command in your terminal to shutdown them.

```bash
anyscale service terminate -n llm_deploy_qwen_service
anyscale service terminate -n weather_mcp_service
anyscale service terminate -n agent_service_langchain
```

## Next steps

You've successfully built, deployed, and tested a multi-tool agent using Ray Serve on Anyscale. This architecture demonstrates how to build production-ready AI applications with independent scaling, fault isolation, and dynamic tool discovery.

### Extend your agent

**Add more tools**  
Extend the MCP service with additional capabilities such as database queries, API integrations, or custom business logic. The MCP protocol allows your agent to discover new tools dynamically without code changes. For implementation examples, see the [Anyscale MCP Deployment Template](https://console.anyscale.com/template-preview/mcp-ray-serve).

**Swap or upgrade LLMs**  
Replace the Qwen model with other tool-calling models such as GPT-4, Claude, or Llama variants. Since the LLM runs as a separate service, you can A/B test different models or perform zero-downtime upgrades. For deployment patterns, see the [Anyscale LLM Serving Template](https://console.anyscale.com/template-preview/deployment-serve-llm).

**Build complex workflows**  
Implement sophisticated reasoning patterns with LangGraph, such as multi-agent collaboration, iterative refinement, or conditional branching based on tool outputs.

### Optimize for production

**Monitor performance**  
Use Anyscale's built-in observability to track:
- Request latency and token throughput
- GPU utilization and memory usage
- Memory or disk usage
- Node count

To enable LLM metrics, see [Monitor with the Ray Serve LLM dashboard](https://docs.anyscale.com/llm/serving/benchmarking/metrics#monitor-with-the-ray-serve-llm-dashboard). Note: Engine metric logging is on by default as of Ray 2.51 or later.
For detailed metrics guidance on monitoring with Anyscale service, see [Monitor a service](https://docs.anyscale.com/services/monitoring#metrics).


**Scale efficiently**  
Configure auto-scaling policies for each service independently:
- Scale the LLM service based on GPU utilization
- Scale the agent service based on request volume
- Scale tool services based on specific workload patterns

See [Ray Serve autoscaling configuration](https://docs.anyscale.com/llm/serving/parameter-tuning#ray-serve-autoscaling-configuration)

### Production best practices

Anyscale services provide enterprise-grade features for running agents in production. Key capabilities include:

- **Zero-downtime deployments**: Update models or agent logic without interrupting service. See [Update an Anyscale service](https://docs.anyscale.com/services/update).

- **Multi-version management**: Deploy up to 10 versions behind a single endpoint for A/B testing and canary deployments. See [Deploy multiple versions of an Anyscale service](https://docs.anyscale.com/services/versions).

- **High availability**: Distribute replicas across availability zones with automatic handling of outages. See [Configure head node fault tolerance](https://docs.anyscale.com/administration/resource-management/head-node-fault-tolerance).


For comprehensive guidance on production deployments, see the [Anyscale Services documentation](https://docs.anyscale.com/services) and [Ray Serve on the Anyscale Runtime](https://docs.anyscale.com/runtime/serve).







