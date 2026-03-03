(serve-llm-architecture-core)=
# Core components

This guide explains the technical implementation details of Ray Serve LLM's core components. You'll learn about the abstractions, protocols, and patterns that enable extensibility and modularity.

## Core abstractions

Beyond `LLMServer` and `OpenAiIngress`, Ray Serve LLM defines several core abstractions that enable extensibility and modularity:

### LLMEngine protocol

The `LLMEngine` abstract base class defines the contract for all inference engines. This abstraction allows Ray Serve LLM to support multiple engine implementations (vLLM, SGLang, TensorRT-LLM, etc.) with a consistent interface.

The engine operates at the **OpenAI API level**, not at the raw prompt level. This means:
- It accepts OpenAI-formatted requests (`ChatCompletionRequest`, `CompletionRequest`, etc.).
- It returns OpenAI-formatted responses.
- Engine-specific details (such as tokenization, sampling) are hidden behind this interface.

#### Key methods

```python

class LLMEngine(ABC):
    """Base protocol for all LLM engines."""
    
    @abstractmethod
    async def chat(
        self, 
        request: ChatCompletionRequest
    ) -> AsyncGenerator[Union[str, ChatCompletionResponse, ErrorResponse], None]:
        """Run a chat completion.
        
        Yields:
        - Streaming: yield "data: <json>\\n\\n" for each chunk.
        - Non-streaming: yield single ChatCompletionResponse.
        - Error: yield ErrorResponse.
        - In all cases, it's still a generator to unify the upper-level logic.
        """
    
    @abstractmethod
    async def completions(
        self, 
        request: CompletionRequest
    ) -> AsyncGenerator[Union[str, CompletionResponse, ErrorResponse], None]:
        """Run a text completion."""
    
    @abstractmethod
    async def embeddings(
        self, 
        request: EmbeddingRequest
    ) -> AsyncGenerator[Union[EmbeddingResponse, ErrorResponse], None]:
        """Generate embeddings."""
    
    @abstractmethod
    async def start(self):
        """Start the engine (async initialization)."""
    
    @abstractmethod
    async def check_health(self) -> bool:
        """Check if engine is healthy."""
    
    @abstractmethod
    async def shutdown(self):
        """Gracefully shutdown the engine."""
```

#### Engine implementations

Ray Serve LLM provides:

- **VLLMEngine**: Production-ready implementation using vLLM.
  - Supports continuous batching and paged attention.
  - Supports all kinds of parallelism.
  - KV cache transfer for prefill-decode disaggregation.
  - Automatic prefix caching (APC).
  - LoRA adapter support.

Future implementations could include:
- **TensorRT-LLM**: NVIDIA's optimized inference engine.
- **SGLang**: Fast serving with RadixAttention.

Ray Serve LLM deeply integrates with vLLM since it has end-to-end Ray support in the engine, which gives benefits in fine-grained placement of workers and other optimizations. The engine abstraction makes it straightforward to add new implementations without changing the core serving logic.

### LLMConfig

`LLMConfig` is the central configuration object that specifies everything needed to deploy an LLM:

```python
@dataclass
class LLMConfig:
    """Configuration for LLM deployment."""
    
    # Model loading
    model_loading_config: Union[dict, ModelLoadingConfig]
    
    # Hardware requirements
    accelerator_type: Optional[str] = None  # For example, "A10G", "L4", "H100"
    
    # Placement group configuration
    placement_group_config: Optional[dict] = None
    
    # Engine-specific arguments
    engine_kwargs: Optional[dict] = None
    
    # Ray Serve deployment configuration
    deployment_config: Optional[dict] = None
    
    # LoRA adapter configuration
    lora_config: Optional[Union[dict, LoraConfig]] = None
    
    # Runtime environment (env vars, pip packages)
    runtime_env: Optional[dict] = None

```

#### Model loading configuration

The `ModelLoadingConfig` specifies where and how to load the model. The following code shows the configuration structure:

```python
@dataclass
class ModelLoadingConfig:
    """Configuration for model loading."""
    
    # Model identifier (used for API requests)
    model_id: str
    
    # Model source (HuggingFace or cloud storage)
    model_source: Union[str, dict]
    # Examples:
    # - "Qwen/Qwen2.5-7B-Instruct" (HuggingFace)
    # - {"bucket_uri": "s3://my-bucket/models/qwen-7b"} (S3)
```

#### LoRA configuration

The following code shows the configuration structure for serving multiple LoRA adapters with a shared base model:

```python
@dataclass
class LoraConfig:
    """Configuration for LoRA multiplexing."""
    
    # Path to LoRA weights (local or S3/GCS)
    dynamic_lora_loading_path: Optional[str] = None
    
    # Maximum number of adapters per replica
    max_num_adapters_per_replica: int = 1
```

Ray Serve's multiplexing feature automatically routes requests to replicas that have the requested LoRA adapter loaded, using an LRU cache for adapter management.

### Deployment protocols

Ray Serve LLM defines two key protocols that components must implement:

#### DeploymentProtocol

The base protocol for all deployments:

```python
class DeploymentProtocol(Protocol):
    """Base protocol for Ray Serve LLM deployments."""
    
    @classmethod
    def get_deployment_options(cls, *args, **kwargs) -> dict:
        """Return Ray Serve deployment options.
        
        Returns:
            dict: Options including:
                - placement_strategy: PlacementGroup configuration
                - num_replicas: Initial replica count
                - autoscaling_config: Autoscaling parameters
                - ray_actor_options: Ray actor options
        """
```

This protocol ensures that all deployments can provide their own configuration for placement, scaling, and resources.

#### LLMServerProtocol

Extended protocol for LLM server deployments:

```python
class LLMServerProtocol(DeploymentProtocol):
    """Protocol for LLM server deployments."""
    
    @abstractmethod
    async def chat(
        self,
        request: ChatCompletionRequest,
        raw_request: Optional[Request] = None
    ) -> AsyncGenerator[Union[str, ChatCompletionResponse, ErrorResponse], None]:
        """Handle chat completion request."""
    
    @abstractmethod
    async def completions(
        self,
        request: CompletionRequest,
        raw_request: Optional[Request] = None
    ) -> AsyncGenerator[Union[str, CompletionResponse, ErrorResponse], None]:
        """Handle text completion request."""
    
    @abstractmethod
    async def embeddings(
        self,
        request: EmbeddingRequest,
        raw_request: Optional[Request] = None
    ) -> AsyncGenerator[Union[EmbeddingResponse, ErrorResponse], None]:
        """Handle embedding request."""
```

This protocol ensures that all LLM server implementations (`LLMServer`, `DPServer`, `PDProxyServer`) provide consistent methods for handling requests.

## Builder pattern

Ray Serve LLM uses the builder pattern to separate class definition from deployment decoration. This provides flexibility and testability.

**Key principle**: Classes aren't decorated with `@serve.deployment`. Decoration happens in builder functions.

### Why use builders?

Builders provide two key benefits:

1. **Flexibility**: Different deployment configurations for the same class.
2. **Production readiness**: You can use builders in YAML files and run `serve run config.yaml` with the target builder module.

### Builder example

```python
def my_build_function(
    llm_config: LLMConfig,
) -> Deployment:
    # Get default options from the class
    serve_options = LLMServer.get_deployment_options(llm_config)
    
    # Merge with user-provided options
    serve_options.update(kwargs)

    # Decorate and bind
    return serve.deployment(deployment_cls).options(
        **serve_options
    ).bind(llm_config)
```

You can use the builder function in two ways:

::::{tab-set}

:::{tab-item} Python
:sync: python

```python
# serve.py
from ray import serve
from ray.serve.llm import LLMConfig
from my_module import my_build_function

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="qwen-0.5b",
        model_source="Qwen/Qwen2.5-0.5B-Instruct",
    ),
    accelerator_type="A10G",
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1,
            max_replicas=2,
        )
    ),
)

app = my_build_function(llm_config)
serve.run(app)
```

Run the deployment:

```bash
python serve.py
```
:::

:::{tab-item} YAML
:sync: yaml

```yaml
# config.yaml
applications:
- args:
    llm_config:
      model_loading_config:
        model_id: qwen-0.5b
        model_source: Qwen/Qwen2.5-0.5B-Instruct
      accelerator_type: A10G
      deployment_config:
        autoscaling_config:
          min_replicas: 1
          max_replicas: 2
  import_path: my_module:my_build_function
  name: custom_llm_deployment
  route_prefix: /
```

Run the deployment:

```bash
serve run config.yaml
```
:::

::::

## Async constructor pattern

`LLMServer` uses an async constructor to handle engine initialization. This pattern ensures the engine is fully started before the deployment begins serving requests.

```python
class LLMServer(LLMServerProtocol):
    """LLM server deployment."""
    
    async def __init__(self, llm_config: LLMConfig, **kwargs):
        """Async constructor - returns fully started instance.
        
        Ray Serve calls this constructor when creating replicas.
        By the time this returns, the engine is ready to serve.
        """
        super().__init__()
        self._init_shared(llm_config, **kwargs)
        await self.start()  # Start engine immediately
    
    def _init_shared(self, llm_config: LLMConfig, **kwargs):
        """Shared initialization logic."""
        self._llm_config = llm_config
        self._engine_cls = self._get_engine_class()
        # ... other initialization
    
    async def start(self):
        """Start the underlying engine."""
        self.engine = self._engine_cls(self._llm_config)
        await asyncio.wait_for(
            self._start_engine(), 
            timeout=600
        )
    
    @classmethod
    def sync_init(cls, llm_config: LLMConfig, **kwargs) -> "LLMServer":
        """Sync constructor for testing.
        
        Returns unstarted instance. Caller must call await start().
        """
        instance = cls.__new__(cls)
        LLMServerProtocol.__init__(instance)
        instance._init_shared(llm_config, **kwargs)
        return instance  # Not started yet!
```

### Why use async constructors?

Async constructors provide several benefits:

1. **Engine initialization is async**: Loading models and allocating GPU memory takes time.
2. **Failure detection**: If the engine fails to start, the replica fails immediately.
3. **Explicit control**: Clear distinction between when the server is ready versus initializing.
4. **Testing flexibility**: `sync_init` allows testing without engine startup.

## Component relationships

The following diagram shows how core components relate to each other:

```
┌─────────────────────────────────────────────────────────┐
│                  RAY SERVE (Foundation)                 │
│     @serve.deployment | DeploymentHandle | Routing      │
└────────────────────────┬────────────────────────────────┘
                         │
      ┌──────────────────┼──────────────────┐
      │                  │                  │
      ▼                  ▼                  ▼
┌──────────┐      ┌──────────┐         ┌──────────┐
│ Protocol │      │ Ingress  │         │ Config   │
│          │      │          │         │          │
│ • Deploy │      │ • OpenAI │         │ • LLM    │
│   Proto  │      │   API    │         │   Config │
│ • Server │      │ • Model  │         │ • Model  │
│   Proto  │      │   Routing│         │   Loading│
└─────┬────┘      └────┬─────┘         └────┬─────┘
      │                │                    │
      └────────┬───────┴────────────────────┘
               │
               ▼
        ┌─────────────┐
        │  LLMServer  │
        │             │
        │ Implements: │
        │ • Protocol  │
        │             │
        │ Uses:       │
        │ • Config    │
        │ • Engine    │
        └──────┬──────┘
               │
               ▼
        ┌─────────────┐
        │  LLMEngine  │
        │  (Protocol) │
        │             │
        │ Implemented │
        │ by:         │
        │ • VLLMEngine│
        │ • Future... │
        └─────────────┘
```

## Extension points

The core architecture provides several extension points:

### Custom engines

Implement `LLMEngine` protocol to support new inference backends:

```python
class MyCustomEngine(LLMEngine):
    """Custom engine implementation."""
    
    async def chat(self, request):
        # Your implementation
        pass
    
    # ... implement other methods
```

### Custom server implementations

Extend `LLMServer` or implement `LLMServerProtocol` directly:

```python
class CustomLLMServer(LLMServer):
    """Custom server with additional features."""
    
    async def chat(self, request, raw_request=None):
        # Add custom preprocessing
        modified_request = self.preprocess(request)
        
        # Call parent implementation
        async for chunk in super().chat(modified_request, raw_request):
            yield chunk
```

### Custom ingress

Implement your own ingress for custom API formats:

```python
from typing import List
from ray import serve
from ray.serve import DeploymentHandle

# Define your FastAPI app or Ray Serve application.
# For example: app = Application()

@serve.ingress(app)
class CustomIngress:
    """Custom ingress with non-OpenAI API."""
    
    def __init__(self, server_handles: List[DeploymentHandle]):
        self.handles = server_handles
    
    @app.post("/custom/endpoint")
    async def custom_endpoint(self, request: "CustomRequest"):
        # CustomRequest is a user-defined request model.
        # Your custom logic
        pass
```

### Custom builders

Create domain-specific builders for common patterns:

```python
def build_multimodal_deployment(
    model_config: dict,
    **kwargs
) -> Deployment:
    """Builder for multimodal models."""
    llm_config = LLMConfig(
        model_loading_config={
            "input_modality": InputModality.MULTIMODAL,
            **model_config
        },
        engine_kwargs={
            "task": "multimodal",
        }
    )
    return build_llm_deployment(llm_config, **kwargs)
```

These extension points allow you to customize Ray Serve LLM for specific use cases without modifying core code.

## See also

- {doc}`overview` - High-level architecture overview
- {doc}`serving-patterns/index` - Detailed serving pattern documentation
- {doc}`routing-policies` - Request routing architecture
- {doc}`../user-guides/index` - Practical deployment guides

