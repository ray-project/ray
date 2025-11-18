import ray
import requests
from ray import serve
from ray.serve.handle import DeploymentHandle
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
import time
import copy
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Dict,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
)
from pydantic import BaseModel

# --- Replacement Models for OpenAI Schema ---

class CompletionUsage(BaseModel):
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int

class CompletionChoice(BaseModel):
    index: int
    text: str
    logprobs: Optional[Any] = None
    finish_reason: Optional[str] = None

class CompletionResponse(BaseModel):
    id: str
    object: str = "text_completion"
    created: int
    model: str
    choices: List[CompletionChoice]
    usage: Optional[CompletionUsage] = None

#@serve.deployment disable serve.deployment
class SGLangServer:
    def __init__(self, _llm_config: LLMConfig):

        self._llm_config = _llm_config
        self.engine_kwargs = _llm_config.engine_kwargs

        try:
            import sglang
        except ImportError as e:
            raise ImportError(
                "SGLang is not installed or failed to import. Please run "
                "`pip install sglang[all]` to install required dependencies."
            ) from e
        self.engine = sglang.Engine(**self.engine_kwargs)
    
    async def chat(self, message: str):
        print('In SGLangServer CHAT with message', message)
        res = await self.engine.async_generate(
            prompt = message,
            stream = False
        )
        return {"echo": res}

    async def completions(self, request) -> AsyncGenerator[CompletionResponse, None]:
        print(f"In SGLangServer COMPLETIONS with request: {request}")

        prompt = request.prompt
        if isinstance(prompt, list):
            prompt = prompt[0]

        # ---- FIXED: Robust Parameter Extraction ----
        # We use a helper logic: (getattr(...) or default) won't work for temp=0.
        # We must check strictly for None.
        
        temp = getattr(request, "temperature", None)
        if temp is None: temp = 0.7

        top_p = getattr(request, "top_p", None)
        if top_p is None: top_p = 1.0  # <--- This prevents the NoneType error

        max_tokens = getattr(request, "max_tokens", None)
        if max_tokens is None: max_tokens = 128

        sampling_params = {
            "temperature": temp,
            "max_new_tokens": max_tokens,
            "stop": getattr(request, "stop", None),
            "top_p": top_p, 
        }
        
        # Log params to debug future issues
        print(f"Generating with params: {sampling_params}")

        # ---- Call SGLang ----
        raw = await self.engine.async_generate(
            prompt=prompt,
            sampling_params=sampling_params,
            stream=False,
        )
        if isinstance(raw, list):
            raw = raw[0]

        # SGLang output processing
        text: str = raw.get("text", "")
        meta: dict[str, Any] = raw.get("meta_info", {}) or {}
        finish_reason_info = meta.get("finish_reason", {}) or {}

        # Handle cases where meta_info might be structured differently
        if isinstance(finish_reason_info, dict):
             finish_reason = finish_reason_info.get("type", "length")
        else:
             finish_reason = str(finish_reason_info)

        prompt_tokens = int(meta.get("prompt_tokens", 0))
        completion_tokens = int(meta.get("completion_tokens", 0))
        total_tokens = prompt_tokens + completion_tokens

        # ---- 4. Build OpenAI-style response objects ----
        usage = CompletionUsage(
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=total_tokens,
        )

        choice = CompletionChoice(
            index=0,
            text=text,
            logprobs=None,
            finish_reason=finish_reason,
        )

        resp = CompletionResponse(
            id=meta.get("id", f"sglang-{int(time.time())}"),
            object="text_completion",
            created=int(time.time()),
            model=request.model,
            choices=[choice],
            usage=usage,
        )

        # ---- 5. Yield the CompletionResponse ----
        yield resp

    async def llm_config(self) -> Optional[LLMConfig]:
        return self._llm_config

    @classmethod
    def get_deployment_options(cls, llm_config: "LLMConfig"):
        
        # 1. Start with the base deployment configuration
        deployment_options = copy.deepcopy(llm_config.deployment_config)

        # 2. Extract necessary resource information from llm_config
        # (assuming the required PG bundles are stored under a 'placement_group_config' 
        # or you hardcode them, as in the first example you shared)
        
        # NOTE: You MUST define the 'placement_group_bundles' here, as Ray cannot generate them.
        # We will use the explicit bundles requested in your first example, 
        # which requested 2 GPUs (1 for controller, 1 for engine worker).
        
        # If using the 'placement_group_config' field in LLMConfig:
        pg_config = llm_config.placement_group_config or {}
        
        # Fallback to a necessary default if not provided, for this custom engine:
        if "placement_group_bundles" not in pg_config:
            # Define the bundles explicitly (adjust GPU count if necessary)
            pg_bundles = [
                {'CPU': 1, 'GPU': 1}, # Controller/Driver Bundle
                {'GPU': 1},           # Worker Bundle (if model is 2-way sharded)
            ]
            pg_strategy = "PACK"
        else:
            pg_bundles = pg_config.get("placement_group_bundles")
            pg_strategy = pg_config.get("placement_group_strategy", "PACK")


        # 3. Update the deployment options with the required Ray Serve fields
        deployment_options.update(
            {
                "placement_group_bundles": pg_bundles,
                "placement_group_strategy": pg_strategy,
            }
        )

        # 4. Handle ray_actor_options and runtime_env (simplified/fixed version)
        ray_actor_options = deployment_options.get("ray_actor_options", {})
        
        # Set the mandatory setup hook for LLM serving
        ray_actor_options.setdefault(
            "runtime_env", 
            {"worker_process_setup_hook": "ray.llm._internal.serve._worker_process_setup_hook"}
        )
        
        # Merge any other runtime_env settings from the LLMConfig
        existing_runtime_env = ray_actor_options["runtime_env"]
        if llm_config.runtime_env:
            existing_runtime_env.update(llm_config.runtime_env)

        deployment_options["ray_actor_options"] = ray_actor_options

        return deployment_options


#sglangServer = SGLangServer.bind()
#my_App = MyFastAPIDeployment.bind(sglangServer)
#handle: DeploymentHandle = serve.run(my_App, blocking = True)
