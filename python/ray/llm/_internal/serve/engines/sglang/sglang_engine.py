import ray
import requests
from ray import serve
from pydantic import BaseModel
from ray.serve.handle import DeploymentHandle
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
import time
import copy
from enum import Enum
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


# --- Replacement Models for OpenAI Schema ---
# --- Helper Models (Necessary to make the code below work) ---


class CompletionUsage(BaseModel): # Assuming you have this defined
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int

class ChatRole(str, Enum):
    user = "user"
    assistant = "assistant"
    system = "system"

class ChatMessage(BaseModel):
    role: ChatRole
    content: Optional[str] = None

class ChatCompletionChoice(BaseModel):
    index: int
    message: ChatMessage
    finish_reason: Optional[str] = None

class ChatCompletionResponse(BaseModel):
    id: str
    object: str = "chat.completion"
    created: int
    model: str
    choices: List[ChatCompletionChoice]
    usage: Optional[CompletionUsage] = None

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

class ChatCompletionRequest(BaseModel):
    model: str
    messages: List[ChatMessage]
    temperature: Optional[float] = 1.0
    top_p: Optional[float] = 1.0
    max_tokens: Optional[int] = 16
    stop: Optional[Union[str, List[str]]] = None

    @property
    def text(self) -> str:
        """
        CRITICAL SGLang Fix: Provides the formatted prompt string. 
        SGLang's internal io_struct.py needs this to determine the batch size 
        and treat the request as a valid input object.
        """
        # Assumes format_messages_to_prompt is defined globally or imported
        return format_messages_to_prompt(self.messages)

    def normalize_batch_and_arguments(self):
        """
        CRITICAL SGLang Fix: This stub satisfies the SGLang internal logic 
        that attempts to call this method on the input object.
        """
        pass

# --- CHAT TEMPLATE HELPER (MUST BE DEFINED) ---
def format_messages_to_prompt(messages: List[ChatMessage]) -> str:
    """Converts a list of ChatMessage objects (or dicts) into a single prompt string."""
    prompt = "A conversation between a user and an assistant.\n"
    
    for message in messages:
        # CRITICAL FIX: Use .get() method if it's a dict, or access the attribute if it's a Pydantic object.
        # Since the traceback shows 'dict' is the type, we must access it like a dictionary.
        
        # Access role and content defensively:
        if isinstance(message, dict):
            role = message.get("role")
            content = message.get("content", "")
        else:
            # Assumes it's the ChatMessage Pydantic object if not a dict
            role = message.role
            content = message.content or "" 
            
        # Ensure 'role' is treated as a string for comparison
        role_str = str(role)

        if role_str == ChatRole.system.value: # Use .value for enum comparison
            prompt += f"### System: {content.strip()}\n"
        elif role_str == ChatRole.user.value:
            prompt += f"### User: {content.strip()}\n"
        elif role_str == ChatRole.assistant.value:
            prompt += f"### Assistant: {content.strip()}\n"

    prompt += "### Assistant:"
    return prompt

# --- FIX: Updated Completion Request Model ---
class CompletionRequest(BaseModel):
    model: str
    prompt: Union[str, List[str]]
    temperature: Optional[float] = 1.0
    top_p: Optional[float] = 1.0
    max_tokens: Optional[int] = 16
    stop: Optional[Union[str, List[str]]] = None

    @property
    def text(self) -> str:
        """CRITICAL FIX: Provides the prompt string for SGLang's internal logic."""
        prompt = self.prompt
        if isinstance(prompt, list):
            return prompt[0] # Assuming non-batched completions
        return prompt

    def normalize_batch_and_arguments(self):
        """CRITICAL FIX: Placeholder stub for SGLang internal logic."""
        pass

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
    '''
    async def chat(self, message: str):
        print('In SGLangServer CHAT with message', message)
        res = await self.engine.async_generate(
            prompt = message,
            stream = False
        )
        return {"echo": res}
    '''

    async def chat(
        self, request: ChatCompletionRequest
    ) -> AsyncGenerator[ChatCompletionResponse, None]:
        
        # 1. CRITICAL FIX: Explicitly call the formatter on the raw messages list.
        # This bypasses the AttributeError on request.text.
        prompt_string = format_messages_to_prompt(request.messages)

        # 2. Extract sampling parameters (using your existing logic)
        temp = request.temperature
        if temp is None: temp = 0.7
        top_p = request.top_p
        if top_p is None: top_p = 1.0 
        max_tokens = request.max_tokens
        if max_tokens is None: max_tokens = 128
        
        sampling_params = {
            "temperature": temp,
            "max_new_tokens": max_tokens,
            "stop": request.stop,
            "top_p": top_p,
        }


        # 3. CRITICAL FIX: Pass the raw 'prompt_string' to the SGLang engine.
        # This avoids the internal SGLang error with 'len(request.text)'.
        raw = await self.engine.async_generate(
            prompt=prompt_string, # <-- Pass the string directly here!
            sampling_params=sampling_params,
            stream=False,
        )
        
        if isinstance(raw, list):
            raw = raw[0]

        # ... (SGLang output processing remains the same) ...
        text: str = raw.get("text", "")
        meta: dict[str, Any] = raw.get("meta_info", {}) or {}
        finish_reason_info = meta.get("finish_reason", {}) or {}

        if isinstance(finish_reason_info, dict):
             finish_reason = finish_reason_info.get("type", "length")
        else:
             finish_reason = str(finish_reason_info)

        prompt_tokens = int(meta.get("prompt_tokens", 0))
        completion_tokens = int(meta.get("completion_tokens", 0))
        total_tokens = prompt_tokens + completion_tokens

        # 4. CRITICAL RESPONSE MODEL FIX: Use ChatCompletionResponse and ChatCompletionChoice
        usage = CompletionUsage(
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=total_tokens,
        )

        assistant_message = ChatMessage(
            role=ChatRole.assistant,
            content=text.strip()
        )

        choice = ChatCompletionChoice( # <-- CORRECT MODEL
            index=0,
            message=assistant_message,
            finish_reason=finish_reason,
        )

        resp = ChatCompletionResponse( # <-- CORRECT MODEL
            id=meta.get("id", f"sglang-chat-{int(time.time())}"),
            object="chat.completion", # <-- CORRECT OBJECT TYPE
            created=int(time.time()),
            model=request.model,
            choices=[choice],
            usage=usage,
        )

        yield resp

    async def completions(self, request) -> AsyncGenerator[CompletionResponse, None]:
        print(f"In SGLangServer COMPLETIONS with request: {request}")

        # 1. CRITICAL FIX: Extract the prompt string directly.
        # We must access the .prompt attribute and handle potential lists, 
        # as Ray's internal Pydantic model doesn't have our custom .text property.
        prompt_input = request.prompt
        
        if isinstance(prompt_input, list):
            # SGLang typically expects a single string input here for simple completion
            prompt_string = prompt_input[0] 
        else:
            prompt_string = prompt_input 
        
        # ---- 2. Robust Parameter Extraction ----
        temp = getattr(request, "temperature", None)
        if temp is None: temp = 0.7

        top_p = getattr(request, "top_p", None)
        if top_p is None: top_p = 1.0

        max_tokens = getattr(request, "max_tokens", None)
        if max_tokens is None: max_tokens = 128

        # Stop sequences can be provided as a string or a list
        stop_sequences = getattr(request, "stop", None)

        sampling_params = {
            "temperature": temp,
            "max_new_tokens": max_tokens,
            "stop": stop_sequences,
            "top_p": top_p,
        }

        # Log params to debug future issues
        print(f"Generating with params: {sampling_params}")

        # ---- 3. Call SGLang ----
        # CRITICAL FIX: Pass the raw prompt string as the 'prompt' keyword argument.
        raw = await self.engine.async_generate(
            prompt=prompt_string, # <-- This avoids the TypeError inside SGLang
            sampling_params=sampling_params,
            stream=False,
        )
        
        # Handle single or list response from SGLang
        if isinstance(raw, list):
            raw = raw[0]

        # ---- 4. SGLang Output Processing ----
        text: str = raw.get("text", "")
        meta: dict[str, Any] = raw.get("meta_info", {}) or {}
        finish_reason_info = meta.get("finish_reason", {}) or {}

        # Standardize finish reason extraction
        if isinstance(finish_reason_info, dict):
             finish_reason = finish_reason_info.get("type", "length")
        else:
             finish_reason = str(finish_reason_info)

        prompt_tokens = int(meta.get("prompt_tokens", 0))
        completion_tokens = int(meta.get("completion_tokens", 0))
        total_tokens = prompt_tokens + completion_tokens

        # ---- 5. Build OpenAI-style response objects ----
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
            id=meta.get("id", f"sglang-comp-{int(time.time())}"),
            object="text_completion",
            created=int(time.time()),
            model=getattr(request, "model", "default_model"), # Use default if model isn't present
            choices=[choice],
            usage=usage,
        )

        # ---- 6. Yield the CompletionResponse ----
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

