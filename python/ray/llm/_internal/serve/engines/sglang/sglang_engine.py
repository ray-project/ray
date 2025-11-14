import ray
import requests
from ray import serve
from ray.serve.handle import DeploymentHandle
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig

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

from ray.serve.llm.openai_api_models import (
    CompletionResponse,
    CompletionChoice,
    CompletionUsage,
)

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
    '''
    async def chat(self, message: str):
        print('In SGLangServer CHAT with message', message)
        res = await self.engine.async_generate(
            prompt = message,
            stream = False
        )
        return {"echo": res}
    '''
    async def completions(self, request) -> AsyncGenerator[CompletionResponse, None]:
        """Implements the LLMEngine.completions protocol for Ray LLM."""
        print("In SGLangServer COMPLETIONS with request", request)

        # ---- 1. Get prompt from CompletionRequest ----
        prompt = request.prompt
        if isinstance(prompt, list):
            prompt = prompt[0]

        # ---- 2. Call SGLang (non-streaming) ----
        # SGLang's async_generate usually returns a list of dicts.
        raw = await self.engine.async_generate(
            prompt=prompt,
            stream=False,
        )

        if isinstance(raw, list):
            raw = raw[0]

        # SGLang output looks like:
        # {
        #   "text": "...",
        #   "output_ids": [...],
        #   "meta_info": {
        #       "id": "...",
        #       "finish_reason": {"type": "length"},
        #       "prompt_tokens": 5,
        #       "completion_tokens": 30,
        #       ...
        #   },
        # }
        text: str = raw.get("text", "")
        meta: dict[str, Any] = raw.get("meta_info", {}) or {}
        finish_reason_info = meta.get("finish_reason", {}) or {}

        prompt_tokens = int(meta.get("prompt_tokens", 0))
        completion_tokens = int(meta.get("completion_tokens", 0))
        total_tokens = prompt_tokens + completion_tokens

        # ---- 3. Build OpenAI-style response objects ----
        usage = CompletionUsage(
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=total_tokens,
        )

        choice = CompletionChoice(
            index=0,
            text=text,
            logprobs=None,
            finish_reason=finish_reason_info.get("type", "length"),
        )

        resp = CompletionResponse(
            id=meta.get("id", "sglang-completion"),
            object="text_completion",
            created=int(time.time()),
            model=request.model,        # comes from the HTTP payload ("8B")
            choices=[choice],
            usage=usage,
        )

        # ---- 4. Yield the CompletionResponse (NOT a dict) ----
        yield resp
    '''
    async def chat(self, request):
        # request.messages is a list of {role, content}
        content_parts = [m["content"] for m in request.messages]
        prompt = "\n".join(content_parts)

        res = await self.engine.async_generate(prompt=prompt, stream=False)
        return {"echo": res}

    async def completions(self, request) -> AsyncGenerator[Dict[str, Any], None]:
        """Ray calls this with a CompletionRequest and expects an *async generator*."""
        print("In SGLangServer COMPLETIONS with request", request)

        # 1. Extract prompt
        prompt = request.prompt
        if isinstance(prompt, list):
            # OpenAI-style API allows list of prompts; simplest is to use the first.
            prompt = prompt[0]

        # 2. Map a few fields from the request into SGLang sampling params (optional)
        sampling_params = {
            "max_new_tokens": request.max_tokens or 16,
            "temperature": request.temperature if request.temperature is not None else 1.0,
        }

        # 3. Call SGLang in non-streaming async mode
        #    For a single prompt, async_generate usually returns a list of one output.
        outputs = await self.engine.async_generate(
            prompt,
            sampling_params,
            stream=False,
        )

        # SGLang offline engine typically returns a list of dicts with "text"
        if isinstance(outputs, list) and outputs:
            text = outputs[0].get("text", str(outputs[0]))
        else:
            # Fallback if the shape is different
            text = str(outputs)

        # 4. Yield ONE message (makes this an async generator)
        yield {
            "echo": text
        }
        # function ends here; generator is exhausted
    '''

    async def llm_config(self) -> Optional[LLMConfig]:
        return self._llm_config

    @classmethod
    def get_deployment_options(cls, _llm_config: "LLMConfig"):
        return {'autoscaling_config': {'min_replicas': 1, 'max_replicas': 1}, 
                'placement_group_bundles': [{'CPU': 1, 'GPU': 1, 'accelerator_type:H200': 0.001}, {'GPU': 1, 'accelerator_type:H200': 0.001}], 
                'placement_group_strategy': 'PACK', 
                'ray_actor_options': {'runtime_env': 
                                      {'worker_process_setup_hook': 'ray.llm._internal.serve._worker_process_setup_hook'}
                                      }
                }

#sglangServer = SGLangServer.bind()
#my_App = MyFastAPIDeployment.bind(sglangServer)
#handle: DeploymentHandle = serve.run(my_App, blocking = True)
