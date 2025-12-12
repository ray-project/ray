from ray.serve.llm import LLMConfig, build_llm_deployment, build_openai_app
from ray.serve.llm.ingress import OpenAiIngress, make_fastapi_ingress
from ray.llm._internal.serve.utils.dispatch import dispatch
from ray.llm._internal.serve.core.ingress.ingress import DEFAULT_ENDPOINTS
from ray import serve
from fastapi import Request
import asyncio
import httpx
import time


# Create a custom OpenAiIngress that exposes an endpoint for kv-cache reset
class MyOpenAiIngress(OpenAiIngress):
    
    async def reset_prefix_cache(self, request: Request):
        """Reset the KV cache on all replicas."""
        
        model_id = request.query_params.get("model")
        handle = self._get_configured_serve_handle(model_id)
        dispatch(handle, "reset_prefix_cache")
        

# Extend the default endpoints with the new endpoint
CUSTOM_ENDPOINTS = {
    "reset_prefix_cache": lambda app: app.post("/reset_prefix_cache"),
    **DEFAULT_ENDPOINTS,
}

def start_server(model: str):
    llm_config = LLMConfig(
        model_loading_config=dict(
            model_id=model,
        ),
        deployment_config=dict(
            num_replicas=2,
            name="llm"
        ),
        engine_kwargs=dict(
            enable_prefix_caching=True,
            enforce_eager=True,
            max_num_batched_tokens=128,
        )
    )
    
    # Build the LLM deployment and ingress
    llm_deployment = build_llm_deployment(llm_config)
    ingress_cls = make_fastapi_ingress(
        MyOpenAiIngress, endpoint_map=CUSTOM_ENDPOINTS)
    ingress_options = MyOpenAiIngress.get_deployment_options([llm_config])
    ingress_app = serve.deployment(ingress_cls, **ingress_options).bind(
        llm_deployments=[llm_deployment],
    )
    serve.run(ingress_app, blocking=True)

async def main():
    model = "Qwen/Qwen2.5-0.5B-Instruct"
    
    # Start the server
    start_server(model)
    

if __name__ == "__main__":
    asyncio.run(main())
    
    