import pytest
import requests

import ray
from ray import serve
from ray.serve._private.utils import get_head_node_id
from ray.serve.llm import LLMConfig
from ray.serve.llm.request_router import KVAwareRouter

from utils import build_kv_app, patch_ingress


@pytest.mark.asyncio
@pytest.mark.timeout(600)
async def test_frontend_config_validation():
    if not ray.is_initialized():
        ray.init(address="auto")
    head_id = get_head_node_id()
    head = next(node for node in ray.nodes() if node["NodeID"] == head_id)
    assert head["Resources"].get("GPU", 0) == 0, "This test requires a CPU-only head"

    serve.shutdown()
    app_name = "frontend_config_test"
    llm_config = LLMConfig(
        model_loading_config=dict(model_id="Qwen/Qwen3-4B"),
        deployment_config=dict(
            num_replicas=1,
            request_router_config=dict(request_router_class=KVAwareRouter),
        ),
        engine_kwargs=dict(
            max_model_len=2048,
            max_num_seqs=16,
            enforce_eager=True,
            gpu_memory_utilization=0.7,
            default_chat_template_kwargs={"enable_thinking": False},
            speculative_config=dict(
                method="dflash",
                model="RedHatAI/Qwen3-4B-speculator.dflash2",
                num_speculative_tokens=8,
            ),
        ),
    )
    try:
        with patch_ingress():
            serve.run(build_kv_app(llm_config), name=app_name)
        response = requests.post(
            f"http://{head['NodeManagerAddress']}:8000/v1/chat/completions",
            json={
                "model": llm_config.model_id,
                "messages": [{"role": "user", "content": "Say hello."}],
                "max_tokens": 16,
            },
            timeout=120,
        )
        assert response.status_code == 200, response.text
        assert response.json()["usage"]["completion_tokens"] > 0
        router = serve.get_deployment_handle("LLMRouter", app_name=app_name)
        reports = await router.broadcast("get_token_push_report").results_async()
        assert (
            sum(
                report["tokenizations"]
                for report in reports
                if report["node_ip"] == head["NodeManagerAddress"]
            )
            > 0  # Zero tokenizations means we didn't observe tokenization on the head
        ), "Expected pre-routing tokenization on the head node"
    finally:
        serve.shutdown()
