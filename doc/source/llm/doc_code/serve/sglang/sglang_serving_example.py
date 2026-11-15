# __sglang_single_node_start__
from ray.llm._internal.serve.engines.sglang import SGLangServer

from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app

llm_config = LLMConfig(
    model_loading_config={
        "model_id": "qwen3.5-0.8b",
        "model_source": "Qwen/Qwen3.5-0.8B",
    },
    deployment_config={
        "autoscaling_config": {
            "min_replicas": 1,
            "max_replicas": 2,
        }
    },
    server_cls=SGLangServer,
    engine_kwargs={
        "trust_remote_code": True,
        "model_path": "Qwen/Qwen3.5-0.8B",
        "tp_size": 1,
        "mem_fraction_static": 0.8,
    },
)

app = build_openai_app({"llm_configs": [llm_config]})
serve.run(app, blocking=True)
# __sglang_single_node_end__
