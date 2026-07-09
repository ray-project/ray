# __sglang_multinode_start__
from ray.llm._internal.serve.engines.sglang import SGLangServer

from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app

llm_config = LLMConfig(
    model_loading_config={
        "model_id": "Llama-3.1-70B-Instruct",
        "model_source": "meta-llama/Llama-3.1-70B-Instruct",
    },
    deployment_config={
        "autoscaling_config": {
            "min_replicas": 1,
            "max_replicas": 2,
            "target_ongoing_requests": 4,
        }
    },
    # PACK fills GPUs on each node before moving to the next.
    # With 8 bundles across 2 nodes (4 GPUs each), each node gets 4 bundles.
    placement_group_config={
        "placement_group_bundles": [{"CPU": 1, "GPU": 1}] + [{"GPU": 1}] * 7,
        "placement_group_strategy": "PACK",
    },
    server_cls=SGLangServer,
    engine_kwargs={
        "model_path": "meta-llama/Llama-3.1-70B-Instruct",
        "tp_size": 4,
        "pp_size": 2,
        "mem_fraction_static": 0.8,
    },
)

app = build_openai_app({"llm_configs": [llm_config]})
serve.run(app, blocking=True)
# __sglang_multinode_end__
