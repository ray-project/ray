"""Multi-node SGLang serving example with tensor and pipeline parallelism.

Requirements:
    - 2 nodes with 4 GPUs each (8 GPUs total for tp_size=4, pp_size=2)
    - pip install ray[serve,llm] "sglang[all,ray]"
    - Set RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES=0

Usage:
    RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES=0 serve run serve_sglang_multinode_example:app
"""

from ray import serve
from ray.llm._internal.serve.engines.sglang import SGLangServer
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
    # SGLangServer (RayEngine) requires one bundle per node, with each
    # bundle holding that node's full GPU allocation. RayEngine indexes the
    # placement group by node, so every tp/pp rank assigned to a given node
    # reuses the same bundle index. With 2 nodes of 4 GPUs each, that means
    # 2 bundles of {"GPU": 4}; STRICT_PACK keeps each bundle on a single node.
    placement_group_config={
        "placement_group_bundles": [
            {"CPU": 1, "GPU": 4},
            {"CPU": 1, "GPU": 4},
        ],
        "placement_group_strategy": "STRICT_PACK",
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

if __name__ == "__main__":
    serve.run(app, blocking=True)
