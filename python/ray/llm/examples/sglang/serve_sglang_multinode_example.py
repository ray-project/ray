"""Multi-node SGLang serving example with tensor and pipeline parallelism.

This is a demonstration and reference only. It is not actively maintained
and is not part of Ray's officially supported feature set.
See https://github.com/ray-project/ray/issues/61114 for status.

Requirements:
    - 2 nodes with 4 GPUs each (8 GPUs total for tp_size=4, pp_size=2)
    - pip install ray[serve,llm] "sglang[all]"
    - Set RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES=0

Usage:
    RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES=0 serve run serve_sglang_multinode_example:app
"""

from modules.sglang_engine import SGLangServer

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
    # SPREAD distributes GPU bundles across nodes for multi-node TP/PP.
    placement_group_config={
        "placement_group_bundles": [{"GPU": 1}] * 8,
        "placement_group_strategy": "SPREAD",
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
serve.start()
serve.run(app, blocking=True)
