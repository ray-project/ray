from modules.sglang_engine import SGLangServer

from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app

llm_config = LLMConfig(
    model_loading_config={
        "model_id": "Llama-3.1-8B-Instruct",
        "model_source": "unsloth/Llama-3.1-8B-Instruct",
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
        "model_path": "unsloth/Llama-3.1-8B-Instruct",
        "tp_size": 1,
        "mem_fraction_static": 0.8,
    },
)

app = build_openai_app({"llm_configs": [llm_config]})
serve.start()
serve.run(app, blocking=True)
