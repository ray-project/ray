# serve_gpt_oss.py
from ray.serve.llm import LLMConfig, build_openai_app

GPT_OSS_SIZE = "20b"  # or "120b"

if GPT_OSS_SIZE == "20b":
    llm_config = LLMConfig(
        model_loading_config=dict(
            model_id="my-gpt-oss",
            model_source="openai/gpt-oss-20b",
        ),
        accelerator_type="L4",
        deployment_config=dict(
            autoscaling_config=dict(
                min_replicas=1,
                max_replicas=2,
            )
        ),
        engine_kwargs=dict(
            max_model_len=32768,
        ),
    )

elif GPT_OSS_SIZE == "120b":
    llm_config = LLMConfig(
        model_loading_config=dict(
            model_id="my-gpt-oss",
            model_source="openai/gpt-oss-120b",
        ),
        accelerator_type="A100-80G",
        deployment_config=dict(
            autoscaling_config=dict(
                min_replicas=1,
                max_replicas=2,
            )
        ),
        engine_kwargs=dict(
            max_model_len=65536,
            tensor_parallel_size=2,
        ),
    )

else:
    raise ValueError("GPT_OSS_SIZE must be either '20b' or '120b'")

app = build_openai_app({"llm_configs": [llm_config]})
