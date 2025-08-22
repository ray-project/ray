# serve_deepseek_r1.py
from ray.serve.llm import LLMConfig, build_openai_app

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="my-deepseek-r1",
        model_source="deepseek-ai/DeepSeek-R1",
    ),
    accelerator_type="H100",
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1,
            max_replicas=1,
        )
    ),
    engine_kwargs=dict(
        max_model_len=32768,
        ### Uncomment if your model is gated and need your Huggingface Token to access it
        # hf_token=os.environ.get("HF_TOKEN"),
        # Split weights among 8 GPUs in the node
        tensor_parallel_size=8,
        pipeline_parallel_size=2,
    ),
)

app = build_openai_app({"llm_configs": [llm_config]})
