import ray
from ray.data.llm import build_llm_processor, vLLMEngineProcessorConfig
from args_utils import get_parser


def main(args):
    tp_size = args.tp_size
    pp_size = args.pp_size
    concurrency = args.concurrency
    vllm_use_v1 = args.vllm_use_v1

    if vllm_use_v1:
        runtime_env = dict(
            env_vars=dict(
                VLLM_USE_V1="1",
            ),
        )
        # vLLM v1 does not support decoupled tokenizer,
        # but since the tokenizer is in a separate process,
        # the overhead should be moderated.
        tokenize = False
        detokenize = False
    else:
        runtime_env = {}
        tokenize = True
        detokenize = True

    process_config_dict = dict(
        model_source=args.model_source,
        engine_kwargs=dict(
            tensor_parallel_size=tp_size,
            pipeline_parallel_size=pp_size,
            max_model_len=16384,
            enable_chunked_prefill=True,
            max_num_batched_tokens=2048,
            trust_remote_code=True,
        ),
        runtime_env=runtime_env,
        tokenize=tokenize,
        detokenize=detokenize,
        batch_size=16,
        accelerator_type=None,
        concurrency=concurrency,
    )

    # Enable LoRA.
    if args.dynamic_lora_loading_path:
        if args.lora_name is None:
            raise ValueError(
                "lora_name must be specified if dynamic_lora_loading_path is provided"
            )
        if args.max_lora_rank is None:
            raise ValueError(
                "max_lora_rank must be specified if dynamic_lora_loading_path is provided"
            )

        process_config_dict[
            "dynamic_lora_loading_path"
        ] = args.dynamic_lora_loading_path
        process_config_dict["engine_kwargs"]["enable_lora"] = True
        process_config_dict["engine_kwargs"]["max_lora_rank"] = args.max_lora_rank

    processor_config = vLLMEngineProcessorConfig(**process_config_dict)

    processor = build_llm_processor(
        processor_config,
        preprocess=lambda row: dict(
            model=args.model_source if args.lora_name is None else args.lora_name,
            messages=[
                {"role": "system", "content": "You are a calculator"},
                {"role": "user", "content": f"{row['id']} ** 3 = ?"},
            ],
            sampling_params=dict(
                temperature=0.3,
                max_tokens=50,
                detokenize=False,
            ),
        ),
        postprocess=lambda row: {
            "resp": row["generated_text"],
        },
    )

    # Chain the pipeline.
    ds = ray.data.range(120)
    ds = ds.map(lambda x: {"id": x["id"], "val": x["id"] + 5})
    ds = processor(ds)
    ds = ds.materialize()
    sampled = ds.limit(1)
    print(sampled)


if __name__ == "__main__":
    parser = get_parser()
    parser.add_argument(
        "--model-source",
        type=str,
        default="unsloth/Llama-3.1-8B-Instruct",
        help="Model source.",
    )
    main(parser.parse_args())
