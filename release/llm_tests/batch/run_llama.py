import argparse

import ray
from ray.data.llm import build_llm_processor, vLLMEngineProcessorConfig


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--tp-size",
        type=int,
        default=1,
        help="Tensor parallel size",
    )
    parser.add_argument(
        "--pp-size",
        type=int,
        default=1,
        help="Pipeline parallel size.",
    )
    parser.add_argument(
        "--concurrency", type=int, default=1, help="Number of concurrency (replicas)."
    )
    parser.add_argument(
        "--vllm-use-v1",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--model-source",
        type=str,
        default="unsloth/Llama-3.1-8B-Instruct",
        help="Model source.",
    )
    parser.add_argument(
        "--dynamic-lora-loading-path",
        type=str,
        default=None,
        help="Path to the dynamic lora loading.",
    )
    parser.add_argument(
        "--lora-name",
        type=str,
        default=None,
        help="Name of the lora to load.",
    )
    parser.add_argument(
        "--max-lora-rank",
        type=int,
        default=None,
        help="Max lora rank.",
    )

    return parser.parse_args()


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

    runtime_env.update(
        dict(
            env_vars=dict(
                # Add your HF token here if you want to use a gated model e.g.
                # meta-llama/Llama-3.1-8B-Instruct
                # HF_TOKEN="hf_xxxxxxxxx",
            ),
        )
    )
    process_config_dict = dict(
        model_source=args.model_source,
        engine_kwargs=dict(
            tensor_parallel_size=tp_size,
            pipeline_parallel_size=pp_size,
            max_model_len=16384,
            enable_chunked_prefill=True,
            max_num_batched_tokens=2048,
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
    args = parse_args()
    main(args)
