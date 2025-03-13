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

    processor_config = vLLMEngineProcessorConfig(
        model_source="unsloth/Llama-3.1-8B-Instruct",
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

    processor = build_llm_processor(
        processor_config,
        preprocess=lambda row: dict(
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
