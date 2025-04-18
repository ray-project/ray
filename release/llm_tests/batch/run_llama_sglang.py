import argparse

import ray
from ray.data.llm import build_llm_processor, SGLangEngineProcessorConfig


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--tp-size",
        type=int,
        default=1,
        help="Tensor parallel size",
    )
    parser.add_argument(
        "--dp-size",
        type=int,
        default=1,
        help="Data parallel size.",
    )
    parser.add_argument(
        "--concurrency", type=int, default=1, help="Number of concurrency (replicas)."
    )

    return parser.parse_args()


def main(args):
    tp_size = args.tp_size
    dp_size = args.dp_size
    concurrency = args.concurrency

    runtime_env = {}
    tokenize = True
    detokenize = True

    processor_config = SGLangEngineProcessorConfig(
        model_source="unsloth/Llama-3.1-8B-Instruct",
        engine_kwargs=dict(
            tp_size=tp_size,
            dp_size=dp_size,
            skip_tokenizer_init=True,
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
                max_new_tokens=50,
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
