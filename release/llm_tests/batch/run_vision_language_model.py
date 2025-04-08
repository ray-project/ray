"""
Runs a vision language model with image inputs.

Usage:
python run_vision_language_model.py --tp-size 1 --pp-size 1 --concurrency 1
"""

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

    processor_config = vLLMEngineProcessorConfig(
        model_source=args.model_source,
        task_type="generate",
        engine_kwargs=dict(
            tensor_parallel_size=tp_size,
            pipeline_parallel_size=pp_size,
            max_model_len=4096,
            enable_chunked_prefill=True,
        ),
        apply_chat_template=True,
        runtime_env=runtime_env,
        tokenize=tokenize,
        detokenize=detokenize,
        batch_size=16,
        accelerator_type=None,
        concurrency=concurrency,
        has_image=True,
    )

    processor = build_llm_processor(
        processor_config,
        preprocess=lambda row: dict(
            model=args.model_source if args.lora_name is None else args.lora_name,
            messages=[
                {"role": "system", "content": "You are an assistant"},
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": f"Say {row['id']} words about this image.",
                        },
                        {
                            "type": "image",
                            "image": "https://vllm-public-assets.s3.us-west-2.amazonaws.com/vision_model_images/cherry_blossom.jpg",
                        },
                    ],
                },
            ],
            sampling_params=dict(
                temperature=0.3,
                max_tokens=50,
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
        default="llava-hf/llava-1.5-7b-hf",
        help="Model source.",
    )
    main(parser.parse_args())
