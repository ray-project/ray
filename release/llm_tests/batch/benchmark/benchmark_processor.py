#!/usr/bin/env python
"""
Benchmark Ray Data LLM offline batch inference throughput.

Sample usage:
python3 benchmark_processor.py --mode vllm_engine --batch-size 64 --concurrency 1 --num-prompts 10000 --model facebook/opt-1.3b
    --tensor-parallel-size 2 --pipeline-parallel-size 2 --distributed-executor-backend ray
"""

from dataclasses import dataclass
from time import perf_counter
from typing import Literal

import argparse
import sys

import ray
import ray.data as data
from ray.data.llm import vLLMEngineProcessorConfig, build_llm_processor

from dataset import ShareGPTDataset

Mode = Literal[
    "vllm_engine",
    "shared_vllm_engine",
]

# Default sampling parameters -- ensure a fair comparison by omitting sampling-induced variance
VLLM_SAMPLING_PARAMS = {
    "temperature": 1.0,
    "max_tokens": 100,
    "top_p": 1.0,
    "ignore_eos": True,
}

# Default vLLM engine kwargs
VLLM_ENGINE_KWARGS = {
    "enable_prefix_caching": True,
    "enable_chunked_prefill": True,
    "max_num_batched_tokens": 4096,
}


def build_vllm_engine_kwargs(**kwargs) -> dict:
    """Build vLLM engine kwargs from command line arguments."""
    engine_kwargs = VLLM_ENGINE_KWARGS.copy()
    engine_kwargs.update({k: v for k, v in kwargs.items() if v is not None})
    return engine_kwargs


@dataclass(slots=True)
class BenchmarkResult:
    mode: Mode
    batch_size: int
    concurrency: int
    samples: int
    elapsed_s: float

    @property
    def throughput(self) -> float:
        return self.samples / self.elapsed_s if self.elapsed_s else 0.0

    def show(self) -> None:
        print("\n" + "=" * 60)
        print(f"BENCHMARK - {self.mode}")
        print("=" * 60)
        print(f"Samples     : {self.samples}")
        print(f"Batch size  : {self.batch_size}")
        print(f"Concurrency : {self.concurrency}")
        print(f"Time (s)    : {self.elapsed_s:.2f}")
        print(f"Throughput  : {self.throughput:.2f} req/s")
        print("=" * 60)


# -----------------------------------------------------------------------------
# Processor builders
# -----------------------------------------------------------------------------


def build_single_vllm_engine_processor(
    batch_size: int,
    concurrency: int,
    model: str,
    sampling_params: dict = VLLM_SAMPLING_PARAMS,
    pipeline_parallel_size: int = None,
    tensor_parallel_size: int = None,
    distributed_executor_backend: str = None,
):
    """Build vLLM engine processor for single-stage benchmark."""
    return build_llm_processor(
        vLLMEngineProcessorConfig(
            model_source=model,
            batch_size=batch_size,
            concurrency=concurrency,
            apply_chat_template=False,
            tokenize=False,
            detokenize=False,
            engine_kwargs=build_vllm_engine_kwargs(
                pipeline_parallel_size=pipeline_parallel_size,
                tensor_parallel_size=tensor_parallel_size,
                distributed_executor_backend=distributed_executor_backend,
            ),
        ),
        preprocess=lambda row: dict(
            prompt=row["prompt"],
            sampling_params=sampling_params,
        ),
        postprocess=lambda row: row,
    )


def build_shared_vllm_engine_processor(
    batch_size: int,
    concurrency: int,
    model: str,
    sampling_params: dict = VLLM_SAMPLING_PARAMS,
    pipeline_parallel_size: int = None,
    tensor_parallel_size: int = None,
    distributed_executor_backend: str = None,
):
    """Build vLLM engine processor for two-stage (multi-turn) benchmark."""
    processor1 = build_llm_processor(
        vLLMEngineProcessorConfig(
            model_source=model,
            batch_size=batch_size,
            concurrency=concurrency,
            apply_chat_template=False,
            tokenize=False,
            detokenize=False,
            engine_kwargs=build_vllm_engine_kwargs(
                pipeline_parallel_size=pipeline_parallel_size,
                tensor_parallel_size=tensor_parallel_size,
                distributed_executor_backend=distributed_executor_backend,
            ),
        ),
        preprocess=lambda row: dict(
            prompt=row["prompt"],
            sampling_params=sampling_params,
        ),
        postprocess=lambda row: {
            "prompt": row["generated_text"]
            if str(row.get("generated_text", "")).strip()
            else row["prompt"]
        },
    )

    processor2 = build_llm_processor(
        vLLMEngineProcessorConfig(
            model_source=model,
            batch_size=batch_size,
            concurrency=concurrency,
            apply_chat_template=False,
            tokenize=False,
            detokenize=False,
            engine_kwargs=build_vllm_engine_kwargs(
                pipeline_parallel_size=pipeline_parallel_size,
                tensor_parallel_size=tensor_parallel_size,
                distributed_executor_backend=distributed_executor_backend,
            ),
        ),
        preprocess=lambda row: dict(
            prompt=row["prompt"],
            sampling_params=sampling_params,
        ),
        postprocess=lambda row: row,
    )

    def multi_turn_processor(dataset):
        return processor2(processor1(dataset))

    return multi_turn_processor


# -----------------------------------------------------------------------------
# Benchmark execution
# -----------------------------------------------------------------------------


def run_processor(
    dataset: data.Dataset,
    builder,
    *,
    batch_size: int,
    concurrency: int,
    stage_type: str,
    model: str,
    sampling_params: dict = VLLM_SAMPLING_PARAMS,
    pipeline_parallel_size: int = None,
    tensor_parallel_size: int = None,
    distributed_executor_backend: str = None,
) -> BenchmarkResult:
    processor = builder(
        batch_size,
        concurrency,
        model,
        sampling_params,
        pipeline_parallel_size,
        tensor_parallel_size,
        distributed_executor_backend,
    )
    total_samples = dataset.count()

    start = perf_counter()
    processor(dataset).materialize()
    elapsed = perf_counter() - start

    return BenchmarkResult(
        mode=stage_type,
        batch_size=batch_size,
        concurrency=concurrency,
        samples=total_samples,
        elapsed_s=elapsed,
    )


def benchmark(
    mode: Mode,
    dataset: data.Dataset,
    *,
    batch_size: int,
    concurrency: int,
    model: str,
    sampling_params: dict = VLLM_SAMPLING_PARAMS,
    pipeline_parallel_size: int = None,
    tensor_parallel_size: int = None,
    distributed_executor_backend: str = None,
) -> BenchmarkResult:
    if mode == "vllm_engine":
        return run_processor(
            dataset,
            build_single_vllm_engine_processor,
            batch_size=batch_size,
            concurrency=concurrency,
            stage_type="vllm_engine",
            model=model,
            sampling_params=sampling_params,
            pipeline_parallel_size=pipeline_parallel_size,
            tensor_parallel_size=tensor_parallel_size,
            distributed_executor_backend=distributed_executor_backend,
        )
    elif mode == "shared_vllm_engine":
        return run_processor(
            dataset,
            build_shared_vllm_engine_processor,
            batch_size=batch_size,
            concurrency=concurrency,
            stage_type="shared_vllm_engine",
            model=model,
            sampling_params=sampling_params,
            pipeline_parallel_size=pipeline_parallel_size,
            tensor_parallel_size=tensor_parallel_size,
            distributed_executor_backend=distributed_executor_backend,
        )
    else:
        raise ValueError(f"Unknown benchmark mode: {mode}")


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="vLLM throughput benchmark")
    parser.add_argument(
        "--mode",
        choices=[
            "vllm_engine",
            "shared_vllm_engine",
        ],
        default="vllm_engine",
        help="Benchmark mode to run",
    )
    # Dataset configuration
    parser.add_argument(
        "--dataset-path",
        type=str,
        default="/home/ubuntu/datasets/Code-feedback-sharegpt-renamed",
        help="Path to dataset on disk",
    )
    parser.add_argument(
        "--num-prompts", type=int, default=1000, help="Number of prompts to process"
    )
    parser.add_argument(
        "--hf-dataset-id",
        type=str,
        default="Crystalcareai/Code-feedback-sharegpt-renamed",
        help="Hugging Face dataset ID to download",
    )
    parser.add_argument(
        "--hf-split",
        type=str,
        default="train",
        help="Hugging Face dataset split to load",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=0,
        help="Random seed for dataset sampling",
    )
    parser.add_argument(
        "--truncate-prompt",
        type=int,
        default=2048,
        help="Maximum prompt length (no truncation if not specified)",
    )
    # Engine configuration
    parser.add_argument(
        "--model",
        type=str,
        required=True,
        help="LLM model to use",
    )
    parser.add_argument(
        "--pipeline-parallel-size",
        type=int,
        default=1,
        help="Pipeline parallel size for vLLM engine",
    )
    parser.add_argument(
        "--tensor-parallel-size",
        type=int,
        default=1,
        help="Tensor parallel size for vLLM engine",
    )
    parser.add_argument(
        "--distributed-executor-backend",
        type=str,
        default="mp",
        choices=["ray", "mp"],
        help="Distributed executor backend for vLLM engine",
    )
    # Ray Data worker configuration
    parser.add_argument(
        "--batch-size",
        type=int,
        required=True,
        help="Ray Data batch size for processing",
    )
    parser.add_argument(
        "--concurrency", type=int, required=True, help="Ray Data concurrency level"
    )
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args(sys.argv[1:])

    ray.init()
    try:
        dataset = ShareGPTDataset(
            dataset_path=args.dataset_path,
            seed=args.seed,
            hf_dataset_id=args.hf_dataset_id,
            hf_split=args.hf_split,
            truncate_prompt=args.truncate_prompt,
        )
        prompts = dataset.sample(args.num_prompts)

        dataset = data.from_items(prompts)
        result = benchmark(
            args.mode,
            dataset,
            batch_size=args.batch_size,
            concurrency=args.concurrency,
            model=args.model,
            sampling_params=VLLM_SAMPLING_PARAMS,
            pipeline_parallel_size=args.pipeline_parallel_size,
            tensor_parallel_size=args.tensor_parallel_size,
            distributed_executor_backend=args.distributed_executor_backend,
        )
        result.show()
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
