#!/usr/bin/env python
"""
Benchmark Ray Data LLM offline batch inference throughput.

Sample usage:
python3 benchmark_processor.py --mode vllm_engine --batch-size 64 --concurrency 1 --num-prompts 10000 --model facebook/opt-1.3b
    --tensor-parallel-size 2 --pipeline-parallel-size 2 --distributed-executor-backend ray
"""

from dataclasses import dataclass
from time import perf_counter, sleep
from typing import Literal

import argparse
import sys

import ray
from ray import data, serve
from ray.data.llm import (
    vLLMEngineProcessorConfig,
    ServeDeploymentProcessorConfig,
    build_llm_processor,
)
from ray.serve.llm import (
    LLMConfig,
    ModelLoadingConfig,
    build_llm_deployment,
)
from ray.serve.llm.openai_api_models import CompletionRequest

from dataset import ShareGPTDataset


Mode = Literal[
    "vllm_engine",
    "shared_vllm_engine",
    "serve_deployment",
    "shared_serve_deployment",
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


def build_single_vllm_engine_processor(
    batch_size: int,
    concurrency: int,
    model: str,
    sampling_params: dict = VLLM_SAMPLING_PARAMS,
    pipeline_parallel_size: int = None,
    tensor_parallel_size: int = None,
    distributed_executor_backend: str = None,
):
    """Build vLLM engine processor for single-turn benchmark."""
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
    """Build vLLM engine processor for multi-turn benchmark."""
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


def setup_serve_deployment(model: str, concurrency: int) -> tuple[str, str]:
    """Set up Ray Serve deployment for hosting the LLM model."""
    deployment_name = "benchmark_deployment"
    app_name = "benchmark_app"

    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id=model,
            model_source=model,
        ),
        deployment_config=dict(
            name=deployment_name,
            # To fairly compare with vLLM engine processor, fix the number of replicas to the concurrency level
            autoscaling_config=dict(
                min_replicas=concurrency,
                max_replicas=concurrency,
            ),
        ),
        engine_kwargs=dict(
            enable_prefix_caching=True,
            enable_chunked_prefill=True,
            max_num_batched_tokens=4096,
        ),
    )

    override_serve_options = dict(name=deployment_name)
    llm_app = build_llm_deployment(
        llm_config, override_serve_options=override_serve_options
    )
    serve.run(llm_app, name=app_name)

    print("Waiting for Serve deployment to be ready...")
    max_wait_time = 120  # seconds
    wait_time = 0
    while not _is_app_ready(app_name) and wait_time < max_wait_time:
        sleep(5)
        wait_time += 5

    if wait_time >= max_wait_time:
        raise TimeoutError("Deployment failed to become ready within timeout")

    print("Deployment is ready!")
    return deployment_name, app_name


def _is_app_ready(app_name: str) -> bool:
    try:
        serve_status = serve.status()

        if app_name in serve_status.applications:
            app_status = serve_status.applications[app_name]
            if app_status.status == "RUNNING":
                print(f"Application '{app_name}' is RUNNING.")
                return True
            else:
                print(f"Application '{app_name}' status: {app_status.status}")
                return False
        else:
            print(f"Application '{app_name}' not found in Serve status.")
            return False
    except Exception as e:
        print(f"Error checking app status: {e}")
        return False


def build_single_serve_deployment_processor(
    batch_size: int,
    concurrency: int,
    model: str,
    sampling_params: dict = VLLM_SAMPLING_PARAMS,
    deployment_name: str = None,
    app_name: str = None,
    **kwargs,
):
    """Build Serve deployment processor for single-turn benchmark."""
    config = ServeDeploymentProcessorConfig(
        deployment_name=deployment_name,
        app_name=app_name,
        dtype_mapping={
            "CompletionRequest": CompletionRequest,
        },
        batch_size=batch_size,
        concurrency=concurrency,
    )
    return build_llm_processor(
        config,
        chat_template_kwargs=None,
        preprocess=lambda row: dict(
            method="completions",
            dtype="CompletionRequest",
            request_kwargs=dict(
                model=model,
                prompt=row["prompt"],
                **sampling_params,
            ),
        ),
        postprocess=lambda row: row,
    )


def build_shared_serve_deployment_processor(
    batch_size: int,
    concurrency: int,
    model: str,
    sampling_params: dict = VLLM_SAMPLING_PARAMS,
    deployment_name: str = None,
    app_name: str = None,
    **kwargs,
):
    """Build Serve deployment processor for multi-turn benchmark."""
    config = ServeDeploymentProcessorConfig(
        deployment_name=deployment_name,
        app_name=app_name,
        dtype_mapping={
            "CompletionRequest": CompletionRequest,
        },
        batch_size=batch_size,
        concurrency=concurrency,
    )

    processor1 = build_llm_processor(
        config,
        chat_template_kwargs=None,
        preprocess=lambda row: dict(
            method="completions",
            dtype="CompletionRequest",
            request_kwargs=dict(
                model=model,
                prompt=row["prompt"],
                stream=False,
            ),
        ),
        postprocess=lambda row: {
            # Fall back to original prompt if generated text is empty
            "prompt": (
                row["choices"][0]["text"]
                if row.get("choices") and str(row["choices"][0].get("text", "")).strip()
                else row["prompt"]
            )
        },
    )

    processor2 = build_llm_processor(
        config,
        chat_template_kwargs=None,
        preprocess=lambda row: dict(
            method="completions",
            dtype="CompletionRequest",
            request_kwargs=dict(
                model=model,
                prompt=row["prompt"],
                stream=False,
            ),
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
    mode: Mode,
    dataset: data.Dataset,
    builder,
    **kwargs,
) -> BenchmarkResult:
    processor = builder(**kwargs)

    total_samples = dataset.count()

    start = perf_counter()
    processor(dataset).materialize()
    elapsed = perf_counter() - start

    return BenchmarkResult(
        mode=mode,
        batch_size=kwargs.get("batch_size"),
        concurrency=kwargs.get("concurrency"),
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
    mode_to_builder = {
        "vllm_engine": build_single_vllm_engine_processor,
        "shared_vllm_engine": build_shared_vllm_engine_processor,
        "serve_deployment": build_single_serve_deployment_processor,
        "shared_serve_deployment": build_shared_serve_deployment_processor,
    }

    if mode not in mode_to_builder:
        raise ValueError(f"Unknown benchmark mode: {mode}")

    builder = mode_to_builder[mode]

    if mode in ["serve_deployment", "shared_serve_deployment"]:
        deployment_name, app_name = setup_serve_deployment(model, concurrency)
        try:
            return run_processor(
                mode,
                dataset,
                builder,
                batch_size=batch_size,
                concurrency=concurrency,
                model=model,
                sampling_params=sampling_params,
                deployment_name=deployment_name,
                app_name=app_name,
            )
        finally:
            serve.delete(app_name)
    else:
        return run_processor(
            mode,
            dataset,
            builder,
            batch_size=batch_size,
            concurrency=concurrency,
            model=model,
            sampling_params=sampling_params,
            pipeline_parallel_size=pipeline_parallel_size,
            tensor_parallel_size=tensor_parallel_size,
            distributed_executor_backend=distributed_executor_backend,
        )


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
            "serve_deployment",
            "shared_serve_deployment",
        ],
        default="vllm_engine",
        help="Ray Data LLM processor to run benchmarks for",
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
        help="Maximum prompt length",
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
