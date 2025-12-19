"""
Model Inference Pipeline Benchmark

This benchmark mimics a production ML inference pipeline with the following structure:
1. Read parquet data with configurable columns
2. Preprocessing with map_batches (CPU tasks) using Pandas
3. Inference with map_batches using actors (GPU) with concurrency control
4. Consume output

Key features mirrored from production:
- Separate worker configurations for preprocessing and inference
- Metadata column passthrough
- Extra output columns added during inference
"""

import argparse
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from benchmark import Benchmark, BenchmarkMetric

import ray
from ray.data import Dataset


@dataclass
class WorkerConfig:
    """Configuration for a worker pool (preprocessing or inference)."""

    batch_size: int
    num_cpus: float
    num_gpus: float
    concurrency: Optional[tuple] = None  # Only for inference actors


@dataclass
class PipelineConfig:
    """Full pipeline configuration."""

    input_path: str
    preprocessing_config: WorkerConfig
    inference_config: WorkerConfig
    metadata_columns: List[str]
    feature_columns: List[str]
    text_columns: List[str]
    extra_output_columns: Dict[str, Any]
    tokenizer_max_length: int = 128


def parse_args():
    parser = argparse.ArgumentParser(description="Model Inference Pipeline Benchmark")
    parser.add_argument(
        "--input-path",
        default="s3://ray-benchmark-data/tpch/parquet/sf10/lineitem",
        help="Path to the input parquet data.",
    )
    parser.add_argument(
        "--preprocessing-batch-size",
        type=int,
        default=4096,
        help="Batch size for preprocessing step.",
    )
    parser.add_argument(
        "--preprocessing-num-cpus",
        type=float,
        default=1.0,
        help="CPUs per preprocessing task.",
    )
    parser.add_argument(
        "--inference-batch-size",
        type=int,
        default=1024,
        help="Batch size for inference step.",
    )
    parser.add_argument(
        "--inference-num-cpus",
        type=float,
        default=1.0,
        help="CPUs per inference actor.",
    )
    parser.add_argument(
        "--inference-num-gpus",
        type=float,
        default=1.0,
        help="GPUs per inference actor.",
    )
    parser.add_argument(
        "--inference-concurrency",
        type=int,
        nargs=2,
        default=[1, 10],
        help="Min and max concurrency for inference actors.",
    )
    parser.add_argument(
        "--tokenizer-max-length",
        type=int,
        default=128,
        help="Max sequence length for tokenization.",
    )
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        default=False,
    )
    return parser.parse_args()


# =============================================================================
# Preprocessing Function (Pandas)
# =============================================================================

# Tokenizer is loaded once per worker and cached
_tokenizer = None


def get_tokenizer():
    """Lazily load and cache the tokenizer."""
    global _tokenizer
    if _tokenizer is None:
        from transformers import AutoTokenizer

        _tokenizer = AutoTokenizer.from_pretrained("bert-base-uncased")
    return _tokenizer


def preprocessing_task_pandas(
    batch: pd.DataFrame,
    metadata_columns: List[str],
    text_columns: List[str],
    metadata_prefix: str = "metadata_",
    max_length: int = 128,
) -> pd.DataFrame:
    """
    Preprocessing task using Pandas with real tokenization.

    Mimics production preprocessing with actual tokenization.
    Renames metadata columns with prefix and applies transformations.
    """
    tokenizer = get_tokenizer()
    result = {}

    # Pass through metadata columns with prefix
    for col in metadata_columns:
        if col in batch.columns:
            result[f"{metadata_prefix}{col}"] = batch[col]

    # Process columns
    for col in batch.columns:
        if col in metadata_columns:
            continue

        if col in text_columns:
            # Real tokenization for text columns
            texts = batch[col].fillna("").astype(str).tolist()
            encoded = tokenizer(
                texts,
                padding="max_length",
                truncation=True,
                max_length=max_length,
                return_tensors="np",
            )
            result[f"input_ids_{col}"] = list(encoded["input_ids"])
            result[f"attention_mask_{col}"] = list(encoded["attention_mask"])
        elif batch[col].dtype in [np.float64, np.float32, np.int64, np.int32]:
            # Normalize numeric columns
            col_data = batch[col].values
            normalized = (col_data - col_data.mean()) / (col_data.std() + 1e-8)
            result[f"feature_{col}"] = normalized

    # Add preprocessing timestamp
    result["preprocessing_timestamp"] = np.full(len(batch), time.time())

    return pd.DataFrame(result)


# =============================================================================
# Inference Actor
# =============================================================================


class InferenceActor:
    """
    Stateful inference actor that mimics production inference patterns.

    Loads model weights on initialization and performs inference on batches.
    Supports metadata passthrough and extra output columns.
    """

    def __init__(
        self,
        model_ref: ray.ObjectRef,
        metadata_columns: List[str],
        extra_output_columns: Dict[str, Any],
        device: str = "cuda",
    ):
        # Load model from object ref (simulates loading from MLflow)
        self.model_weights = ray.get(model_ref)
        self.metadata_columns = metadata_columns
        self.extra_output_columns = extra_output_columns
        self.device = device

        # Simulate model initialization
        self._init_model()

    def _init_model(self):
        """Initialize model on the appropriate device."""
        assert self.model_weights is not None
        self._initialized = True

    def __call__(self, batch: pd.DataFrame) -> pd.DataFrame:
        """
        Run inference on a batch.

        Mimics production inference with:
        - Model forward pass simulation using numeric features and tokenized text
        - Metadata column passthrough
        - Extra output columns addition
        """
        batch_size = len(batch)
        result = {}

        # Pass through metadata columns (prefixed from preprocessing)
        for col in batch.columns:
            if col.startswith("metadata_"):
                result[col] = batch[col].values

        # Gather numeric features
        feature_cols = [c for c in batch.columns if c.startswith("feature_")]

        # Gather tokenized text (input_ids columns)
        input_ids_cols = [c for c in batch.columns if c.startswith("input_ids_")]

        # Simulate model inference combining numeric and text features
        # In production, this would be an embedding lookup + neural network forward pass
        if feature_cols:
            features = batch[feature_cols].values.astype(np.float32)
        else:
            features = np.zeros((batch_size, 1), dtype=np.float32)

        # Simulate processing tokenized inputs (e.g., embedding lookup, transformer)
        if input_ids_cols:
            # Stack input_ids and compute mean as a simple aggregation
            token_features = []
            for col in input_ids_cols:
                # Each row is an array of token ids - compute mean as feature
                token_means = np.array(
                    [np.mean(ids) for ids in batch[col].values], dtype=np.float32
                )
                token_features.append(token_means)
            token_features = np.column_stack(token_features)
            # Concatenate with numeric features
            features = np.concatenate([features, token_features], axis=1)

        # Simulate inference computation
        time.sleep(0.001 * batch_size / 100)  # Simulate GPU compute time
        predictions = np.matmul(features, self.model_weights[: features.shape[1], :])

        result["predictions"] = [pred.tolist() for pred in predictions]
        result["prediction_confidence"] = predictions.max(axis=1)

        # Add extra output columns (static values from config)
        for col_name, col_value in self.extra_output_columns.items():
            result[col_name] = np.full(batch_size, col_value)

        # Add inference timestamp
        result["inference_timestamp"] = np.full(batch_size, time.time())

        return pd.DataFrame(result)


# =============================================================================
# Pipeline Execution
# =============================================================================


def preprocess_dataset(
    dataset: Dataset,
    config: PipelineConfig,
) -> Dataset:
    """Apply preprocessing to dataset using Pandas task."""
    preprocessed = dataset.map_batches(
        preprocessing_task_pandas,
        fn_kwargs=dict(
            metadata_columns=config.metadata_columns,
            text_columns=config.text_columns,
            metadata_prefix="metadata_",
            max_length=config.tokenizer_max_length,
        ),
        batch_format="pandas",
        batch_size=config.preprocessing_config.batch_size,
        num_cpus=config.preprocessing_config.num_cpus,
    )
    preprocessed._set_name("preprocessed_data")
    return preprocessed


def infer_dataset(
    dataset: Dataset,
    model_ref: ray.ObjectRef,
    config: PipelineConfig,
) -> Dataset:
    """Run inference on dataset using configured inference actor."""
    inferred = dataset.map_batches(
        InferenceActor,
        fn_constructor_kwargs=dict(
            model_ref=model_ref,
            metadata_columns=config.metadata_columns,
            extra_output_columns=config.extra_output_columns,
            device="cuda" if config.inference_config.num_gpus > 0 else "cpu",
        ),
        batch_format="pandas",
        batch_size=config.inference_config.batch_size,
        num_cpus=config.inference_config.num_cpus,
        num_gpus=config.inference_config.num_gpus,
        concurrency=config.inference_config.concurrency,
    )
    inferred._set_name("inference_output")
    return inferred


def execute_pipeline(
    dataset: Dataset,
    model_ref: ray.ObjectRef,
    config: PipelineConfig,
) -> Dataset:
    """Execute full end-to-end pipeline."""
    preprocessed = preprocess_dataset(dataset, config)
    return infer_dataset(preprocessed, model_ref, config)


# =============================================================================
# Main Benchmark
# =============================================================================


def main(args):
    print("Running model inference pipeline benchmark")
    print(f"  Input path: {args.input_path}")
    print(f"  Preprocessing batch size: {args.preprocessing_batch_size}")
    print(f"  Inference batch size: {args.inference_batch_size}")
    print(f"  Inference concurrency: {args.inference_concurrency}")
    print(f"  Tokenizer max length: {args.tokenizer_max_length}")

    # Adjust for smoke test
    if args.smoke_test:
        args.inference_num_gpus = 0
        args.inference_concurrency = [1, 2]

    # Build pipeline configuration
    # Use TPC-H lineitem columns:
    # - column00, column01: metadata (l_orderkey, l_partkey)
    # - column04-07: numeric features (l_quantity, l_extendedprice, l_discount, l_tax)
    # - column08, column09: text columns (l_returnflag, l_linestatus) for tokenization
    config = PipelineConfig(
        input_path=args.input_path,
        preprocessing_config=WorkerConfig(
            batch_size=args.preprocessing_batch_size,
            num_cpus=args.preprocessing_num_cpus,
            num_gpus=0,
        ),
        inference_config=WorkerConfig(
            batch_size=args.inference_batch_size,
            num_cpus=args.inference_num_cpus,
            num_gpus=args.inference_num_gpus,
            concurrency=tuple(args.inference_concurrency),
        ),
        metadata_columns=["column00", "column01"],
        feature_columns=["column04", "column05", "column06", "column07"],
        text_columns=["column08", "column09"],
        extra_output_columns={
            "model_version": "v1.0.0",
            "pipeline_id": "benchmark_run",
        },
        tokenizer_max_length=args.tokenizer_max_length,
    )

    # Create dummy model weights and put in object store
    model_weights = np.random.randn(100, 10).astype(np.float32)
    model_ref = ray.put(model_weights)

    start_time = time.time()

    # Load input data
    columns_to_load = list(
        set(config.metadata_columns + config.feature_columns + config.text_columns)
    )

    if args.smoke_test:
        ds = ray.data.read_parquet(
            config.input_path,
            columns=columns_to_load,
            override_num_blocks=10,
        )
        ds = ds.limit(10000)
    else:
        ds = ray.data.read_parquet(
            config.input_path,
            columns=columns_to_load,
        )
    ds._set_name("input_data")

    # Execute end-to-end pipeline
    output_ds = execute_pipeline(ds, model_ref, config)

    # Consume output
    total_rows = 0
    for bundle in output_ds.iter_internal_ref_bundles():
        total_rows += bundle.num_rows()

    end_time = time.time()

    total_time = end_time - start_time
    throughput = total_rows / total_time if total_time > 0 else 0

    print(f"Total rows processed: {total_rows}")
    print(f"Total time (sec): {total_time:.2f}")
    print(f"Throughput (rows/sec): {throughput:.2f}")

    return {
        BenchmarkMetric.RUNTIME: total_time,
        BenchmarkMetric.THROUGHPUT: throughput,
        BenchmarkMetric.NUM_ROWS: total_rows,
        "preprocessing_batch_size": args.preprocessing_batch_size,
        "inference_batch_size": args.inference_batch_size,
        "inference_concurrency_min": args.inference_concurrency[0],
        "inference_concurrency_max": args.inference_concurrency[1],
        "tokenizer_max_length": args.tokenizer_max_length,
    }


if __name__ == "__main__":
    args = parse_args()

    benchmark = Benchmark()
    benchmark.run_fn("model-inference-pipeline", main, args)
    benchmark.write_result()
