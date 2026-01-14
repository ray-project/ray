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
import torch
from benchmark import Benchmark, BenchmarkMetric
from transformers import AutoModel, AutoTokenizer

import ray
from ray.data import Dataset, ActorPoolStrategy

# Default HuggingFace model for inference
DEFAULT_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"


@dataclass
class WorkerConfig:
    """Configuration for a worker pool (preprocessing or inference)."""

    batch_size: int
    num_cpus: float
    num_gpus: float
    # Actor pool sizing (only for inference actors)
    min_actors: Optional[int] = None
    max_actors: Optional[int] = None


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
    model_name: str = DEFAULT_MODEL_NAME


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
        "--inference-min-actors",
        type=int,
        default=1,
        help="Minimum number of inference actors.",
    )
    parser.add_argument(
        "--inference-max-actors",
        type=int,
        default=10,
        help="Maximum number of inference actors.",
    )
    parser.add_argument(
        "--tokenizer-max-length",
        type=int,
        default=128,
        help="Max sequence length for tokenization.",
    )
    parser.add_argument(
        "--model-name",
        type=str,
        default=DEFAULT_MODEL_NAME,
        help="HuggingFace model name for inference.",
    )
    return parser.parse_args()


# =============================================================================
# Preprocessing Function (Pandas)
# =============================================================================


def preprocessing_task_pandas(
    batch: pd.DataFrame,
    metadata_columns: List[str],
    feature_columns: List[str],
    text_columns: List[str],
    metadata_prefix: str = "metadata_",
) -> pd.DataFrame:
    """
    Preprocessing task using Pandas.

    Mimics production preprocessing with:
    - Metadata columns passed through with prefix
    - Text columns passed through for model tokenization
    - Feature columns normalized
    """
    result = {}

    # Pass through metadata columns with prefix
    for col in metadata_columns:
        if col in batch.columns:
            result[f"{metadata_prefix}{col}"] = batch[col]

    # Pass through text columns (tokenization happens in inference actor)
    for col in text_columns:
        if col in batch.columns:
            result[f"text_{col}"] = batch[col].fillna("").astype(str)

    # Process feature columns (numeric)
    # Cast to float64 to handle DECIMAL types from Parquet which become object dtype
    for col in feature_columns:
        if col in batch.columns:
            col_data = pd.to_numeric(batch[col], errors="coerce").values
            normalized = (col_data - np.nanmean(col_data)) / (
                np.nanstd(col_data) + 1e-8
            )
            result[f"feature_{col}"] = normalized

    # Add preprocessing timestamp
    result["preprocessing_timestamp"] = np.full(len(batch), time.time())

    return pd.DataFrame(result)


# =============================================================================
# Inference Actor with HuggingFace Model
# =============================================================================


class InferenceActor:
    """
    Stateful inference actor that performs GPU inference using HuggingFace models.

    Downloads model weights on initialization and performs inference on batches
    using the configured device (GPU or CPU).
    Supports metadata passthrough and extra output columns.
    """

    def __init__(
        self,
        model_name: str,
        text_columns: List[str],
        metadata_columns: List[str],
        extra_output_columns: Dict[str, Any],
        max_length: int = 128,
        device: str = "cuda",
    ):
        self.model_name = model_name
        self.text_columns = text_columns
        self.metadata_columns = metadata_columns
        self.extra_output_columns = extra_output_columns
        self.max_length = max_length
        self.device = torch.device(device if torch.cuda.is_available() else "cpu")

        self._init_model()

    def _init_model(self):
        """Download and initialize HuggingFace model on the appropriate device."""
        print(f"Loading HuggingFace model: {self.model_name}")
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        self.model = AutoModel.from_pretrained(self.model_name)
        self.model.to(self.device)
        self.model.eval()
        print(f"Model loaded on device: {self.device}")

    def _mean_pooling(self, model_output, attention_mask):
        """Apply mean pooling to get sentence embeddings."""
        token_embeddings = model_output.last_hidden_state
        input_mask_expanded = (
            attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
        )
        return torch.sum(token_embeddings * input_mask_expanded, 1) / torch.clamp(
            input_mask_expanded.sum(1), min=1e-9
        )

    @torch.inference_mode()
    def __call__(self, batch: pd.DataFrame) -> pd.DataFrame:
        """
        Run inference on a batch using HuggingFace model.

        Performs:
        - Text concatenation from configured columns
        - Tokenization using HuggingFace tokenizer
        - Model inference on GPU
        - Mean pooling to get embeddings
        - Metadata column passthrough
        - Extra output columns addition
        """
        batch_size = len(batch)
        result = {}

        # Pass through metadata columns (prefixed from preprocessing)
        for col in batch.columns:
            if col.startswith("metadata_"):
                result[col] = batch[col].values

        # Concatenate text columns into single text for each row
        text_col_names = [f"text_{col}" for col in self.text_columns]
        available_text_cols = [c for c in text_col_names if c in batch.columns]

        if available_text_cols:
            texts = (
                batch[available_text_cols].astype(str).agg(" ".join, axis=1).tolist()
            )
        else:
            texts = [""] * batch_size

        # Tokenize with HuggingFace tokenizer
        encoded = self.tokenizer(
            texts,
            padding=True,
            truncation=True,
            max_length=self.max_length,
            return_tensors="pt",
        )

        # Move to device
        input_ids = encoded["input_ids"].to(self.device)
        attention_mask = encoded["attention_mask"].to(self.device)

        # Run model inference
        model_output = self.model(input_ids=input_ids, attention_mask=attention_mask)

        # Get embeddings via mean pooling
        embeddings = self._mean_pooling(model_output, attention_mask)

        # Move results back to CPU
        embeddings_np = embeddings.cpu().numpy()

        # Store embeddings as list of arrays
        result["embeddings"] = [emb.tolist() for emb in embeddings_np]
        result["embedding_dim"] = np.full(batch_size, embeddings_np.shape[1])

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
            feature_columns=config.feature_columns,
            text_columns=config.text_columns,
            metadata_prefix="metadata_",
        ),
        batch_format="pandas",
        batch_size=config.preprocessing_config.batch_size,
        num_cpus=config.preprocessing_config.num_cpus,
    )
    preprocessed._set_name("preprocessed_data")
    return preprocessed


def infer_dataset(
    dataset: Dataset,
    config: PipelineConfig,
) -> Dataset:
    """Run inference on dataset using configured inference actor."""
    inferred = dataset.map_batches(
        InferenceActor,
        fn_constructor_kwargs=dict(
            model_name=config.model_name,
            text_columns=config.text_columns,
            metadata_columns=config.metadata_columns,
            extra_output_columns=config.extra_output_columns,
            max_length=config.tokenizer_max_length,
            device="cuda" if config.inference_config.num_gpus > 0 else "cpu",
        ),
        batch_format="pandas",
        batch_size=config.inference_config.batch_size,
        compute=ActorPoolStrategy(
            min_size=config.inference_config.min_actors,
            max_size=config.inference_config.max_actors,
        ),
        num_cpus=config.inference_config.num_cpus,
        num_gpus=config.inference_config.num_gpus,
    )
    inferred._set_name("inference_output")
    return inferred


def execute_pipeline(
    dataset: Dataset,
    config: PipelineConfig,
) -> Dataset:
    """Execute full end-to-end pipeline."""
    preprocessed = preprocess_dataset(dataset, config)
    return infer_dataset(preprocessed, config)


# =============================================================================
# Main Benchmark
# =============================================================================


def main(args):
    print("Running model inference pipeline benchmark")
    print(f"  Input path: {args.input_path}")
    print(f"  Preprocessing batch size: {args.preprocessing_batch_size}")
    print(f"  Inference batch size: {args.inference_batch_size}")
    print(
        f"  Inference actors: min={args.inference_min_actors}, max={args.inference_max_actors}"
    )
    print(f"  Tokenizer max length: {args.tokenizer_max_length}")
    print(f"  Model: {args.model_name}")

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
            min_actors=args.inference_min_actors,
            max_actors=args.inference_max_actors,
        ),
        metadata_columns=["column00", "column01"],
        feature_columns=["column04", "column05", "column06", "column07"],
        text_columns=["column08", "column09"],
        extra_output_columns={
            "model_version": "v1.0.0",
            "pipeline_id": "benchmark_run",
        },
        tokenizer_max_length=args.tokenizer_max_length,
        model_name=args.model_name,
    )

    start_time = time.time()

    # Load input data
    columns_to_load = list(
        set(config.metadata_columns + config.feature_columns + config.text_columns)
    )

    ds = ray.data.read_parquet(
        config.input_path,
        columns=columns_to_load,
    ).limit(15_000_000)
    ds._set_name("input_data")

    # Execute end-to-end pipeline
    output_ds = execute_pipeline(ds, config)

    # Consume output
    total_rows = 0
    for batch in output_ds.iter_batches(batch_size=None, batch_format="pandas"):
        total_rows += len(batch)

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
        "inference_min_actors": args.inference_min_actors,
        "inference_max_actors": args.inference_max_actors,
        "tokenizer_max_length": args.tokenizer_max_length,
    }


if __name__ == "__main__":
    args = parse_args()

    benchmark = Benchmark()
    benchmark.run_fn("model-inference-pipeline", main, args)
    benchmark.write_result()
