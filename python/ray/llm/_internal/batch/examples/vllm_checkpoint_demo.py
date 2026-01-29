#!/usr/bin/env python3
"""
Minimal script demonstrating vLLM processor with Ray Data checkpointing.

This demo tests whether failures inside the vLLM stage can still allow
earlier blocks to be checkpointed when using sequential processing
with a large enough dataset.
"""
import argparse
import os

import pandas as pd
import ray
from ray.data.checkpoint import CheckpointConfig
from ray.data.llm import build_processor, vLLMEngineProcessorConfig
from ray.llm._internal.batch.stages.configs import (
    ChatTemplateStageConfig,
    DetokenizeStageConfig,
    TokenizerStageConfig,
)

# Setup paths
base_dir = "/tmp/ray_vllm_checkpoint_demo"
input_path = os.path.join(base_dir, "input")
output_path = os.path.join(base_dir, "output")
checkpoint_path = os.path.join(base_dir, "checkpoint")

# Model configuration
MODEL_SOURCE = "Qwen/Qwen3-0.6B"
BATCH_SIZE = 10  # Small batch size to match rows per file


def create_sample_data(num_rows=200, rows_per_file=10):
    """Create sample input data split across multiple files."""
    import shutil
    
    # Clean up
    for path in [input_path, output_path, checkpoint_path]:
        if os.path.exists(path):
            shutil.rmtree(path)
    os.makedirs(input_path, exist_ok=True)
    os.makedirs(output_path, exist_ok=True)
    os.makedirs(checkpoint_path, exist_ok=True)
    
    # Create multiple parquet files
    num_files = (num_rows + rows_per_file - 1) // rows_per_file
    for i in range(0, num_rows, rows_per_file):
        end_idx = min(i + rows_per_file, num_rows)
        df = pd.DataFrame({
            "id": range(i, end_idx),
            "prompt": [f"Calculate {j} ** 3" for j in range(i, end_idx)],
        })
        df.to_parquet(os.path.join(input_path, f"data_{i:03d}.parquet"), index=False)
    
    print(f"Created {num_rows} rows in {num_files} files ({rows_per_file} rows each)")


def run_pipeline(fail_after_id=None, concurrency=1):
    """Run pipeline with optional simulated failure inside vLLM stage.
    
    Args:
        fail_after_id: If set, fail batches containing rows with id > this value
        concurrency: Number of vLLM actors
    """
    # Setup checkpoint configuration
    ctx = ray.data.DataContext.get_current()
    ctx.checkpoint_config = CheckpointConfig(
        id_column="id",
        checkpoint_path=checkpoint_path,
        delete_checkpoint_on_success=False,
    )

    processor_config = vLLMEngineProcessorConfig(
        model_source=MODEL_SOURCE,
        engine_kwargs=dict(
            distributed_executor_backend="uni",
        ),
        task_type="generate",
        batch_size=BATCH_SIZE,
        concurrency=concurrency,
        chat_template_stage=ChatTemplateStageConfig(enabled=False),
        tokenize_stage=TokenizerStageConfig(enabled=True),
        detokenize_stage=DetokenizeStageConfig(enabled=False),
        # Simulate failure inside vLLM stage for rows with id > threshold
        simulate_failure=fail_after_id,
    )

    processor = build_processor(
        processor_config,
        preprocess=lambda row: dict(
            id=row["id"],  # Preserve id for checkpointing AND failure check
            prompt=row["prompt"],
            sampling_params={
                "max_tokens": 10,
                "ignore_eos": False,
                "temperature": 1.0,
                "top_p": 1.0,
            }
        ),
        postprocess=lambda row: {
            "id": row["id"],
            "generated_text": row.get("generated_text", ""),
        },
    )

    # Read input - each parquet file becomes a block
    ds = ray.data.read_parquet(input_path)
    print(f"Dataset has {ds.count()} rows")

    # Process with vLLM - failure happens INSIDE this stage if fail_after_id is set
    ds = processor(ds)

    # Write output
    ds.write_parquet(output_path)

    print(f"Pipeline completed successfully")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="vLLM checkpoint demo")
    parser.add_argument(
        "mode",
        choices=["setup", "run1", "run2", "verify", "clean"],
        help="Execution mode",
    )
    parser.add_argument(
        "--fail-after-id",
        type=int,
        default=100,
        help="ID threshold for simulated failure (default: 100)",
    )
    parser.add_argument(
        "--num-rows",
        type=int,
        default=200,
        help="Number of rows to create (default: 200)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=1,
        help="Number of vLLM actors (default: 1)",
    )
    args = parser.parse_args()

    if args.mode == "clean":
        import shutil
        for path in [input_path, output_path, checkpoint_path]:
            if os.path.exists(path):
                shutil.rmtree(path)
                print(f"Removed {path}")

    elif args.mode == "setup":
        print(f"Creating {args.num_rows} rows...")
        create_sample_data(num_rows=args.num_rows)

    elif args.mode == "run1":
        print(f"\n=== Run 1: Pipeline with failure at id > {args.fail_after_id} ===")
        print(f"Concurrency: {args.concurrency}")
        ray.init(ignore_reinit_error=True)
        try:
            run_pipeline(fail_after_id=args.fail_after_id, concurrency=args.concurrency)
        except Exception as e:
            print(f"\nFailed as expected: {type(e).__name__}")
        finally:
            ray.shutdown()

    elif args.mode == "run2":
        print("\n=== Run 2: Resume from checkpoint ===")
        ray.init(ignore_reinit_error=True)
        try:
            run_pipeline(fail_after_id=None, concurrency=args.concurrency)
        finally:
            ray.shutdown()

    elif args.mode == "verify":
        print("\n=== Verification ===")
        import pyarrow.parquet as pq
        
        # Check checkpoints
        if os.path.exists(checkpoint_path):
            checkpoint_files = [f for f in os.listdir(checkpoint_path) if f.endswith('.parquet')]
            checkpoint_ids = set()
            for f in checkpoint_files:
                df = pq.read_table(os.path.join(checkpoint_path, f)).to_pandas()
                checkpoint_ids.update(df['id'].tolist())
            print(f"Checkpoint files: {len(checkpoint_files)}")
            print(f"Checkpointed IDs: {len(checkpoint_ids)} rows")
            if checkpoint_ids:
                print(f"  Range: {min(checkpoint_ids)} - {max(checkpoint_ids)}")
        else:
            print("No checkpoint directory")
        
        # Check output
        if os.path.exists(output_path):
            output_files = [f for f in os.listdir(output_path) if f.endswith('.parquet')]
            output_ids = set()
            for f in output_files:
                df = pq.read_table(os.path.join(output_path, f)).to_pandas()
                output_ids.update(df['id'].tolist())
            print(f"\nOutput files: {len(output_files)}")
            print(f"Output IDs: {len(output_ids)} rows")
            if output_ids:
                print(f"  Range: {min(output_ids)} - {max(output_ids)}")
        else:
            print("No output directory")
