import argparse
import uuid
from typing import Dict, Any

import ray
from benchmark import Benchmark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Wide schema pipeline benchmark")
    parser.add_argument(
        "--data-type",
        choices=["primitives", "tensors", "objects", "nested_structs"],
        default="primitives",
        help="Type of pre-generated dataset to benchmark"
    )
    
    return parser.parse_args()




def simple_map_batches_fn(batch):
    """Simple transformation function for map_batches - adds a new column with row count."""
    # Simple, fast transformation that works with any schema: add a row count column
    batch = batch.copy()
    batch["_row_count"] = len(batch)
    return batch


def main(args: argparse.Namespace) -> None:
    benchmark = Benchmark()
    
    # Use pre-generated datasets from S3
    input_path = f"s3://ray-benchmark-data-internal-us-west-2/wide_schema/{args.data_type}"
    
    # Create unique output path for this run
    unique_id = uuid.uuid4().hex[:8]
    output_path = f"s3://ray-data-write-benchmark/wide-schema-output-{args.data_type}-{unique_id}"
    
    print(f"Using pre-generated dataset: {input_path}")
    
    # Run the pipeline benchmark (TIMED)
    def run_pipeline() -> Dict[str, Any]:
        """Run the data pipeline: read -> map_batches -> write"""
        print("Reading dataset...")
        ds = ray.data.read_parquet(input_path)
        
        print("Applying map_batches transformation...")
        ds = ds.map_batches(
            simple_map_batches_fn, 
            batch_format="pandas",
            batch_size=10000
        )
        
        print("Writing results...")
        ds.write_parquet(output_path)
        
        # Get dataset stats for reporting
        row_count = ds.count()
        actual_num_columns = len(ds.schema().base_schema)
        
        return {
            "num_columns": actual_num_columns,
            "data_type": args.data_type,
            "num_rows": row_count,
            "input_path": input_path,
            "output_path": output_path,
        }
    
    # Run the timed benchmark
    benchmark.run_fn("wide_schema_pipeline", run_pipeline)
    benchmark.write_result()
    
    print(f"Pipeline completed. Results written to: {output_path}")


if __name__ == "__main__":
    args = parse_args()
    main(args)
