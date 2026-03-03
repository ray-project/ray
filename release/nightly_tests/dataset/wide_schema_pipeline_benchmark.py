import argparse
from typing import Dict, Any

import ray
from benchmark import Benchmark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Wide schema pipeline benchmark")
    parser.add_argument(
        "--data-type",
        choices=["primitives", "tensors", "objects", "nested_structs"],
        default="primitives",
        help="Type of pre-generated dataset to benchmark",
    )

    return parser.parse_args()


def main(args: argparse.Namespace) -> None:
    benchmark = Benchmark()

    # Each dataset contains about 500-600Mbs of data, except for objects,
    # which contain about 150Mb (this is because their pickle bloat is big).
    # Furthermore, the schema contains 5000 fields, and each column contains
    # 500 characters.
    input_path = (
        f"s3://ray-benchmark-data-internal-us-west-2/wide_schema/{args.data_type}"
    )

    print(f"Using pre-generated dataset: {input_path}")

    # Run the pipeline benchmark (TIMED)
    def run_pipeline() -> Dict[str, Any]:
        """Run the data pipeline: read -> map_batches -> write"""
        ds = ray.data.read_parquet(input_path)

        for _ in ds.iter_internal_ref_bundles():
            pass

        # Get dataset stats for reporting
        actual_num_columns = len(ds.schema().base_schema)

        return {
            "num_columns": actual_num_columns,
            "data_type": args.data_type,
            "input_path": input_path,
        }

    # Run the timed benchmark
    benchmark.run_fn("wide_schema_pipeline", run_pipeline)
    benchmark.write_result()


if __name__ == "__main__":
    args = parse_args()
    main(args)
