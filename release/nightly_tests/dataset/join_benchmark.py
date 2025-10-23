import ray
import argparse

from benchmark import Benchmark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--left_dataset", required=True, type=str, help="Path to the left dataset"
    )
    parser.add_argument(
        "--right_dataset", required=True, type=str, help="Path to the right dataset"
    )
    parser.add_argument(
        "--num_partitions",
        required=True,
        type=int,
        help="Number of partitions to use for the join",
    )
    parser.add_argument(
        "--left_join_keys",
        required=True,
        nargs="+",
        type=str,
        help="Join keys for the left dataset",
    )
    parser.add_argument(
        "--right_join_keys",
        required=True,
        nargs="+",
        type=str,
        help="Join keys for the right dataset",
    )
    parser.add_argument(
        "--join_type",
        required=True,
        choices=["inner", "left_outer", "right_outer", "full_outer"],
        help="Type of join operation",
    )
    parser.add_argument(
        "--max_aggregators",
        required=True,
        type=int,
        help="Max number of hash shuffle aggregators",
    )
    return parser.parse_args()


def main(args):
    benchmark = Benchmark()

    def benchmark_fn():

        ctx = ray.data.DataContext.get_current()
        ctx.max_hash_shuffle_aggregators = args.max_aggregators
        ctx.downstream_capacity_backpressure_max_queued_bundles = 500
        ctx.downstream_capacity_backpressure_ratio = 0

        left_ds = ray.data.read_parquet(args.left_dataset, memory=1e9)
        right_ds = ray.data.read_parquet(args.right_dataset, memory=1e9)
        # Check if join keys match; if not, rename right join keys
        if len(args.left_join_keys) != len(args.right_join_keys):
            raise ValueError("Number of left and right join keys must match.")

        # Perform join
        joined_ds = left_ds.join(
            right_ds,
            num_partitions=args.num_partitions,
            on=args.left_join_keys,
            right_on=args.right_join_keys,
            join_type=args.join_type,
        )

        # Process joined_ds if needed
        print(f"Join completed with {joined_ds.count()} records.")

    benchmark.run_fn(str(vars(args)), benchmark_fn)
    benchmark.write_result()


if __name__ == "__main__":
    args = parse_args()
    main(args)
