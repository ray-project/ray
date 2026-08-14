import ray
import argparse

from benchmark import Benchmark, collect_operator_metrics, consume_ref_bundles


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
    return parser.parse_args()


def main(args):
    benchmark = Benchmark()

    def benchmark_fn():
        left_ds = ray.data.read_parquet(args.left_dataset)
        right_ds = ray.data.read_parquet(args.right_dataset)
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

        # Consume the bundles rather than calling count(): count() executes a *copy* of
        # the plan, so the stats never attach to `joined_ds` and the per-operator
        # numbers below would all be empty. Row count comes from the bundles instead.
        total_rows = 0

        def tally(bundle):
            nonlocal total_rows
            total_rows += bundle.num_rows()

        consume_ref_bundles(joined_ds, tally)
        print(f"Join completed with {total_rows} records.")

        # Per-operator wall time / output bytes / per-task USS+RSS: separates the two
        # reads from the shuffle and the join itself.
        return {"num_rows": total_rows, **collect_operator_metrics(joined_ds)}

    benchmark.run_fn(str(vars(args)), benchmark_fn)
    benchmark.write_result()


if __name__ == "__main__":
    args = parse_args()
    main(args)
