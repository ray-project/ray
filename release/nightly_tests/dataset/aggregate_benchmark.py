import ray
import argparse

from benchmark import Benchmark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=str)
    parser.add_argument("--group-by", required=True, nargs="+", type=str)
    parser.add_argument(
        "--aggregation", required=True, choices=["count", "mean", "std"]
    )
    parser.add_argument("--aggregate-on", type=str)

    return parser.parse_args()


def main(args):
    benchmark = Benchmark("aggregate")

    def benchmark_fn():
        ds = ray.data.read_parquet(args.path).groupby(args.group_by)
        if args.aggregation == "count":
            aggregate_ds = ds.count()
        elif args.aggregation == "mean":
            aggregate_ds = ds.mean(args.aggregate_on)
        elif args.aggregation == "std":
            aggregate_ds = ds.std(args.aggregate_on)
        else:
            assert False, f"Invalid aggregate argument: {args.aggregation}"

        aggregate_ds.materialize()

    benchmark.run_fn(str(vars(args)), benchmark_fn)
    benchmark.write_result()


if __name__ == "__main__":
    args = parse_args()
    main(args)
