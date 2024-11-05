import ray

from benchmark import Benchmark

import argparse
import requests
from requests.adapters import HTTPAdapter, Retry
from typing import Callable


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TODO")
    parser.add_argument(
        "--data-path",
        type=str,
    )
    parser.add_argument(
        "--data-format", choices=["image", "parquet", "tfrecords", "uris"]
    )
    parser.add_argument("--consumption", choices=["count", "iterate"])
    return parser.parse_args()


def main(args):
    benchmark = Benchmark("read-images")
    read_fn = get_read_fn(args.data_format)
    consume_fn = get_consume_fn(args.consumption)

    def benchmark_fn():
        ds = read_fn(args.data_path)
        consume_fn(ds)

    benchmark.run_fn(benchmark_fn)
    benchmark.write_result()


def get_read_fn(data_format: str) -> Callable[[str], ray.data.Dataset]:
    if args.data_format == "image":
        read_fn = ray.data.read_images
    elif args.data_format == "parquet":
        read_fn = ray.data.read_parquet
    elif args.data_format == "tfrecords":
        read_fn = ray.data.read_tfrecords
    elif args.data_format == "uris":

        def download_uri(row):
            # Configure retries for transient errors.
            session = requests.Session()
            retry = Retry(
                total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504]
            )
            session.mount("http://", HTTPAdapter(max_retries=retry))

            try:
                response = requests.get(row["uri"], timeout=10)
                # Raises an HTTPError for bad responses (4xx or 5xx)
                response.raise_for_status()
                row["data"] = response.content
            except requests.RequestException as e:
                print(f"An error occurred: {e}")

            return row

        def read_fn(path):
            return ray.data.read_parquet(path).map(download_uri)

    return read_fn


def get_consume_fn(consumption: str) -> Callable[[ray.data.Dataset], None]:
    if consumption == "count":

        def consume_fn(ds):
            ds.count()

    elif consumption == "iterate":

        def consume_fn(ds):
            for _ in ds.iter_batches():
                pass

    return consume_fn


if __name__ == "__main__":
    args = parse_args()
    main(args)
