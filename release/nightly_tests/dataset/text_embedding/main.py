import argparse
from typing import Dict
import uuid
import boto3
import json

import numpy as np
import pyarrow as pa
from sentence_transformers import SentenceTransformer
import torch

from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from ray._private.test_utils import EC2InstanceTerminatorWithGracePeriod
import ray

from benchmark import Benchmark

BATCH_SIZE = 128

# This dataset has 50 files, each with 20,000 rows of <1024-token text spans. It
# includes one empty Parquet file and some nulls. See `create_dataset.py` for details.
INPUT_PREFIX = "s3://ray-benchmark-data-internal-us-west-2/text-spans"
# Add a random prefix to avoid conflicts between different runs.
OUTPUT_PREFIX = f"s3://ray-data-write-benchmark/{uuid.uuid4().hex}"

# These are used to fetch the HF token from AWS Secrets Manager.
SECRET_REGION_NAME = "us-west-2"
SECRET_ID = (
    "arn:aws:secretsmanager:us-west-2:188439194153:secret:release_test_hf_token-p3Lcqy"
)

# FIXME: We need to explicitly define the schema and specify lists of variable-size
# binaries because Ray Data can't handle lists of fixed-size binaries.
SCHEMA = pa.schema(
    [
        ("metadata00", pa.string()),
        ("metadata01", pa.list_(pa.binary())),
        ("metadata02", pa.string()),
        ("metadata03", pa.uint64()),
        ("metadata04", pa.list_(pa.binary())),
        ("metadata05", pa.list_(pa.binary())),
        ("metadata06", pa.binary()),
        ("metadata07", pa.string()),
        ("metadata08", pa.binary()),
        ("metadata09", pa.uint64()),
        ("metadata10", pa.binary()),
        ("metadata11", pa.list_(pa.binary())),
        ("metadata12", pa.uint64()),
        ("metadata13", pa.uint64()),
        ("metadata14", pa.list_(pa.binary())),
        ("span_text", pa.string()),
        ("metadata15", pa.binary()),
        ("metadata16", pa.string()),
        ("metadata17", pa.list_(pa.binary())),
        ("metadata18", pa.list_(pa.binary())),
    ]
)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--inference-concurrency",
        nargs=2,
        type=int,
        required=True,
        help="The minimum and maximum concurrency for the inference operator.",
    )
    parser.add_argument(
        "--chaos",
        action="store_true",
        help=(
            "Whether to enable chaos. If set, this script terminates one worker node "
            "every minute with a grace period."
        ),
    )
    return parser.parse_args()


def main(args: argparse.Namespace):
    benchmark = Benchmark()

    if args.chaos:
        start_chaos()

    def benchmark_fn():
        (
            ray.data.read_parquet(INPUT_PREFIX, schema=SCHEMA)
            .repartition(target_num_rows_per_block=256)
            .map_batches(
                EncodingUDF,
                concurrency=tuple(args.inference_concurrency),
                num_gpus=1,
                batch_size=BATCH_SIZE,
                fn_constructor_kwargs={"model": "BAAI/bge-m3", "token": get_hf_token()},
            )
            .write_parquet(OUTPUT_PREFIX, mode="overwrite")
        )

    benchmark.run_fn("main", benchmark_fn)
    benchmark.write_result()


def start_chaos():
    assert ray.is_initialized()

    head_node_id = ray.get_runtime_context().get_node_id()
    scheduling_strategy = NodeAffinitySchedulingStrategy(
        node_id=head_node_id, soft=False
    )
    resource_killer = EC2InstanceTerminatorWithGracePeriod.options(
        scheduling_strategy=scheduling_strategy
    ).remote(head_node_id, max_to_kill=None)

    ray.get(resource_killer.ready.remote())

    resource_killer.run.remote()


class EncodingUDF:
    def __init__(self, model: str, token: str):
        device = "cuda" if torch.cuda.is_available() else "cpu"
        self._model = SentenceTransformer(
            model,
            device=device,
            token=token,
            model_kwargs={"torch_dtype": torch.bfloat16},
        )

    def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        batch["vector"] = self._model.encode(
            batch["span_text"], batch_size=BATCH_SIZE, convert_to_numpy=True
        )
        return batch


def get_hf_token() -> str:
    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager", region_name=SECRET_REGION_NAME
    )
    secret_string = client.get_secret_value(SecretId=SECRET_ID)["SecretString"]
    return json.loads(secret_string)["HF_TOKEN"]


if __name__ == "__main__":
    ray.init()
    args = parse_args()
    main(args)
