"""
Benchmark a text embeddings job
"""

import argparse
import uuid
import time
from typing import Dict, List
from numpy import ndarray

import ray
import torch
from sentence_transformers import SentenceTransformer
from langchain_text_splitters import (
    RecursiveCharacterTextSplitter,
    CharacterTextSplitter,
)

from benchmark import Benchmark, BenchmarkMetric


SOURCE_DIRECTORY_S3 = "s3://air-example-data/common-pile-mirror/arxiv_papers/"
# Add a random prefix to avoid conflicts between different runs.
WRITE_PATH = f"s3://ray-data-write-benchmark/{uuid.uuid4().hex}/"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Text Embeddings Batch Inference Benchmark"
    )
    parser.add_argument(
        "--source-directory",
        type=str,
        default=SOURCE_DIRECTORY_S3,
        help="S3 URI of source documents",
    )
    parser.add_argument(
        "--read-concurrency",
        type=int,
        default=None,
        help="Number of concurrent readers for input files",
    )
    parser.add_argument(
        "--num-blocks",
        type=int,
        default=None,
        help="Number of blocks to override for input file reading",
    )
    parser.add_argument(
        "--chunk-concurrency",
        type=int,
        default=20,
        help="Concurrency for Chunker stage",
    )
    parser.add_argument(
        "--chunk-cpus", type=int, default=None, help="Number of CPUs per Chunker"
    )
    parser.add_argument(
        "--chunk-method",
        choices=["fixed", "recursive"],
        default="recursive",
        help="Chunking method",
    )
    parser.add_argument(
        "--chunk-size", type=int, default=2048, help="Chunk size for text splitting"
    )
    parser.add_argument(
        "--chunk-overlap",
        type=int,
        default=200,
        help="Chunk overlap for text splitting",
    )
    parser.add_argument(
        "--embed-batch-size",
        type=int,
        default=8,
        help="Batch size for embedding inference",
    )
    parser.add_argument(
        "--embed-concurrency",
        type=int,
        default=15,
        help="Number of Embedder replicas",
    )
    parser.add_argument(
        "--num-gpus", type=int, default=1, help="Number of GPUs per Embedder"
    )
    parser.add_argument(
        "--model-name",
        type=str,
        default="Salesforce/SFR-Embedding-Mistral",
        help="Embedding model name",
    )
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        help="Runs a smoke test with a small subset of the data",
    )
    parser.add_argument(
        "--chaos-test",
        action="store_true",
        default=False,
        help="Enable chaos testing to simulate node failures",
    )
    return parser.parse_args()


class Chunker:
    def __init__(self, method: str, chunk_size: int, chunk_overlap: int):
        if method == "fixed":
            self.splitter = CharacterTextSplitter(
                chunk_size=chunk_size, chunk_overlap=chunk_overlap
            )
        else:
            self.splitter = RecursiveCharacterTextSplitter(
                chunk_size=chunk_size, chunk_overlap=chunk_overlap
            )

    def __call__(self, page: Dict) -> List[Dict]:
        return [
            {
                "text": text,
                "source": page["source"],
                "chunk_id": f"{page['id']}_{str(uuid.uuid4())}",
                "doc_id": page["id"],
            }
            for text in self.splitter.split_text(page["text"])
        ]


class Embedder:
    def __init__(self, model_name: str):
        self.model = SentenceTransformer(
            model_name, device="cuda" if torch.cuda.is_available() else "cpu"
        )

    def __call__(self, batch: Dict[str, ndarray]) -> Dict[str, ndarray]:
        batch["embeddings"] = self.model.encode(
            batch["text"], convert_to_numpy=True, batch_size=len(batch["text"])
        )
        return batch


def main(args):
    ds = ray.data.read_parquet(
        args.source_directory,
        include_paths=True,
        concurrency=args.read_concurrency,
        override_num_blocks=args.num_blocks,
    )
    # Record start time after metadata fetching
    start_time_without_metadata_fetching = time.time()
    if args.smoke_test:
        ds = ds.limit(100)
    ds = ds.flat_map(
        Chunker(
            method=args.chunk_method,
            chunk_size=args.chunk_size,
            chunk_overlap=args.chunk_overlap,
        ),
        concurrency=args.chunk_concurrency,
        num_cpus=args.chunk_cpus,
    )
    ds = ds.map_batches(
        Embedder,
        fn_constructor_kwargs={"model_name": args.model_name},
        batch_size=args.embed_batch_size,
        concurrency=args.embed_concurrency,
        num_gpus=args.num_gpus,
    )
    start = time.time()
    ds.write_parquet(WRITE_PATH)
    duration = time.time() - start
    count = ds.count()
    throughput = count / duration if duration > 0 else 0.0

    # Compute metrics for time and throughput without metadata fetch
    total_time_s_wo_metadata_fetch = time.time() - start_time_without_metadata_fetching
    throughput_rows_s_wo_metadata_fetch = (
        count / total_time_s_wo_metadata_fetch
        if total_time_s_wo_metadata_fetch > 0
        else 0.0
    )

    # Report chaos testing node failures
    if args.chaos_test:
        dead_nodes = [node["NodeID"] for node in ray.nodes() if not node["Alive"]]
        assert dead_nodes, "No dead nodes during chaos test"
        print(f"Total chaos killed: {dead_nodes}")

    return {
        BenchmarkMetric.RUNTIME: duration,
        BenchmarkMetric.NUM_ROWS: count,
        BenchmarkMetric.THROUGHPUT: throughput,
        "source_directory": args.source_directory,
        "model_name": args.model_name,
        "chunk_method": args.chunk_method,
        "start_time_without_metadata_fetching": start_time_without_metadata_fetching,
        "total_time_s_wo_metadata_fetch": total_time_s_wo_metadata_fetch,
        "throughput_rows_s_wo_metadata_fetch": throughput_rows_s_wo_metadata_fetch,
        "chaos_test": args.chaos_test,
    }


if __name__ == "__main__":
    args = parse_args()
    print(f"Writing to {WRITE_PATH}")
    benchmark = Benchmark()
    benchmark.run_fn("text-embeddings-benchmark", main, args)
    benchmark.write_result()
