import os
from timeit import default_timer as timer
from typing import Dict
import numpy as np
import torch

import ray
from ray.anyscale.data import AudioDatasource
from benchmark import Benchmark, BenchmarkMetric
import whisper

DATA_URI = (
    "s3://anonymous@air-example-data-2/"
    "6G-audio-data-LibriSpeech-train-clean-100-flac/train-clean-100/"
)


def parse_args():
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--batch-size",
        default=110,
        type=int,
        help="Batch size to use.",
    )

    args = parser.parse_args()
    return args


def run_benchmark(args):
    """Read in ~6GB of audio files (FLAC format) from a nested S3 bucket,
    and perform speech to text as the batch inference task.
    Reports the time taken and throughput in bytes."""
    ray.init()
    actor_pool_size = int(ray.cluster_resources().get("GPU"))

    ds = (
        ray.data.read_datasource(
            AudioDatasource(
                paths=DATA_URI,
                include_paths=True,
                file_extensions=["flac"],
            )
        )
        .map(preprocess)
        .map_batches(
            BatchInference,
            compute=ray.data.ActorPoolStrategy(size=actor_pool_size),
            batch_size=args.batch_size,
            num_gpus=1,
        )
    )

    total_size_bytes = 0
    start_time = timer()
    for batch in ds.iter_batches(batch_size=None, batch_format="pyarrow"):
        total_size_bytes += sum(batch["audio_size_bytes"].to_pylist())
    end_time = timer()

    total_time = end_time - start_time
    throughput_bytes = total_size_bytes / total_time

    # For structured output integration with internal tooling
    results = {
        BenchmarkMetric.RUNTIME: total_time,
        BenchmarkMetric.THROUGHPUT: throughput_bytes,
    }
    return results


def preprocess(row):
    assert row["amplitude"].shape[0] == 1, (
        "OpenAI Whisper only supports mono audio,"
        f" but found {row['amplitude'].shape[0]} channels."
    )
    processed_amplitude = whisper.pad_or_trim(np.squeeze(row["amplitude"], axis=0))
    row["audio_size_bytes"] = processed_amplitude.nbytes
    row["mel"] = whisper.log_mel_spectrogram(processed_amplitude)
    del row["amplitude"]
    return row


class BatchInference:
    def __init__(self):
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model = whisper.load_model("base", self.device)
        self.options = whisper.DecodingOptions()

    def __call__(self, batch: Dict[str, np.ndarray]):
        inputs = torch.as_tensor(batch["mel"], device=self.device)
        # Pass the entire batch of processed inputs into the model at once.
        outputs = whisper.decode(self.model, inputs, self.options)
        return {
            "path": batch["path"],
            "audio_size_bytes": batch["audio_size_bytes"],
            "transcribed_text": [result.text for result in outputs],
        }


if __name__ == "__main__":
    args = parse_args()
    benchmark = Benchmark("audio_batch_inference_benchmark")
    benchmark.run_fn("main", run_benchmark, args)
    test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/release_test_out.json")
    benchmark.write_result(test_output_json)
