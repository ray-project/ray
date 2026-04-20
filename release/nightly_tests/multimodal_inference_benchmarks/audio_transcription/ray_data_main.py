from __future__ import annotations

import argparse
import io
import uuid

import numpy as np
import ray
import torch
import torchaudio
import torchaudio.transforms as T
from transformers import AutoModelForSpeechSeq2Seq, AutoProcessor
from benchmark import Benchmark


TRANSCRIPTION_MODEL = "openai/whisper-tiny"
SAMPLING_RATE = 16000
INPUT_PATH = "s3://anonymous@ray-example-data/common_voice_17/parquet/"
OUTPUT_PATH = f"s3://ray-data-write-benchmark/{uuid.uuid4().hex}"
BATCH_SIZE = 64


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--preprocess-batch-size",
        type=lambda v: "auto" if v == "auto" else int(v),
        default=None,
        help=(
            "Batch size for CPU preprocessing and decoding stages. "
            "Use 'auto' to let Ray Data pick based on data size. "
            "Defaults to 1024 (Ray Data default)."
        ),
    )
    return parser.parse_args()


args = parse_args()
PREPROCESS_BATCH_SIZE = args.preprocess_batch_size if args.preprocess_batch_size is not None else 1024

ray.init()


@ray.remote
def warmup():
    pass


# NOTE: On a fresh Ray cluster, it can take a minute or longer to schedule the first
#       task. To ensure benchmarks compare data processing speed and not cluster startup
#       overhead, this code launches a several tasks as warmup.
ray.get([warmup.remote() for _ in range(64)])


def resample(item):
    # NOTE: Remove the `audio` column since we don't need it anymore. This is done by
    # the system automatically on Ray Data 2.51+ with the `with_column` API.
    audio = item.pop("audio")
    audio_bytes = audio["bytes"]
    waveform, sampling_rate = torchaudio.load(io.BytesIO(audio_bytes), format="flac")
    waveform = T.Resample(sampling_rate, SAMPLING_RATE)(waveform).squeeze()
    item["arr"] = np.array(waveform)
    return item


processor = AutoProcessor.from_pretrained(TRANSCRIPTION_MODEL)


def whisper_preprocess(batch):
    array = batch.pop("arr")
    extracted_features = processor(
        array.tolist(),
        sampling_rate=SAMPLING_RATE,
        return_tensors="np",
        device="cpu",
    ).input_features
    batch["input_features"] = list(extracted_features)
    return batch


class Transcriber:
    def __init__(self):
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.dtype = torch.float16
        self.model_id = TRANSCRIPTION_MODEL
        self.model = AutoModelForSpeechSeq2Seq.from_pretrained(
            self.model_id,
            torch_dtype=self.dtype,
            low_cpu_mem_usage=True,
            use_safetensors=True,
        )
        self.model.to(self.device)

    def __call__(self, batch):
        input_features = batch.pop("input_features")
        spectrograms = np.array(input_features)
        spectrograms = torch.tensor(spectrograms).to(self.device, dtype=self.dtype)
        with torch.no_grad():
            token_ids = self.model.generate(spectrograms)
        batch["token_ids"] = token_ids.cpu().numpy()
        return batch


def decoder(batch):
    # NOTE: Remove the `token_ids` column since we don't need it anymore. This is done by
    # the system automatically on Ray Data 2.51+ with the `with_column` API.
    token_ids = batch.pop("token_ids")
    transcription = processor.batch_decode(token_ids, skip_special_tokens=True)
    batch["transcription"] = transcription
    batch["transcription_length"] = np.array([len(t) for t in transcription])
    return batch


def run_pipeline():
    ds = ray.data.read_parquet(INPUT_PATH)
    ds = ds.map(resample)
    ds = ds.map_batches(whisper_preprocess, batch_size=PREPROCESS_BATCH_SIZE)
    ds = ds.map_batches(
        Transcriber,
        batch_size=BATCH_SIZE,
        num_gpus=1,
    )
    ds = ds.map_batches(decoder, batch_size=PREPROCESS_BATCH_SIZE)
    ds.write_parquet(OUTPUT_PATH)


benchmark = Benchmark()
benchmark.run_fn("main", run_pipeline)
benchmark.write_result()
