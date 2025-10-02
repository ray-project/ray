# This file is adapted from https://github.com/Eventual-Inc/Daft/tree/9da265d8f1e5d5814ae871bed3cee1b0757285f5/benchmarking/ai/audio_transcription
from __future__ import annotations

import io
import time
import uuid

import ray
import numpy as np
import torch
import torchaudio
import torchaudio.transforms as T
from transformers import AutoModelForSpeechSeq2Seq, AutoProcessor

import daft

TRANSCRIPTION_MODEL = "openai/whisper-tiny"
NUM_GPUS = 8
NEW_SAMPLING_RATE = 16000
INPUT_PATH = "s3://anonymous@ray-example-data/common_voice_17/parquet/"
OUTPUT_PATH = f"s3://ray-data-write-benchmark/{uuid.uuid4().hex}"

daft.context.set_runner_ray()


@ray.remote
def warmup():
    pass


# NOTE: On a fresh Ray cluster, it can take a minute or longer to schedule the first
#       task. To ensure benchmarks compare data processing speed and not cluster startup
#       overhead, this code launches a several tasks as warmup.
ray.get([warmup.remote() for _ in range(64)])


def resample(audio_bytes):
    waveform, sampling_rate = torchaudio.load(io.BytesIO(audio_bytes), format="flac")
    waveform = T.Resample(sampling_rate, NEW_SAMPLING_RATE)(waveform).squeeze()
    return np.array(waveform)


processor = AutoProcessor.from_pretrained(TRANSCRIPTION_MODEL)


@daft.udf(return_dtype=daft.DataType.tensor(daft.DataType.float32()))
def whisper_preprocess(resampled):
    extracted_features = processor(
        resampled.to_arrow().to_numpy(zero_copy_only=False).tolist(),
        sampling_rate=NEW_SAMPLING_RATE,
        device="cpu",
    ).input_features
    return extracted_features


@daft.udf(
    return_dtype=daft.DataType.list(daft.DataType.int32()),
    batch_size=64,
    concurrency=NUM_GPUS,
    num_gpus=1,
)
class Transcriber:
    def __init__(self) -> None:
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.dtype = torch.float16
        self.model = AutoModelForSpeechSeq2Seq.from_pretrained(
            TRANSCRIPTION_MODEL,
            torch_dtype=self.dtype,
            low_cpu_mem_usage=True,
            use_safetensors=True,
        )
        self.model.to(self.device)

    def __call__(self, extracted_features):
        spectrograms = np.array(extracted_features)
        spectrograms = torch.tensor(spectrograms).to(self.device, dtype=self.dtype)
        with torch.no_grad():
            token_ids = self.model.generate(spectrograms)

        return token_ids.cpu().numpy()


@daft.udf(return_dtype=daft.DataType.string())
def decoder(token_ids):
    transcription = processor.batch_decode(token_ids, skip_special_tokens=True)
    return transcription


start_time = time.time()

df = daft.read_parquet(INPUT_PATH)
df = df.with_column(
    "resampled",
    df["audio"]["bytes"].apply(
        resample, return_dtype=daft.DataType.list(daft.DataType.float32())
    ),
)
df = df.with_column("extracted_features", whisper_preprocess(df["resampled"]))
df = df.with_column("token_ids", Transcriber(df["extracted_features"]))
df = df.with_column("transcription", decoder(df["token_ids"]))
df = df.with_column("transcription_length", df["transcription"].str.length())
df = df.exclude("token_ids", "extracted_features", "resampled")
df.write_parquet(OUTPUT_PATH)

print("Runtime:", time.time() - start_time)
