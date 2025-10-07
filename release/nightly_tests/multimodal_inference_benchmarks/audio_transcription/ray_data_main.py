from __future__ import annotations

import io
import time
import uuid

import numpy as np
import ray
import torch
import torchaudio
import torchaudio.transforms as T
from transformers import AutoModelForSpeechSeq2Seq, AutoProcessor


TRANSCRIPTION_MODEL = "openai/whisper-tiny"
NUM_GPUS = 8
SAMPLING_RATE = 16000
INPUT_PATH = "s3://anonymous@ray-example-data/common_voice_17/parquet/"
OUTPUT_PATH = f"s3://ray-data-write-benchmark/{uuid.uuid4().hex}"
BATCH_SIZE = 64

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


start_time = time.time()

ds = ray.data.read_parquet(INPUT_PATH)
ds = ds.map(resample)
ds = ds.map_batches(whisper_preprocess, batch_size=BATCH_SIZE)
ds = ds.map_batches(
    Transcriber,
    batch_size=BATCH_SIZE,
    concurrency=NUM_GPUS,
    num_gpus=1,
)
ds = ds.map_batches(decoder)
ds.write_parquet(OUTPUT_PATH)

print("Runtime:", time.time() - start_time)
