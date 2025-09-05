# Audio batch inference

<div align="left">
<a target="_blank" href="https://console.anyscale.com/"><img src="https://img.shields.io/badge/🚀 Run_on-Anyscale-9hf"></a>&nbsp;
<a href="https://github.com/anyscale/e2e-audio" role="button"><img src="https://img.shields.io/static/v1?label=&amp;message=View%20On%20GitHub&amp;color=586069&amp;logo=github&amp;labelColor=2f363d"></a>&nbsp;
</div>


This tutorial demonstrates a batch inference pipeline that converts raw
audio files into a curated subset using two different ML models.
The pipeline chains together four stages that each run on the same Ray cluster using heterogeneous resources:

1. Stream the English validation split of [Common Voice 11.0](https://huggingface.co/datasets/mozilla-foundation/common_voice_11_0) into a Ray Dataset.
2. Resample each clip to 16 kHz for compatibility with Whisper.
3. Transcribe the audio with the `openai/whisper-large-v3-turbo` model.
4. Judge the educational quality of each transcription with a small Llama-3 model.
5. Persist only clips that score ≥ 3 to a Parquet dataset.

Ray Data is particularly powerful for this use case because it:
- **Parallelizes work** across a cluster of machines automatically
- **Handles heterogeneous compute resources** seamlessly
- Uses **lazy execution** to optimize the execution plan
- Processes data through each stage as soon as the first data block is available. This **streaming execution model** minimizes the time-to-first-result, eliminates large intermediate data storage, and maximizes resource utilization
- The same script scales to larger GPU clusters with minimal code changes

## Prerequisites

Install the dependencies using:

```bash
pip install -r requirements.txt
```

This tutorial runs on a cluster with five L4 GPU worker nodes.

## Setup

First, import the necessary modules:


```python
import io
import os

import numpy as np
import ray
import torch
import torchaudio
import torchaudio.transforms as T
from ray.data.llm import build_llm_processor, vLLMEngineProcessorConfig
from transformers import AutoModelForSpeechSeq2Seq, AutoProcessor

TRANSCRIPTION_MODEL = "openai/whisper-tiny"
JUDGE_MODEL = "unsloth/Meta-Llama-3.1-8B-Instruct"
```

## Streaming data ingestion

`ray.data.read_parquet` reads the records lazily **and** distributes them across the cluster.
This approach leverages every node's network bandwidth and starts work immediately without waiting
for the entire dataset download.
After loading, Ray divides the data into blocks and dispatches them to workers for processing.


```python
# Load the English subset of Common Voice 11.0.
raw_ds = ray.data.read_parquet("s3://anonymous@air-example-data/common_voice_11_0_audio_dataset.parquet")
```


```python
# Subsample for demonstration purposes.
raw_ds = raw_ds.limit(1000)
```

## Audio preprocessing

The Whisper checkpoint expects 16 kHz mono audio.  
The sample rate adjustment happens on CPU using TorchAudio, streaming the tensors
back into the Dataset. The operation runs in parallel, so a simple `ds.map` distributes the work across the cluster's CPU cores.


`ds.map()` applies the transformation function to each record in parallel across the cluster. Whenever possible, Ray avoids transferring objects across network connections to take advantage of zero-copy reads, avoiding serialization and deserialization overhead.

As soon as blocks of data finish preprocessing, they can move to the next stage without waiting for the entire dataset.


```python
def resample(item):
    # Resample at 16kHz, which is what openai/whisper-large-v3-turbo was trained on.
    audio_bytes = item.pop("audio_bytes")
    new_sampling_rate = 16000
    waveform, sampling_rate = torchaudio.load(io.BytesIO(audio_bytes), format="flac")
    waveform = T.Resample(sampling_rate, new_sampling_rate)(waveform).squeeze()
    item["arr"] = np.array(waveform)
    item["sampling_rate"] = new_sampling_rate
    return item


ds = raw_ds.map(resample)
```

Next, preprocess the data using Whisper's preprocessor. This step runs as a separate stage to scale it independently from the Whisper model itself.

`map_batches()` transforms entire batches of records at a time rather than individual items. By passing a class to `map_batches()`, Ray creates an Actor process that recycles state between batches. The `concurrency` parameter controls how many parallel workers process batches. The `batch_format="pandas"` setting converts batches to pandas DataFrames before processing.


```python
class WhisperPreprocessor:
    def __init__(self):
        self.processor = AutoProcessor.from_pretrained(TRANSCRIPTION_MODEL)

    def __call__(self, batch):
        # The Whisper processor expects a list of 1D NumPy arrays (mono audio).

        # Extract log-mel spectogram of audio.
        extracted_features = self.processor(
            batch["arr"].tolist(),
            sampling_rate=batch["sampling_rate"][0],  # Expects int, not list.
            return_tensors="np",
            device="cpu",  # Eligible for GPU, but reserving GPUs for the models.
        ).input_features
        # extracted_featues is a pd.Series of np.array shape (3000,).
        return {"input_features": [arr for arr in extracted_features], "id": batch["id"]}


ds = ds.map_batches(WhisperPreprocessor, batch_size=2, batch_format="pandas", concurrency=1)
# ds.show(1)
```

Running `ds.show(1)` displays an output similar to:

`[{'id': '19ba96...', 'transcription': ' It is from Westport above the villages of Morrisk and La Canvae.'}]`

Note that `ds.show(1)` would trigger execution of the pipeline up to this point due to Ray Data's lazy execution model. This approach means that the ingestion and preprocessing would actually run to produce the first result.

## GPU inference with Whisper

Ray Data can schedule user-defined Python callables on the appropriate resources.
The `Transcriber` below lazily loads `openai/whisper-large-v3-turbo` onto each GPU
worker the **first** time it runs, and subsequent batches reuse the warm model.

The `num_gpus=1` parameter tells Ray to run each replica on a node with a GPU. Ray Data automatically handles variable-sized batches based on available resources. If a worker fails, Ray automatically restarts the task on another node.


```python
class Transcriber:
    def __init__(self):
        self.device = "cuda"
        self.dtype = torch.float16
        self.model_id = "openai/whisper-tiny"
        self.model = AutoModelForSpeechSeq2Seq.from_pretrained(self.model_id, torch_dtype=self.dtype, low_cpu_mem_usage=True, use_safetensors=True)
        self.model.to(self.device)

    def __call__(self, batch):
        # Fuse all list of np.array into a single np.array for faster tensor creation.
        spectograms = np.array(batch["input_features"])
        spectograms = torch.tensor(spectograms).to(self.device, dtype=self.dtype)

        with torch.no_grad():
            # Generate token IDs for the batched input features.
            token_ids = self.model.generate(spectograms)

        return {"id": batch["id"], "token_ids": token_ids.cpu().numpy()}


# Transcribe audio to text tokens using Whisper.
# Use 2 workers using 1 GPU each.
ds = ds.map_batches(Transcriber, batch_size=2, batch_format="numpy", concurrency=2, num_gpus=1)
```

Now decode the tokens into actual transcriptions. This step decouples from the previous step to prevent GPU blocks on CPU work and avoid idle time. This approach also allows independent scaling of the number of decoders from the number of Whisper replicas.

Separating the GPU work from CPU work eliminates GPU idle. The `concurrency=5` and `batch_size=32` parameters show how to use more CPU workers and bigger batch sizes than GPU workers.


```python
class Decoder:
    def __init__(self):
        self.processor = AutoProcessor.from_pretrained(TRANSCRIPTION_MODEL)

    def __call__(self, batch):
        token_ids = batch.pop("token_ids")
        transcription = self.processor.batch_decode(token_ids, skip_special_tokens=True)
        batch["transcription"] = transcription
        return batch


ds = ds.map_batches(Decoder, batch_size=16, concurrency=5, batch_format="pandas")  # CPU only

# ds.take(1)
```

## LLM-based quality filter

A Llama-3 model serves as a *machine judge* that scores each transcription
from 1 👎 to 5 👍 on its educational value. The **LLM Processor** API wraps the heavy
lifting of batching, prompt formatting, and vLLM engine interaction using a declarative API style.

Ray Data provides a high-level API for integrating LLMs into data pipelines. The preprocessing and postprocessing functions handle data preparation and result parsing.


```python
# LLM as judge.
judge_config = vLLMEngineProcessorConfig(
    model_source=JUDGE_MODEL,
    engine_kwargs={
        "enable_chunked_prefill": True,
        "max_num_batched_tokens": 1028,
        "max_model_len": 4096,
        "guided_decoding_backend": "xgrammar",
        "dtype": torch.float16,
    },
    concurrency=3,
    batch_size=2,
)

processor = build_llm_processor(
    judge_config,
    preprocess=lambda row: dict(
        messages=[
            {
                "role": "system",
                "content": "You are an assistant that rates educational quality of a given text from a scale of 1 (not educational) to 5 (very educational). Only output your score, nothing else (no explanation, no comments, no other text). Acceptable outputs: 1, 2, 3, 4, 5",
            },
            {"role": "user", "content": row["transcription"]},
        ],
        sampling_params=dict(
            guided_decoding={"choice": ["1", "2", "3", "4", "5"]},
        ),
    ),
    postprocess=lambda row: dict(
        score=float(row.pop("generated_text")),  # Rename generated_text to score and convert to float.
        **row,  # Pass through the original fields.
    ),
)

# Rate educational quality of each transcription.
ds = processor(ds)

# Filter out uneducational content.
ds = ds.filter(expr="score >= 3")
# print(ds.take(1))
```

## Persist the curated subset

At this point a small, high-quality slice of Common Voice is ready for
downstream tasks such as fine-tuning or evaluation.  
The `write_parquet` call finally triggers execution of the whole pipeline and writes the blocks out in parallel
to the target storage location. This function writes outputs as they become available and automatically
shards the results into multiple files for efficient parallel reads in downstream steps.

As this is a distributed workload, the destination storage needs to be writable from all workers. This storage can be S3, NFS, or another network-attached solution. Anyscale simplifies this process by automatically creating and mounting [shared storage options](https://docs.anyscale.com/configuration/storage/#storage-shared-across-nodes) on every cluster.


```python
# Save the filtered dataset to a Parquet file.
# This line triggers the lazy execution of the entire pipeline.
output_dir = "/mnt/cluster_storage/filtered_dataset/"
os.makedirs(output_dir, exist_ok=True)
ds.write_parquet(output_dir)
```

`write_parquet` triggers the full execution of the data pipeline and streams the results into a series of local Parquet files. Ray automatically shards these files across multiple outputs for efficient downstream reading:


```python
# List the files in the output directory.
print(os.listdir(output_dir))
```
