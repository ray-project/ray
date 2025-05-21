# Audio Curation with Ray Data – Offline Batch Inference Tutorial

This script demonstrates an end-to-end *batch inference* pipeline that turns raw
audio files into a curated subset using **Ray Data's streaming
execution engine**.

The pipeline chains together four stages that each run on the same Ray cluster using heterogeneous resources:

1. 🔍 Distributed ingestion from the `common_voice_11_0` dataset (CPU)
2. 🎚️ Audio preprocessing – format conversion, sample rate adjustment, mel spectogram feature extraction (CPU)
3. 🗣️ Speech-to-text transcription with Whisper (GPU)
4. Detokenization (CPU)
5. 🏷️ LLM-based educational rating & filtering (GPU)

Ray Datasets enable stages to begin
processing as soon as the first data *block* becomes available. This **streaming
execution** model drastically reduces time-to-first-result and eliminates large
intermediate data storage.

Ray Data is particularly powerful for this use case because it:
- **Parallelizes work** across a cluster of machines automatically
- **Handles heterogeneous compute resources** (CPUs and GPUs) seamlessly
- Uses **lazy execution** to optimize the execution plan
- Supports efficient **streaming** to minimize time-to-first-result and maximize resource utilization

Note: this tutorial runs on a cluster with five L4 GPU worker nodes.


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

## 1. Streaming data ingestion

`ray.data.read_parquet` reads the records lazily **and** distributes them across the cluster.
This approach leverages every node's network bandwidth and starts work immediately without waiting
for the entire dataset download.
After loading, Ray divides the data into blocks and dispatches them to workers for processing.


```python
# Load the English subset of Common Voice 11.0
raw_ds = ray.data.read_parquet("s3://anonymous@air-example-data/common_voice_11_0_audio_dataset.parquet")
```


```python
# Subsample for demonstration purposes
raw_ds = raw_ds.limit(1000)
```

## 2. Audio preprocessing

The Whisper checkpoint expects 16 kHz mono audio.  
The sample rate adjustment happens on CPU using TorchAudio, streaming the tensors right
back into the Dataset. The operation runs in parallel, so a simple `ds.map` distributes the work across the cluster's CPU cores.


`ds.map()` applies the transformation function to each record in parallel across the cluster. Whenever possible, Ray avoids transferring objects across network connections to take advantage of zero-copy reads, avoiding serialization/deserialization overhead.

As soon as blocks of data finish preprocessing, they can move to the next stage without waiting for the entire dataset.


```python
def resample(item):
    # Resample at 16kHz, which is what openai/whisper-large-v3-turbo was trained on
    audio_bytes = item.pop("audio_bytes")
    new_sampling_rate = 16000
    waveform, sampling_rate = torchaudio.load(io.BytesIO(audio_bytes))
    waveform = T.Resample(sampling_rate, new_sampling_rate)(waveform).squeeze()
    item["arr"] = np.array(waveform)
    item["sampling_rate"] = new_sampling_rate
    return item


ds = raw_ds.map(resample)
```

Next, preprocess the data using Whisper's preprocessor. This runs as a separate stage to scale it independently from the Whisper model itself.

Here, `map_batches()` transforms entire batches of records at a time rather than individual items. By passing a class to `map_batches()`, Ray creates an Actor process that recycles state between batches. The `concurrency` parameter controls how many parallel workers process batches. The `batch_format="pandas"` setting converts batches to pandas DataFrames before processing.


```python
class WhisperPreprocessor:
    def __init__(self):
        self.processor = AutoProcessor.from_pretrained(TRANSCRIPTION_MODEL)

    def __call__(self, batch):
        # The Whisper processor expects a list of 1D NumPy arrays (mono audio)

        # Extract log-mel spectogram of audio
        extracted_features = self.processor(
            batch["arr"].tolist(),
            sampling_rate=batch["sampling_rate"][0],  # expects int, not list
            return_tensors="np",
            device="cpu",  # Eligible for GPU, but let's reserve GPUs for the models
        ).input_features
        # extracted_featues is a pd.Series of np.array shape (3000,)
        return {"input_features": [arr for arr in extracted_features], "id": batch["id"]}


ds = ds.map_batches(WhisperPreprocessor, batch_size=2, batch_format="pandas", concurrency=1)
# ds.show(1)
```

Running `ds.show(1)` displays an output similar to:

`[{'id': '19ba96...', 'transcription': ' It is from Westport above the villages of Morrisk and La Canvae.'}]`

Note that `ds.show(1)` would trigger execution of the pipeline up to this point due to Ray Data's lazy execution model. This means the ingestion and preprocessing would actually run to produce the first result.

## 3. GPU inference with Whisper

Ray Data can schedule user-defined Python callables on the appropriate resources.
The `Transcriber` below lazily loads `openai/whisper-large-v3-turbo` onto each GPU
worker the **first** time it runs; subsequent batches reuse the warm model.

The `num_gpus=1` parameter tells Ray to run each replica on a node with a GPU. Ray Data automatically handles variable-sized batches based on available resources. If a worker fails, Ray will automatically restart the task on another node.


```python
class Transcriber:
    def __init__(self):
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.dtype = torch.float16 if torch.cuda.is_available() else torch.float32
        self.model_id = "openai/whisper-tiny"
        self.model = AutoModelForSpeechSeq2Seq.from_pretrained(self.model_id, torch_dtype=self.dtype, low_cpu_mem_usage=True, use_safetensors=True)
        self.model.to(self.device)

    def __call__(self, batch):
        # fuse all list of np.array into a single np.array for faster tensor creation
        spectograms = np.array(batch["input_features"])
        spectograms = torch.tensor(spectograms).to(self.device, dtype=self.dtype)

        with torch.no_grad():
            # Generate token IDs for the batched input features
            token_ids = self.model.generate(spectograms)

        return {"id": batch["id"], "token_ids": token_ids.cpu().numpy()}


# Transcribe audio to text tokens using Whisper
# Here, we use 2 workers using 1 GPU each
ds = ds.map_batches(Transcriber, batch_size=4, batch_format="numpy", concurrency=2, num_gpus=1 if torch.cuda.is_available() else 0)
```

Now we can decode the tokens into actual transcriptions. We decouple this from the previous step so that we don't block our GPUs on CPU work and avoid idle time. This also allows us to independently scale the number of decoders from the number of Whisper replicas.

By separating GPU work from CPU work, we maximize GPU utilization. The `concurrency=5` and `batch_size=32` parameters show how we can use more CPU workers and bigger batch sizes than our GPU workers.


```python
class Decoder:
    def __init__(self):
        self.processor = AutoProcessor.from_pretrained(TRANSCRIPTION_MODEL)

    def __call__(self, batch):
        token_ids = batch.pop("token_ids")
        transcription = self.processor.batch_decode(token_ids, skip_special_tokens=True)
        batch["transcription"] = transcription
        return batch


ds = ds.map_batches(Decoder, batch_size=32, concurrency=5, batch_format="pandas")  # CPU only

# ds.take(1)
```

## 4. LLM-based quality filter

We use a Llama-3 checkpoint as a *machine judge* that scores each transcription
from 1 👎 to 5 👍 on its educational value. The **LLM Processor** API wraps the heavy
lifting of batching, prompt formatting, and vLLM engine interaction so we can keep
this section declarative.

Ray Data provides a high-level API for integrating LLMs into data pipelines. The preprocessing and postprocessing functions handle data preparation and result parsing.


```python
# LLM as Judge
judge_config = vLLMEngineProcessorConfig(
    model_source=JUDGE_MODEL,
    engine_kwargs={
        "enable_chunked_prefill": True,
        "max_num_batched_tokens": 1028,
        "max_model_len": 4096,
        "guided_decoding_backend": "auto",
        "dtype": torch.float16 if torch.cuda.is_available() else torch.float32,
    },
    concurrency=3,
    batch_size=8,
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
        score=float(row.pop("generated_text")),  # rename generated_text to score and convert to float
        **row,  # pass through the original fields
    ),
)

# Rate educational quality of each transcription
ds = processor(ds)

# Filter out uneducational content
ds = ds.filter(expr="score >= 3")
# print(ds.take(1))
```

## 5. Persist the curated subset

At this point we have a small, high-quality slice of Common Voice ready for
downstream tasks such as fine-tuning or evaluation.  
The `write_parquet` call finally triggers execution of the whole pipeline and writes the blocks out in parallel
to the target storage location.

Here, we use Anyscale's shared cluster network storage so that each worker can write its own output files in parallel. This is done in incremental fashion, so that results are written as soon as they are available. The files are automatically sharded across multiple files for efficient downstream reading.


```python
# Save the filtered dataset to a parquet file
# This line triggers the lazy execution of the entire pipeline
output_dir = "/mnt/cluster_storage/filtered_dataset/"
os.makedirs(output_dir, exist_ok=True)
ds.write_parquet(output_dir)
```

`write_parquet` triggers the full execution of the data pipeline and streams the results into a series of local parquet files. Let's view them:


```python
# List the files in the output directory
print(os.listdir(output_dir))
```
