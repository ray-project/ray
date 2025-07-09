{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# Audio batch inference\n",
    "\n",
    "<div align=\"left\">\n",
    "<a target=\"_blank\" href=\"https://console.anyscale.com/\"><img src=\"https://img.shields.io/badge/🚀 Run_on-Anyscale-9hf\"></a>&nbsp;\n",
    "<a href=\"https://github.com/anyscale/e2e-audio\" role=\"button\"><img src=\"https://img.shields.io/static/v1?label=&amp;message=View%20On%20GitHub&amp;color=586069&amp;logo=github&amp;labelColor=2f363d\"></a>&nbsp;\n",
    "</div>\n",
    "\n",
    "\n",
    "This tutorial demonstrates a batch inference pipeline that converts raw\n",
    "audio files into a curated subset using two different ML models.\n",
    "The pipeline chains together four stages that each run on the same Ray cluster using heterogeneous resources:\n",
    "\n",
    "1. Stream the English validation split of [Common Voice 11.0](https://huggingface.co/datasets/mozilla-foundation/common_voice_11_0) into a Ray Dataset.\n",
    "2. Resample each clip to 16 kHz for compatibility with Whisper.\n",
    "3. Transcribe the audio with the `openai/whisper-large-v3-turbo` model.\n",
    "4. Judge the educational quality of each transcription with a small Llama-3 model.\n",
    "5. Persist only clips that score ≥ 3 to a Parquet dataset.\n",
    "\n",
    "Ray Data is particularly powerful for this use case because it:\n",
    "- **Parallelizes work** across a cluster of machines automatically\n",
    "- **Handles heterogeneous compute resources** seamlessly\n",
    "- Uses **lazy execution** to optimize the execution plan\n",
    "- Processes data through each stage as soon as the first data block is available. This **streaming execution model** minimizes the time-to-first-result, eliminates large intermediate data storage, and maximizes resource utilization\n",
    "- The same script scales to larger GPU clusters with minimal code changes\n",
    "\n",
    "## Prerequisites\n",
    "\n",
    "Install the dependencies using:\n",
    "\n",
    "```bash\n",
    "pip install -r requirements.txt\n",
    "```\n",
    "\n",
    "This tutorial runs on a cluster with five L4 GPU worker nodes.\n",
    "\n",
    "## Setup\n",
    "\n",
    "First, import the necessary modules:"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "import io\n",
    "import os\n",
    "\n",
    "import numpy as np\n",
    "import ray\n",
    "import torch\n",
    "import torchaudio\n",
    "import torchaudio.transforms as T\n",
    "from ray.data.llm import build_llm_processor, vLLMEngineProcessorConfig\n",
    "from transformers import AutoModelForSpeechSeq2Seq, AutoProcessor\n",
    "\n",
    "TRANSCRIPTION_MODEL = \"openai/whisper-tiny\"\n",
    "JUDGE_MODEL = \"unsloth/Meta-Llama-3.1-8B-Instruct\""
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "vscode": {
     "languageId": "plaintext"
    }
   },
   "source": [
    "## Streaming data ingestion\n",
    "\n",
    "`ray.data.read_parquet` reads the records lazily **and** distributes them across the cluster.\n",
    "This approach leverages every node's network bandwidth and starts work immediately without waiting\n",
    "for the entire dataset download.\n",
    "After loading, Ray divides the data into blocks and dispatches them to workers for processing."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Load the English subset of Common Voice 11.0.\n",
    "raw_ds = ray.data.read_parquet(\"s3://anonymous@air-example-data/common_voice_11_0_audio_dataset.parquet\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Subsample for demonstration purposes.\n",
    "raw_ds = raw_ds.limit(1000)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Audio preprocessing\n",
    "\n",
    "The Whisper checkpoint expects 16 kHz mono audio.  \n",
    "The sample rate adjustment happens on CPU using TorchAudio, streaming the tensors\n",
    "back into the Dataset. The operation runs in parallel, so a simple `ds.map` distributes the work across the cluster's CPU cores.\n",
    "\n",
    "\n",
    "`ds.map()` applies the transformation function to each record in parallel across the cluster. Whenever possible, Ray avoids transferring objects across network connections to take advantage of zero-copy reads, avoiding serialization and deserialization overhead.\n",
    "\n",
    "As soon as blocks of data finish preprocessing, they can move to the next stage without waiting for the entire dataset."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "def resample(item):\n",
    "    # Resample at 16kHz, which is what openai/whisper-large-v3-turbo was trained on.\n",
    "    audio_bytes = item.pop(\"audio_bytes\")\n",
    "    new_sampling_rate = 16000\n",
    "    waveform, sampling_rate = torchaudio.load(io.BytesIO(audio_bytes), format=\"flac\")\n",
    "    waveform = T.Resample(sampling_rate, new_sampling_rate)(waveform).squeeze()\n",
    "    item[\"arr\"] = np.array(waveform)\n",
    "    item[\"sampling_rate\"] = new_sampling_rate\n",
    "    return item\n",
    "\n",
    "\n",
    "ds = raw_ds.map(resample)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "Next, preprocess the data using Whisper's preprocessor. This step runs as a separate stage to scale it independently from the Whisper model itself.\n",
    "\n",
    "`map_batches()` transforms entire batches of records at a time rather than individual items. By passing a class to `map_batches()`, Ray creates an Actor process that recycles state between batches. The `concurrency` parameter controls how many parallel workers process batches. The `batch_format=\"pandas\"` setting converts batches to pandas DataFrames before processing."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "class WhisperPreprocessor:\n",
    "    def __init__(self):\n",
    "        self.processor = AutoProcessor.from_pretrained(TRANSCRIPTION_MODEL)\n",
    "\n",
    "    def __call__(self, batch):\n",
    "        # The Whisper processor expects a list of 1D NumPy arrays (mono audio).\n",
    "\n",
    "        # Extract log-mel spectogram of audio.\n",
    "        extracted_features = self.processor(\n",
    "            batch[\"arr\"].tolist(),\n",
    "            sampling_rate=batch[\"sampling_rate\"][0],  # Expects int, not list.\n",
    "            return_tensors=\"np\",\n",
    "            device=\"cpu\",  # Eligible for GPU, but reserving GPUs for the models.\n",
    "        ).input_features\n",
    "        # extracted_featues is a pd.Series of np.array shape (3000,).\n",
    "        return {\"input_features\": [arr for arr in extracted_features], \"id\": batch[\"id\"]}\n",
    "\n",
    "\n",
    "ds = ds.map_batches(WhisperPreprocessor, batch_size=2, batch_format=\"pandas\", concurrency=1)\n",
    "# ds.show(1)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "Running `ds.show(1)` displays an output similar to:\n",
    "\n",
    "`[{'id': '19ba96...', 'transcription': ' It is from Westport above the villages of Morrisk and La Canvae.'}]`\n",
    "\n",
    "Note that `ds.show(1)` would trigger execution of the pipeline up to this point due to Ray Data's lazy execution model. This approach means that the ingestion and preprocessing would actually run to produce the first result."
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## GPU inference with Whisper\n",
    "\n",
    "Ray Data can schedule user-defined Python callables on the appropriate resources.\n",
    "The `Transcriber` below lazily loads `openai/whisper-large-v3-turbo` onto each GPU\n",
    "worker the **first** time it runs, and subsequent batches reuse the warm model.\n",
    "\n",
    "The `num_gpus=1` parameter tells Ray to run each replica on a node with a GPU. Ray Data automatically handles variable-sized batches based on available resources. If a worker fails, Ray automatically restarts the task on another node."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "class Transcriber:\n",
    "    def __init__(self):\n",
    "        self.device = \"cuda\"\n",
    "        self.dtype = torch.float16\n",
    "        self.model_id = \"openai/whisper-tiny\"\n",
    "        self.model = AutoModelForSpeechSeq2Seq.from_pretrained(self.model_id, torch_dtype=self.dtype, low_cpu_mem_usage=True, use_safetensors=True)\n",
    "        self.model.to(self.device)\n",
    "\n",
    "    def __call__(self, batch):\n",
    "        # Fuse all list of np.array into a single np.array for faster tensor creation.\n",
    "        spectograms = np.array(batch[\"input_features\"])\n",
    "        spectograms = torch.tensor(spectograms).to(self.device, dtype=self.dtype)\n",
    "\n",
    "        with torch.no_grad():\n",
    "            # Generate token IDs for the batched input features.\n",
    "            token_ids = self.model.generate(spectograms)\n",
    "\n",
    "        return {\"id\": batch[\"id\"], \"token_ids\": token_ids.cpu().numpy()}\n",
    "\n",
    "\n",
    "# Transcribe audio to text tokens using Whisper.\n",
    "# Use 2 workers using 1 GPU each.\n",
    "ds = ds.map_batches(Transcriber, batch_size=2, batch_format=\"numpy\", concurrency=2, num_gpus=1)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "Now decode the tokens into actual transcriptions. This step decouples from the previous step to prevent GPU blocks on CPU work and avoid idle time. This approach also allows independent scaling of the number of decoders from the number of Whisper replicas.\n",
    "\n",
    "Separating the GPU work from CPU work eliminates GPU idle. The `concurrency=5` and `batch_size=32` parameters show how to use more CPU workers and bigger batch sizes than GPU workers."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "class Decoder:\n",
    "    def __init__(self):\n",
    "        self.processor = AutoProcessor.from_pretrained(TRANSCRIPTION_MODEL)\n",
    "\n",
    "    def __call__(self, batch):\n",
    "        token_ids = batch.pop(\"token_ids\")\n",
    "        transcription = self.processor.batch_decode(token_ids, skip_special_tokens=True)\n",
    "        batch[\"transcription\"] = transcription\n",
    "        return batch\n",
    "\n",
    "\n",
    "ds = ds.map_batches(Decoder, batch_size=16, concurrency=5, batch_format=\"pandas\")  # CPU only\n",
    "\n",
    "# ds.take(1)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## LLM-based quality filter\n",
    "\n",
    "A Llama-3 model serves as a *machine judge* that scores each transcription\n",
    "from 1 👎 to 5 👍 on its educational value. The **LLM Processor** API wraps the heavy\n",
    "lifting of batching, prompt formatting, and vLLM engine interaction using a declarative API style.\n",
    "\n",
    "Ray Data provides a high-level API for integrating LLMs into data pipelines. The preprocessing and postprocessing functions handle data preparation and result parsing."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# LLM as judge.\n",
    "judge_config = vLLMEngineProcessorConfig(\n",
    "    model_source=JUDGE_MODEL,\n",
    "    engine_kwargs={\n",
    "        \"enable_chunked_prefill\": True,\n",
    "        \"max_num_batched_tokens\": 1028,\n",
    "        \"max_model_len\": 4096,\n",
    "        \"guided_decoding_backend\": \"xgrammar\",\n",
    "        \"dtype\": torch.float16,\n",
    "    },\n",
    "    concurrency=3,\n",
    "    batch_size=2,\n",
    ")\n",
    "\n",
    "processor = build_llm_processor(\n",
    "    judge_config,\n",
    "    preprocess=lambda row: dict(\n",
    "        messages=[\n",
    "            {\n",
    "                \"role\": \"system\",\n",
    "                \"content\": \"You are an assistant that rates educational quality of a given text from a scale of 1 (not educational) to 5 (very educational). Only output your score, nothing else (no explanation, no comments, no other text). Acceptable outputs: 1, 2, 3, 4, 5\",\n",
    "            },\n",
    "            {\"role\": \"user\", \"content\": row[\"transcription\"]},\n",
    "        ],\n",
    "        sampling_params=dict(\n",
    "            guided_decoding={\"choice\": [\"1\", \"2\", \"3\", \"4\", \"5\"]},\n",
    "        ),\n",
    "    ),\n",
    "    postprocess=lambda row: dict(\n",
    "        score=float(row.pop(\"generated_text\")),  # Rename generated_text to score and convert to float.\n",
    "        **row,  # Pass through the original fields.\n",
    "    ),\n",
    ")\n",
    "\n",
    "# Rate educational quality of each transcription.\n",
    "ds = processor(ds)\n",
    "\n",
    "# Filter out uneducational content.\n",
    "ds = ds.filter(expr=\"score >= 3\")\n",
    "# print(ds.take(1))"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Persist the curated subset\n",
    "\n",
    "At this point a small, high-quality slice of Common Voice is ready for\n",
    "downstream tasks such as fine-tuning or evaluation.  \n",
    "The `write_parquet` call finally triggers execution of the whole pipeline and writes the blocks out in parallel\n",
    "to the target storage location. This function writes outputs as they become available and automatically\n",
    "shards the results into multiple files for efficient parallel reads in downstream steps.\n",
    "\n",
    "As this is a distributed workload, the destination storage needs to be writable from all workers. This storage can be S3, NFS, or another network-attached solution. Anyscale simplifies this process by automatically creating and mounting [shared storage options](https://docs.anyscale.com/configuration/storage/#storage-shared-across-nodes) on every cluster."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Save the filtered dataset to a Parquet file.\n",
    "# This line triggers the lazy execution of the entire pipeline.\n",
    "output_dir = \"/mnt/cluster_storage/filtered_dataset/\"\n",
    "os.makedirs(output_dir, exist_ok=True)\n",
    "ds.write_parquet(output_dir)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "`write_parquet` triggers the full execution of the data pipeline and streams the results into a series of local Parquet files. Ray automatically shards these files across multiple outputs for efficient downstream reading:"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# List the files in the output directory.\n",
    "print(os.listdir(output_dir))"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": ".venv",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.12.0"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 2
}
