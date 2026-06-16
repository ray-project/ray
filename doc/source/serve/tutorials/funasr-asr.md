---
orphan: true
---

(serve-funasr-asr-tutorial)=

# Serve a FunASR Speech Recognition Model

This tutorial shows how to deploy an automatic speech recognition (ASR) service
with [FunASR](https://github.com/modelscope/FunASR) and Ray Serve. The example
exposes an OpenAI-compatible `POST /v1/audio/transcriptions` endpoint and scales
the FunASR deployment independently from the HTTP ingress.

## Install Dependencies

Install Ray Serve and the packages used by this example:

```bash
pip install "ray[serve]" funasr modelscope python-multipart openai
```

FunASR models can run on CPU, but GPU replicas are recommended for production
traffic. The example requests one GPU per ASR replica. If you want to try the
example on a CPU-only machine, remove `ray_actor_options={"num_gpus": 1}` from
the `FunASRModel` deployment in the code.

## Define the Serve Application

Save the following code to a file named `funasr_asr.py`:

```{literalinclude} ../doc_code/funasr_asr.py
:language: python
:start-after: __example_code_start__
:end-before: __example_code_end__
```

The application has two deployments:

1. `OpenAICompatibleIngress` accepts multipart form uploads at `/v1/audio/transcriptions`.
2. `FunASRModel` loads FunASR models and runs batched ASR inference. Serve can
   autoscale this deployment separately from the ingress deployment.

The ingress deployment is lightweight, so the example keeps one to two ingress
replicas and allows each replica to queue more concurrent requests. The ASR
deployment owns the GPU and is the bottleneck, so it uses more conservative
concurrency: `max_ongoing_requests=8`, `target_ongoing_requests=4`, and
`@serve.batch(max_batch_size=4, batch_wait_timeout_s=0.1)`. This keeps
`max_ongoing_requests` high enough for a full batch while avoiding too many
simultaneous GPU-bound requests per replica. Tune these values with the
benchmark below for your model, GPU, and audio duration distribution.

The default model is `iic/SenseVoiceSmall`. The example restricts requests to a
preconfigured model allowlist to avoid loading arbitrary models. Requests pass
the selected model as a Serve multiplexed model ID, and `FunASRModel` uses
`@serve.multiplexed(max_num_models_per_replica=1)` so Serve handles model
affinity and LRU eviction instead of using an unbounded per-replica cache. To
serve additional FunASR models, add their model names to `ALLOWED_MODELS` and
pass one of those names in the multipart `model` field.

## Run the Service

Start the Serve application:

```bash
serve run funasr_asr:entrypoint
```

The first request downloads the configured FunASR model, so it may take longer
than later requests.

## Query the Endpoint

Send an audio file with the OpenAI Python client:

```python
from openai import OpenAI

client = OpenAI(base_url="http://127.0.0.1:8000/v1", api_key="not-needed")

with open("audio.wav", "rb") as audio_file:
    transcription = client.audio.transcriptions.create(
        model="iic/SenseVoiceSmall",
        file=audio_file,
        response_format="json",
    )

print(transcription.text)
```

You can also use `curl`:

```bash
curl http://127.0.0.1:8000/v1/audio/transcriptions \
    -F "model=iic/SenseVoiceSmall" \
    -F "file=@audio.wav" \
    -F "response_format=json"
```

Set `response_format=text` to return plain transcript text, or
`response_format=verbose_json` to include FunASR segment metadata when the
selected model returns it.

## Benchmark the Deployment

ASR throughput depends heavily on the model, GPU, audio duration, sample rate,
and batching settings. Before using this example for production traffic, measure
latency and requests per second with representative audio files. For example,
save this script as `benchmark_funasr_asr.py` and pass audio clips of different
durations:

```python
import argparse
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from openai import OpenAI


def transcribe(client, path, model):
    start = time.perf_counter()
    with open(path, "rb") as audio_file:
        client.audio.transcriptions.create(
            model=model,
            file=audio_file,
            response_format="json",
        )
    return time.perf_counter() - start


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("audio", nargs="+")
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--requests-per-file", type=int, default=10)
    parser.add_argument("--model", default="iic/SenseVoiceSmall")
    args = parser.parse_args()

    client = OpenAI(base_url="http://127.0.0.1:8000/v1", api_key="not-needed")

    for path in args.audio:
        work = [path] * args.requests_per_file
        start = time.perf_counter()
        with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            futures = [pool.submit(transcribe, client, item, args.model) for item in work]
            latencies = [future.result() for future in as_completed(futures)]
        elapsed = time.perf_counter() - start
        p50 = statistics.median(latencies)
        p95 = statistics.quantiles(latencies, n=20)[18]
        rps = len(latencies) / elapsed
        print(f"{path}: rps={rps:.2f}, p50={p50:.2f}s, p95={p95:.2f}s")


if __name__ == "__main__":
    main()
```

Run it against short, medium, and long clips:

```bash
python benchmark_funasr_asr.py short.wav medium.wav long.wav --concurrency 8
```

If p95 latency is high and GPU utilization is low, increase `max_batch_size` or
`batch_wait_timeout_s`. If p95 latency rises while GPU memory is saturated,
reduce `max_batch_size`, `max_ongoing_requests`, or the ASR deployment's
`target_ongoing_requests`.
