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
2. `FunASRModel` loads FunASR models and runs ASR inference. Serve can autoscale
   this deployment separately from the ingress deployment.

The default model is `iic/SenseVoiceSmall`. The example restricts requests to a
preconfigured model allowlist to avoid loading arbitrary models into memory. To
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
