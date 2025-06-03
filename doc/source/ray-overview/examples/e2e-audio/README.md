# Audio data curation with Ray

<div align="left">
<a target="_blank" href="https://console.anyscale.com/"><img src="https://img.shields.io/badge/🚀 Run_on-Anyscale-9hf"></a>&nbsp;
<a href="https://github.com/anyscale/e2e-audio" role="button"><img src="https://img.shields.io/static/v1?label=&amp;message=View%20On%20GitHub&amp;color=586069&amp;logo=github&amp;labelColor=2f363d"></a>&nbsp;
</div>

This example demonstrates how to build a scalable, end-to-end audio curation pipeline with Ray with the following steps:

1. Stream the English validation split of [Common Voice 11.0](https://huggingface.co/datasets/mozilla-foundation/common_voice_11_0) into a Ray Dataset.
2. Resample each clip to 16 kHz for compatibility with Whisper.
3. Transcribe the audio with the `openai/whisper-large-v3-turbo` model.
4. Judge the educational quality of each transcription with a small Llama-3 model.
5. Persist only clips that score ≥ 3 to a Parquet dataset.

Because this example expresses every as a Ray transformation the same script scales seamlessly from a laptop to a multi-node GPU cluster.

## Quickstart

```bash
# Install dependencies.
pip install -r requirements.txt
```

