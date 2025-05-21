# Audio Data Curation with Ray

<div align="left">
<a target="_blank" href="https://console.anyscale.com/"><img src="https://img.shields.io/badge/🚀 Run_on-Anyscale-9hf"></a>&nbsp;
<a href="https://github.com/anyscale/e2e-audio" role="button"><img src="https://img.shields.io/static/v1?label=&amp;message=View%20On%20GitHub&amp;color=586069&amp;logo=github&amp;labelColor=2f363d"></a>&nbsp;
</div>

## Overview

This example demonstrates how to build a scalable, end-to-end audio curation pipeline with **Ray**.

1. Stream the English validation split of [Common Voice 11.0](https://huggingface.co/datasets/mozilla-foundation/common_voice_11_0) into a Ray Dataset.
2. Resample each clip to 16 kHz for compatibility with Whisper.
3. Transcribe audio with the `openai/whisper-large-v3-turbo` model.
4. Judge the educational quality of each transcription with a small Llama-3 model.
5. Persist only clips that score ≥ 3 to a Parquet dataset.

Because every step is expressed as a Ray transformation the same script scales seamlessly from a laptop to a multi-node GPU cluster.

## Quick start

```bash
# Install dependencies
pip install -q "ray[data]==2.23.0" "torch==2.5.1" "torchaudio==2.2.3" \
              "transformers==4.47.1" "datasets==2.18.0"

# Run the pipeline locally
python e2e_audio/curation.py
```

## Development on Anyscale Workspaces

Developing directly in an [Anyscale Workspace](https://docs.anyscale.com/platform/workspaces/) provides:

* **Remote development**: connect your local IDE (Cursor, VS Code, etc.) to a fully managed Ray cluster.
* **Dependency management**: install Python packages with `pip`; Anyscale propagates them to every node.
* **Compute elasticity**: scale from CPU-only machines to large multi-GPU clusters without changing code.
* **Distributed debugging**: get the same VS Code-like debugging experience across all workers.

Learn more from the [official documentation](https://docs.anyscale.com/platform/workspaces/).

## Production

Promote the same script to production by submitting an [Anyscale Job](https://docs.anyscale.com/platform/jobs/). Since development already happens on a Ray cluster, the dev → prod hand-off is straightforward.

## No infrastructure headaches

Abstract away infrastructure so ML engineers can focus on ML. Anyscale offers governance, cost controls, and observability features—including resource quotas, workload prioritization, and a Telescope dashboard—to keep your compute fleet under control.
