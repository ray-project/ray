# Video Processing Example

This folder contains a self-contained example that demonstrates how Ray Data can
prepare video inputs before they are passed to a multimodal model. The
implementation lives in `video_processor.py`; it focuses on being a small,
re-usable utility that you can compose inside `map_batches` or call directly from
an async workflow.

Capabilities:
- Decode and sample frames from a video with a single asynchronous call.
- Compose a two-stage Ray Data pipeline (decode → VLM inference).
- Run multimodal generation on GPU using vLLM.

## What the module does

`VideoProcessor` performs three high-level tasks:

1. Resolves each source URI – supporting HTTP(S), local paths, and data URIs – with
   an optional disk/memory cache and atomic writes for robustness.
2. Uses PyAV/FFmpeg to decode frames and applies deterministic sampling policies:
   * timeline sampling at a user-provided FPS, or
   * fixed `num_frames` sampling from the beginning of the clip.
3. Optionally resizes/crops/converts frames via Pillow before returning either PIL
   images or NumPy arrays (channels-first or channels-last) alongside structured
   metadata (dimensions, timestamps, source, failure details).

The processor exposes a single async method:

```python
results: list[dict[str, Any]] = await VideoProcessor(...).process(list_of_sources)
```

Each entry in `results` is a dictionary with two keys:

- `frames`: the sampled frames (list of `PIL.Image.Image` or `numpy.ndarray`)
- `meta`: metadata containing `video_num_frames`, `video_size`, timestamps, and
  error details if a source failed.

Because the processor is just an async helper, you can easily integrate it with
Ray Data or any other orchestration layer. A minimal direct usage example:

```python
import asyncio

from ray.data.examples.data.video_processing.video_processor import VideoProcessor

async def main() -> None:
  sources = [
    "https://storage.googleapis.com/ray-demo-assets/video/ray-demo-video.mp4"
  ]
  processor = VideoProcessor(sampling={"fps": 3})
  results = await processor.process(sources)
  for result in results:
    print(result["meta"], len(result["frames"]))

asyncio.run(main())
```

### Example walkthrough and use cases

`main.py` shows a minimal, reproducible flow without CLI plumbing:

1) Single video summary
   - Decode a handful of frames, materialize them as images, build a chat-style
     prompt, and generate a summary with a multimodal model through vLLM.

2) Dataset pipeline (decode → VLM)
  - Start from a small in-memory dataset with items like `{ "video_url": EXAMPLE_VIDEO_PATH, "prompt": DEFAULT_PROMPT }`.
  - The first stage returns PyArrow batches whose rows contain base64-encoded JPEG frames (list of strings) under `frames_b64` (the `data:image/jpeg;base64,` prefix is added later during preprocess before sending to the model).
  - The second stage runs vLLM on GPU to produce a text summary per item.
  - A minimal postprocess keeps only `video_url` and `generated_text` columns in the final dataset for clarity.

Where it’s useful
- Batch/offline summarization, highlight generation, safety/QA scanning.
- Low-latency multimodal pre-processing before model inference.
- The same stages can be applied to streaming inputs (e.g., Kafka) by swapping
  the source with a streaming dataset.

Dependencies
- `av` (PyAV with FFmpeg)
- `Pillow`
- `transformers`, `vllm`, `qwen-vl-utils`
- `ray[data]`

## Configuration reference

The processor is highly configurable. Every argument defaults to a sensible value
so you can opt in only to the knobs you need.

### Core options

| Argument | Default | Description |
|----------|---------|-------------|
| `sampling` | `{"fps": 3.0}` | Frame selection policy captured by a lightweight `Sampling` Pydantic model. Accepts `{"fps": float}` for timeline sampling or `{"num_frames": int}` to take the first _n_ decoded frames; when both are missing it falls back to `fps=3.0`. |
| `cache_dir` | `None` | Filesystem directory used when on-disk caching is enabled. Created on demand; intermediate downloads use atomic renames. |
| `cache_mode` | `"auto"` | How remote sources are cached. `"auto"` streams unless `num_frames` sampling benefits from random access, `"disk"` always writes to `cache_dir`, `"memory"` fetches into a `BytesIO`. |
| `output_format` | `"pil"` | Output type for each frame. `"pil"` yields `PIL.Image.Image` instances; `"numpy"` yields `numpy.ndarray` tensors (`RGB` by default). |
| `channels_first` | `False` | When `output_format="numpy"`, return arrays as `(C, H, W)` if `True`; otherwise `(H, W, C)`. Ignored for PIL output. |
| `timeout_s` | `30.0` | HTTP timeout in seconds for downloading remote videos. |
| `max_concurrency` | `8` | Async semaphore that bounds concurrent decode jobs per processor instance. |
| `retries` | `2` | Maximum number of retry attempts for retriable exceptions (network/decoder). Non-retriable errors (`ImportError`, `ValueError`) surface immediately. |
| `retry_backoff_base` | `0.5` | Initial backoff in seconds; doubles after each retry attempt. |
| `keep_downloaded` | `False` | Retain files placed in `cache_dir` instead of deleting them after processing. Ignored when not writing to disk. |
| `preprocess` | `{}` | Optional lightweight Pillow transforms applied to each sampled frame. See [Preprocess schema](#preprocess-schema). |
| `max_sampled_frames` | `None` | Upper bound on frames returned per video. Applies after the sampling policy runs: useful as a guard rail when upstream metadata is unreliable. |

Additional flags (currently reserved):

- `bypass_if_frames_present` (bool): Reserved for future use; ignored.
- `pack_for_model` (bool): Reserved for future use; ignored.

### Preprocess schema

The `preprocess` dictionary mirrors a subset of Pillow transformations:

- `{"resize": {"size": (width, height), "resample": "BILINEAR"}}`
  resizes each frame (fallbacks to the numeric value for `BILINEAR` if newer enums
  are missing).
- `{"crop": {"box": (left, upper, right, lower)}}` crops using standard Pillow box
  tuples.
- `{"convert": "RGB"}` converts to another mode before tensor conversion.

Compose them in one dictionary; they apply in the order listed above. When
`output_format="numpy"`, preprocessing occurs in PIL space first, then the frame is
converted to a tensor. Invalid schemas raise `ValueError` from Pillow or NumPy.

## Dependencies

The module expects the following optional dependencies at runtime:

- `av` (PyAV) with FFmpeg installed
- `Pillow`
- `numpy` (only when `output_format="numpy"`)

Import failures are surfaced as structured errors in the metadata so the pipeline
can skip or retry inputs gracefully.

## Return schema

Each processed source produces a dictionary with the following fields:

- `frames`: list of sampled frames
  - `PIL.Image.Image` objects when `output_format="pil"`
  - `numpy.ndarray` when `output_format="numpy"` (RGB, channels-first/last per config)
- `meta`: mapping with structured metadata
  - `video_size`: `[width, height]` when available; otherwise `None`
  - `video_num_frames`: integer count of frames actually returned
  - `frame_timestamps`: list of timestamps (seconds) for each sampled frame
  - `source`: a human-readable representation of the resolved source
  - `failed`: `False` on success; `True` if the processor failed
  - on failure, additional keys are present: `error_type`, `error`, `attempts`, `retried`

## Error handling and retries

The processor wraps each decode in a retry loop (default `retries=2`) with
exponential backoff (`retry_backoff_base`). Retries are applied to network and
decoder errors; configuration errors (`ImportError`, `ValueError`) are surfaced
immediately. Failures are encoded in the `meta` dictionary so downstream
operators can branch, skip, or log without raising exceptions.

## Sampling semantics

`sampling={"fps": k}`
- Constructs a deterministic sequence of target timestamps spaced at 1/k
  seconds. When media duration is unknown, a small bounded sequence is used.

`sampling={"num_frames": n}`
- Decodes frames from the beginning of the clip and returns exactly `n` frames
  (subject to `max_sampled_frames`, if set).

If neither key is provided, the processor defaults to `fps=3.0`.

## Caching behavior

For remote HTTP(S) sources, caching is controlled by `cache_mode`:

- `"auto"`: streams by default; switches to on-disk caching when `num_frames`
  is used (random-access patterns benefit from disk).
- `"disk"`: always downloads to `cache_dir` and decodes from the cached file.
- `"memory"`: fetches the object into a `BytesIO` buffer and decodes from
  memory.

When writing to disk, intermediate downloads use atomic renames. Cached files
are deleted by default after processing unless `keep_downloaded=True`.

## Environment variables

Two environment variables bound safety limits used during sampling and decoding:

- `RAY_VIDEO_EXAMPLE_MAX_TARGETS` (default: `10000`):
  upper bound on the number of timeline targets generated when using `fps`.
- `RAY_VIDEO_EXAMPLE_MAX_DECODE_FRAMES` (default: `100000`):
  maximum number of frames decoded per video to prevent excessive work.


## Integration patterns

- Static datasets: items like `{ "video_url": ..., "prompt": ... }`, then
  `map_batches(decode)` → `map_batches(vlm)`.
- Streaming datasets: swap in a streaming source (e.g., Kafka) and reuse the
  same two stages.

## Performance notes

- For higher throughput, wrap vLLM in a Ray Actor to reuse the engine across
  batches and shutdown explicitly at teardown.
- Tune `batch_size` based on model latency and memory.
- Increase `max_concurrency` carefully to balance decode throughput and CPU use.

## Limitations

- Requires PyAV with FFmpeg available at runtime.
- The example uses a lightweight preprocessing schema; complex vision
  transformations should be applied in model-specific code.
- The driver script is intentionally minimal and assumes all optional
  dependencies are installed.
