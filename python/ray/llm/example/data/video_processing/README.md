# Video Processing Example

This folder contains a self-contained example that demonstrates how Ray’s LLM batch
pipeline prepares video inputs before they are passed to a multimodal model. The
implementation lives in `video_processor.py`; it is designed to be production ready
while remaining easy to adapt for experimentation.

## What the module does

`VideoProcessor` and its companion classes form a stage that:

1. Parses OpenAI-style chat messages (looking for `video` or `video_url` items).
2. Resolves each source URI – supporting HTTP(S), local paths, and data URIs – with
	an optional disk/memory cache and atomic writes for robustness.
3. Uses PyAV/FFmpeg to decode frames and applies deterministic sampling policies:
	* timeline sampling at a user-provided FPS, or
	* fixed `num_frames` sampling from the beginning of the clip.
4. Optionally resizes/crops/converts frames via Pillow before returning either PIL
	images or NumPy arrays (channels-first or channels-last).
5. Emits structured metadata (dimensions, timestamps, source, failure details) that
	downstream stages can use for logging, retries, or model prompts.

The stage is implemented as:

- `VideoProcessor`: synchronous decode/sampling engine wrapped with async orchestration,
- `PrepareVideoUDF`: Ray batch stage UDF that extracts sources per row and invokes the
  processor concurrently, and
- `PrepareVideoStage`: declarative wrapper that plugs into Ray’s LLM batch pipeline.

## Configuration reference

The processor is highly configurable. Every argument defaults to a sensible value so
you can opt in only to the knobs you need.

### Core options

| Argument | Default | Description |
|----------|---------|-------------|
| `sampling` | `{"fps": 3.0}` | Frame selection policy. Accepts `{"fps": float}` for timeline sampling or `{"num_frames": int}` to take the first _n_ decoded frames. Invalid values (<0) raise `ValueError`. |
| `cache_dir` | `None` | Filesystem directory used when on-disk caching is enabled. Created on demand; intermediate downloads use atomic renames. |
| `cache_mode` | `"auto"` | How remote sources are cached. `"auto"` streams unless `num_frames` sampling benefits from random access, `"disk"` always writes to `cache_dir`, `"memory"` fetches into a `BytesIO`. |
| `output_format` | `"pil"` | Output type for each frame. `"pil"` yields `PIL.Image.Image` instances; `"numpy"` yields `numpy.ndarray` tensors (`RGB` by default). |
| `channels_first` | `False` | When `output_format="numpy"`, return arrays as `(C, H, W)` if `True`; otherwise `(H, W, C)`. Ignored for PIL output. |
| `timeout_s` | `30.0` | HTTP timeout in seconds for downloading remote videos. |
| `max_concurrency` | `8` | Async semaphore that bounds concurrent decode jobs per processor instance. |
| `retries` | `2` | Maximum number of retry attempts for retriable exceptions (network/decoder). Non-retriable errors (`ImportError`, `ValueError`) surface immediately. |
| `retry_backoff_base` | `0.5` | Initial backoff in seconds; doubles after each retry attempt. |
| `bypass_if_frames_present` | `False` | Reserved stub for future pipelines that reuse pre-decoded frames; currently unused but wired for compatibility. |
| `pack_for_model` | `False` | Reserved stub for future model-specific packing. Present to mirror other multimodal stages. |
| `keep_downloaded` | `False` | Retain files placed in `cache_dir` instead of deleting them after processing. Ignored when not writing to disk. |
| `preprocess` | `{}` | Optional lightweight PIL transforms applied to each sampled frame. See [Preprocess schema](#preprocess-schema). |
| `max_sampled_frames` | `None` | Upper bound on frames returned per video. Applies after sampling policy: useful as a guard rail when upstream metadata is unreliable. |

These options are surfaced through both `VideoProcessor` and the batch stage. When
constructing `PrepareVideoStage`, pass them as keyword arguments to configure the
underlying `PrepareVideoUDF`. Options unique to `VideoProcessor` (`keep_downloaded`
and `preprocess`) are available when you instantiate the processor directly.

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

## Typical usage patterns

1. **Streaming / real-time ingestion** – wrap the processor inside a Ray actor or
	Ray Serve deployment that consumes URLs from Kafka (or any message bus), decodes
	frames immediately, batches them, and forwards them to a VLM for inference.
2. **Micro-batching with `map_batches`** – when slight latency is acceptable, bundle
	multiple rows into batches and let Ray Data distribute work across workers while
	`VideoProcessor` handles per-row concurrency.
3. **Offline preprocessing** – adapt the code to read from a manifest of video files,
	export frames + metadata to disk, and feed a later training or evaluation job.

## Dependencies

The module expects the following optional dependencies at runtime:

- `av` (PyAV) with FFmpeg installed
- `Pillow`
- `numpy` (only when `output_format="numpy"`)

Import failures are surfaced as structured errors in the metadata so the pipeline can
skip or retry inputs gracefully.
