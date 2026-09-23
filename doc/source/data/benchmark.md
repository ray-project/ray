---
myst:
  html_meta:
    description: "Ray Data performance benchmarks across image, document, audio, and video workloads, with methodology and comparisons to Daft."
---

# Ray Data benchmarks

This page documents benchmark results and the methodology for evaluating Ray Data performance across several data modalities and workloads.

---

## Workload summary

These benchmarks compare Ray Data 2.50 with Daft 0.6.2, an open source multimodal data processing library built on Ray. They cover the following five workloads:

- **Image classification**: Processing 800k ImageNet images using ResNet18. The pipeline downloads images, deserializes them, applies transformations, runs ResNet18 inference on GPU, and outputs predicted labels.
- **Document embedding**: Processing 10k PDF documents from Digital Corpora. The pipeline reads PDF documents, extracts text page by page, splits the text into overlapping chunks, embeds the chunks with an `all-MiniLM-L6-v2` model on GPU, and outputs embeddings with metadata.
- **Audio transcription**: Transcribing 113,800 audio files from the Mozilla Common Voice 17 dataset with a Whisper-tiny model. The pipeline loads FLAC audio files, resamples to 16kHz, extracts features with Whisper's processor, runs GPU-accelerated batch inference with the model, and outputs transcriptions with metadata.
- **Video object detection**: Processing 10k video frames from the Hollywood2 action videos dataset with YOLOv11n for object detection. The pipeline loads video frames, resizes them to 640x640, runs batch inference with YOLO to detect objects, extracts individual object crops, and outputs object metadata and cropped images in Parquet format.
- **Large-scale image embedding**: Processing 4TiB of base64-encoded images from a Parquet dataset with ViT for image embedding. The pipeline decodes base64 images, converts them to RGB, resizes and normalizes them with ViTImageProcessor, runs GPU-accelerated batch inference with ViT to generate embeddings, and outputs results to Parquet format.

:::{note}
These results are a point-in-time snapshot of Ray Data 2.50. The benchmark code linked on this page points at the [`ray-2.50.0`](https://github.com/ray-project/ray/tree/ray-2.50.0) tag, the version used to collect these numbers.
:::

---

## Results summary

![Bar chart of run time in seconds for Ray Data 2.50 and Daft 0.6.2 on each of the five workloads, where lower is better](/data/images/multimodal_inference_results.png)

The following table lists the result for each workload in seconds:

```{list-table}
:header-rows: 1
:stub-columns: 1
:name: benchmark-results-summary
-   - Workload
    - Daft (s)
    - Ray Data (s)
-   - Image classification
    - 195.3 ± 2.5
    - **111.2 ± 1.2**
-   - Document embedding
    - 51.3 ± 1.3
    - **29.4 ± 0.8**
-   - Audio transcription
    - 510.5 ± 10.4
    - **312.6 ± 3.1**
-   - Video object detection
    - 735.3 ± 7.6
    - **623 ± 1.4**
-   - Large-scale image embedding
    - 752.75 ± 5.5
    - **105.81 ± 0.79**
```

Each result is the mean and standard deviation across four runs. A warmup run first downloads the model and removes startup overhead that would otherwise affect the result.

## Workload configuration

The following table lists the dataset, data path, cluster configuration, and benchmark code for each workload:

```{list-table}
:header-rows: 1
:stub-columns: 1
:name: workload-configuration
-   - Workload
    - Dataset
    - Data path
    - Cluster configuration
    - Code
-   - Image classification
    - 800k images from ImageNet
    - `s3://ray-example-data/imagenet/metadata_file.parquet`
    - 1 head, 8 workers of varying instance types
    - [Benchmark code](https://github.com/ray-project/ray/tree/ray-2.50.0/release/nightly_tests/multimodal_inference_benchmarks/image_classification)
-   - Document embedding
    - 10k PDFs from Digital Corpora
    - `s3://ray-example-data/digitalcorpora/metadata`
    - g6.xlarge head, 8 g6.xlarge workers
    - [Benchmark code](https://github.com/ray-project/ray/tree/ray-2.50.0/release/nightly_tests/multimodal_inference_benchmarks/document_embedding)
-   - Audio transcription
    - 113,800 audio files from Mozilla Common Voice 17 en dataset
    - `s3://air-example-data/common_voice_17/parquet/`
    - g6.xlarge head, 8 g6.xlarge workers
    - [Benchmark code](https://github.com/ray-project/ray/tree/ray-2.50.0/release/nightly_tests/multimodal_inference_benchmarks/audio_transcription)
-   - Video object detection
    - 1,000 videos from Hollywood-2 Human Actions dataset
    - `s3://ray-example-data/videos/Hollywood2-actions-videos/Hollywood2/AVIClips/`
    - 1 head, 8 workers of varying instance types
    - [Benchmark code](https://github.com/ray-project/ray/tree/ray-2.50.0/release/nightly_tests/multimodal_inference_benchmarks/video_object_detection)
-   - Large-scale image embedding
    - 4 TiB of Parquet files containing base64-encoded images
    - `s3://ray-example-data/image-datasets/10TiB-b64encoded-images-in-parquet-v3/`
    - m5.24xlarge head, 40 g6e.xlarge GPU workers, 64 r6i.8xlarge CPU workers
    - [Benchmark code](https://github.com/ray-project/ray/tree/ray-2.50.0/release/nightly_tests/multimodal_inference_benchmarks/large_image_embedding)
```

## Image classification across different instance types

This experiment compares Ray Data with Daft on the image classification workload across several instance types. Each result is the mean and standard deviation across three runs. A warmup run first downloads the model and removes startup overhead that would otherwise affect the result.

```{list-table}
:header-rows: 1
:stub-columns: 1
:name: image-classification-results
-   -
    - g6.xlarge (4 CPUs)
    - g6.2xlarge (8 CPUs)
    - g6.4xlarge (16 CPUs)
    - g6.8xlarge (32 CPUs)
-   - Ray Data (s)
    - 456.2 ± 39.9
    - **195.5 ± 7.6**
    - **144.8 ± 1.9**
    - **111.2 ± 1.2**
-   - Daft (s)
    - **315.0 ± 31.2**
    - 202.0 ± 2.2
    - 195.0 ± 6.6
    - 195.3 ± 2.5
```

## Video object detection across different instance types

This experiment compares Ray Data with Daft on the video object detection workload across several instance types. Each result is the mean and standard deviation across four runs. A warmup run first downloads the model and removes startup overhead that would otherwise affect the result.

```{list-table}
:header-rows: 1
:stub-columns: 1
:name: video-object-detection-results
-   -
    - g6.xlarge (4 CPUs)
    - g6.2xlarge (8 CPUs)
    - g6.4xlarge (16 CPUs)
    - g6.8xlarge (32 CPUs)
-   - Ray Data (s)
    - 922 ± 13.8
    - **704.8 ± 25.0**
    - **629 ± 1.8**
    - **623 ± 1.4**
-   - Daft (s)
    - **758.8 ± 10.4**
    - 735.3 ± 7.6
    - 747.5 ± 13.4
    - 771.3 ± 25.6
```
