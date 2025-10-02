# Ray Data Benchmarks

This page documents benchmark results and methodologies for evaluating Ray Data performance across a variety of data modalities and workloads.

---

## Workload Summary

- **Image Classification**: Processing 800k ImageNet images using ResNet18 for inference, with image loading, preprocessing, and GPU-based batch inference. The pipeline downloads images, deserializes them, applies transformations, runs ResNet18 inference on GPU, and outputs predicted labels.
- **Document Embedding**: Processing 10k PDF documents from Digital Corpora using PyMuPDF for text extraction, chunking, and GPU-accelerated embedding via `all-MiniLM-L6-v2`. The pipeline reads PDFs, extracts text page-by-page, splits into chunks with overlap, embeds using a small model on GPU, and outputs embeddings with metadata.
- **Audio Transcription**: Transcribing 113,800 audio files from Mozilla Common Voice 17 dataset using Whisper-tiny model. The pipeline loads FLAC audio files, resamples to 16kHz, extracts features using Whisper's processor, runs GPU-accelerated batch inference with the model, and outputs transcriptions with metadata.
- **Video Object Detection**: Processing 10k video frames from Hollywood2 action videos dataset using YOLOv11n for object detection. The pipeline loads video frames, resizes them to 640x640, runs batch inference with YOLO to detect objects, extracts individual object crops, and outputs object metadata and cropped images in Parquet format.
- **Large-scale Image Embedding**: Processing 4TiB of base64-encoded images from a Parquet dataset using ViT for image embedding. The pipeline decodes base64 images, converts to RGB, preprocesses using ViTImageProcessor (resizing, normalization), runs GPU-accelerated batch inference with ViT to generate embeddings, and outputs results to Parquet format.

We compare Ray Data with Daft, an open source multimodal data processing library built on Ray.

---

## Results Summary

![Multimodal Inference Benchmark Results](/data/images/multimodal_inference_results.png)

---

## Workload Configuration


```{list-table}
:header-rows: 1
:name: workload-configuration
-   - Workload
    - Dataset
    - Data Path
    - Cluster Configuration
    - Code
-   - **Image Classification**
    - 800k images from ImageNet
    - s3://ray-example-data/imagenet/metadata_file
    - 1 head / 8 workers of varying instance types
    - ...
-   - **Document Embedding**
    - 10k PDFs from Digital Corpora
    - s3://ray-example-data/digitalcorpora/metadata
    - g6.xlarge head, 8 g6.xlarge workers
    - ...
-   - **Audio Transcription**
    - 113,800 audio files from Mozilla Common Voice 17 en dataset
    - s3://air-example-data/common_voice_17/parquet/
    - g6.xlarge head,  8 g6.xlarge workers
    - ...
-   - **Video Object Detection**
    - 1,000 videos from Hollywood-2 Human Actions dataset
    - s3://ray-example-data/videos/Hollywood2-actions-videos/Hollywood2/AVIClips/
    - 1 head, 8 workers of varying instance types
    - ...
-   - **Large-scale Image Embedding**
    - 4 TiB of Parquet files containing base64 encoded images
    - s3://ray-example-data/image-datasets/10TiB-b64encoded-images-in-parquet-v3/
    - m5.24xlarge (head), 40 g6e.xlarge (gpu workers), 64 r6i.8xlarge (cpu workers)
    - ...
```

---

## Methodology

All benchmark results are taken from an average/std across 4 runs. We also ran 1 warmup run to download the model and remove any startup overheads that would affect the result.