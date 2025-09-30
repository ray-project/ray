# Ray Data Benchmarks

This page documents benchmark results and methodologies for evaluating Ray Data performance across a variety of data modalities and workloads.

---

## 1. Image Classification

### Data and Cluster Environment

- **Dataset:** 800k images from ImageNet
- **Data Path:** `s3://ray-example-data/imagenet/metadata_file`
- **Cluster:** 1 head / 8 workers of varying instance types
- **Code**: ...

### Evaluation

The numbers below are taken from an average/std across 4 runs. We also ran 1 warmup run to download the model and remove any startup overheads that would affect the result.

```{list-table} Image Classification Benchmark Results
:header-rows: 1
:name: image-classification-results
-   -
    - g6.xlarge (4 CPUs)
    - g6.2xlarge (8 CPUs)
    - g6.4xlarge (16 CPUs)
    - g6.8xlarge (32 CPUs)
-   **Ray Data (2.50)**
    - 456.23 +/- 39.92
    - 195.52 +/- 7.59
    - 144.83 +/- 1.92
    - 111.24 +/- 1.15
-   **Daft (0.6.2)**
    - 314.97 +/- 31.16
    - 201.99 +/- 2.19
    - 195.00 +/- 6.64
    - 195.27 +/- 2.53
```


---

## 2. Document Embedding

### Dataset and Methodology

- **Dataset:** 10k PDFs from Digital Corpora
- **Metadata Path:** `s3:/ray-example-data/pdf_dump/metadata`
- **Cluster:** g6.xlarge head / 8 g6.xlarge workers


### Benchmark Results

The numbers below are taken from an average/std across 4 runs. We also ran 1 warmup run to download the model and remove any startup overheads that would affect the result.


```{list-table} Document Embedding Benchmark Results
:header-rows: 1
:name: document-embedding-results
-   -
    - Results (seconds)
-   **Daft (0.6.2)**
    - 51.30 +/- 1.34
-   **Ray Data (2.50.0)**
    - 31.45 +/- 4.54
```

---

## 3. Audio Transcription

### Dataset and Methodology

- **Dataset:** Mozilla Common Voice 17 en (113,800 files)
- **Data Path:** `s3://air-example-data/common_voice_17/parquet/`
- **Cluster:** g6.xlarge head / 8 g6.xlarge workers

### Benchmark Results

```{list-table} Audio Transcription Benchmark Results
:header-rows: 1
:name: audio-transcription-results
-   -
    - Results (seconds)
-   **Daft (0.6.2)**
    - 510.5 +/- 10.38
-   **Ray Data (2.50)**
    - 306.47 +/- 0.74
```



---

## 4. Video Object Detection

This workload

### Dataset and Compute

- **Dataset:**
- **Compute Config:** 1 head / 8 workers of varying instance types

### Benchmark Results

The numbers below are taken from an average/std across 4 runs. We also ran 1 warmup run to download the model and remove any startup overheads that would affect the result.


```{list-table} Video Object Detection Results
:header-rows: 1
:label: example-table
-   |
    | g6.xlarge (4 CPUs)
    | g6.2xlarge (8 CPUs)
    | g6.4xlarge (16 CPUs)
    | g6.8xlarge (32 CPUs)
-   **Ray Data (2.50.0)**
    | 1590.5 +/- 116.67
    | 908.75 +/- 13.7
    | 663.5 +/- 23.67
    | 644.5 +/- 24.34
-   **Daft (0.6.2)**
    | 758.75 +/- 10.37
    | 735.25 +/- 7.59
    | 747.5 +/- 13.43
    | 771.25 +/- 25.63
```

---
## Large-scale image embedding



The numbers below are taken from an average/std across 4 runs. We also ran 1 warmup run to download the model and remove any startup overheads that would affect the result.

```{list-table} Large-scale Image Embedding Results
:header-rows: 1
:name: large-scale-image-embedding-results
-   -
    - Results (seconds)
-   **Ray Data (2.50)**
    - 105.81 +/- 0.79
-   **Daft (0.6.2)**
    - 752.75 +/- 5.52
```
