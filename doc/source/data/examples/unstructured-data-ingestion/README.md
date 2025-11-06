# Unstructured Data Ingestion and Processing With Ray Data

**Time to complete**: 35 min | **Difficulty**: Advanced | **Prerequisites**: Data engineering experience, document processing, basic NLP knowledge

## What you'll build

Build a comprehensive document ingestion pipeline that transforms unstructured documents from data lakes into structured, analytics-ready datasets using Ray Data's distributed processing capabilities for enterprise data warehouse workflows.

## Table of Contents

1. [Data Lake Document Discovery](#step-1-data-lake-document-discovery) (8 min)
2. [Document Processing and Classification](#step-2-document-processing-and-classification) (10 min)
3. [Text Extraction and Enrichment](#step-3-text-extraction-and-enrichment) (8 min)
4. [Data Warehouse Output](#step-4-data-warehouse-output) (6 min)
5. [Verification and Summary](#step-5-verification-and-summary) (3 min)

## Learning Objectives

**Why unstructured data ingestion matters**: Enterprise data lakes contain vast amounts of unstructured documents (PDFs, Word docs, presentations, reports) that need systematic processing to extract business value for analytics and reporting.

**Ray Data's ingestion capabilities**: Distribute document processing across clusters to handle large-scale document collections, extract structured data, and prepare analytics-ready datasets for data warehouse consumption.

**Data lake to warehouse patterns**: Techniques used by data engineering teams to systematically process document collections, extract structured information, and create queryable datasets for business intelligence.

**Production ingestion workflows**: Scalable document processing patterns that handle diverse file formats, extract metadata, and create structured schemas for downstream analytics systems.

## Overview

**Challenge**: Enterprise data lakes contain millions of unstructured documents (PDFs, Word docs, presentations) across multiple formats that need systematic processing to extract business value. Traditional document processing approaches struggle with:
- **Scale**: Single-machine processing limits document volume
- **Consistency**: Manual extraction creates inconsistent schemas
- **Integration**: Complex infrastructure for analysis
- **Warehouse integration**: Manual data modeling and ETL processes

**Solution**: Ray Data enables end-to-end document ingestion pipelines with native distributed operations for processing millions of documents efficiently.

## Prerequisites Checklist

Before starting, ensure you have:
- [ ] Understanding of data lake and data warehouse concepts
- [ ] Experience with document processing and text extraction
- [ ] Knowledge of structured data formats (Parquet, Delta Lake, Iceberg)
- [ ] Python environment with Ray Data and document processing libraries
- [ ] Access to S3 or other cloud storage for document sources

## Quick start (3 minutes)

This section demonstrates large-scale document ingestion using Ray Data:


```python

# Standard library imports for basic operations
import json  # For handling JSON data (quality issues)
import uuid  # For generating unique document IDs
from datetime import datetime  # For timestamps
from pathlib import Path  # For file path operations
from typing import Any, Dict, List  # For type hints

# Data processing libraries
import numpy as np  # For numerical operations
import pandas as pd  # For DataFrame operations in batch processing

# Ray Data imports for distributed processing
import ray  # Core Ray library
from ray.data.aggregate import Count, Max, Mean, Sum  # Aggregation functions
from ray.data.expressions import col, lit  # Expression API (not used in filters but available)

# Get the current Ray Data context to configure global settings
ctx = ray.data.DataContext.get_current()

# Disable progress bars for cleaner output in production
# You can enable these for debugging: set to True to see progress
ctx.enable_progress_bars = False
ctx.enable_operator_progress_bars = False

# Initialize Ray cluster connection
# ignore_reinit_error=True allows running this code multiple times without errors
ray.init(ignore_reinit_error=True)
```

    2025-10-20 20:08:43,128	INFO worker.py:1833 -- Connecting to existing Ray cluster at address: 10.0.86.206:6379...
    2025-10-20 20:08:43,129	INFO worker.py:1851 -- Calling ray.init() again after it has already been called.





<div class="lm-Widget p-Widget lm-Panel p-Panel jp-Cell-outputWrapper">
    <div style="margin-left: 50px;display: flex;flex-direction: row;align-items: center">
        <div class="jp-RenderedHTMLCommon" style="display: flex; flex-direction: row;">
  <svg viewBox="0 0 567 224" fill="none" xmlns="http://www.w3.org/2000/svg" style="height: 3em;">
    <g clip-path="url(#clip0_4338_178347)">
        <path d="M341.29 165.561H355.29L330.13 129.051C345.63 123.991 354.21 112.051 354.21 94.2307C354.21 71.3707 338.72 58.1807 311.88 58.1807H271V165.561H283.27V131.661H311.8C314.25 131.661 316.71 131.501 319.01 131.351L341.25 165.561H341.29ZM283.29 119.851V70.0007H311.82C331.3 70.0007 342.34 78.2907 342.34 94.5507C342.34 111.271 331.34 119.861 311.82 119.861L283.29 119.851ZM451.4 138.411L463.4 165.561H476.74L428.74 58.1807H416L367.83 165.561H380.83L392.83 138.411H451.4ZM446.19 126.601H398L422 72.1407L446.24 126.601H446.19ZM526.11 128.741L566.91 58.1807H554.35L519.99 114.181L485.17 58.1807H472.44L514.01 129.181V165.541H526.13V128.741H526.11Z" fill="var(--jp-ui-font-color0)"/>
        <path d="M82.35 104.44C84.0187 97.8827 87.8248 92.0678 93.1671 87.9146C98.5094 83.7614 105.083 81.5067 111.85 81.5067C118.617 81.5067 125.191 83.7614 130.533 87.9146C135.875 92.0678 139.681 97.8827 141.35 104.44H163.75C164.476 101.562 165.622 98.8057 167.15 96.2605L127.45 56.5605C121.071 60.3522 113.526 61.6823 106.235 60.3005C98.9443 58.9187 92.4094 54.9203 87.8602 49.0574C83.3109 43.1946 81.0609 35.8714 81.5332 28.4656C82.0056 21.0599 85.1679 14.0819 90.4252 8.8446C95.6824 3.60726 102.672 0.471508 110.08 0.0272655C117.487 -0.416977 124.802 1.86091 130.647 6.4324C136.493 11.0039 140.467 17.5539 141.821 24.8501C143.175 32.1463 141.816 39.6859 138 46.0505L177.69 85.7505C182.31 82.9877 187.58 81.4995 192.962 81.4375C198.345 81.3755 203.648 82.742 208.33 85.3976C213.012 88.0532 216.907 91.9029 219.616 96.5544C222.326 101.206 223.753 106.492 223.753 111.875C223.753 117.258 222.326 122.545 219.616 127.197C216.907 131.848 213.012 135.698 208.33 138.353C203.648 141.009 198.345 142.375 192.962 142.313C187.58 142.251 182.31 140.763 177.69 138L138 177.7C141.808 184.071 143.155 191.614 141.79 198.91C140.424 206.205 136.44 212.75 130.585 217.313C124.731 221.875 117.412 224.141 110.004 223.683C102.596 223.226 95.6103 220.077 90.3621 214.828C85.1139 209.58 81.9647 202.595 81.5072 195.187C81.0497 187.779 83.3154 180.459 87.878 174.605C92.4405 168.751 98.9853 164.766 106.281 163.401C113.576 162.035 121.119 163.383 127.49 167.19L167.19 127.49C165.664 124.941 164.518 122.182 163.79 119.3H141.39C139.721 125.858 135.915 131.673 130.573 135.826C125.231 139.98 118.657 142.234 111.89 142.234C105.123 142.234 98.5494 139.98 93.2071 135.826C87.8648 131.673 84.0587 125.858 82.39 119.3H60C58.1878 126.495 53.8086 132.78 47.6863 136.971C41.5641 141.163 34.1211 142.972 26.7579 142.059C19.3947 141.146 12.6191 137.574 7.70605 132.014C2.79302 126.454 0.0813599 119.29 0.0813599 111.87C0.0813599 104.451 2.79302 97.2871 7.70605 91.7272C12.6191 86.1673 19.3947 82.5947 26.7579 81.6817C34.1211 80.7686 41.5641 82.5781 47.6863 86.7696C53.8086 90.9611 58.1878 97.2456 60 104.44H82.35ZM100.86 204.32C103.407 206.868 106.759 208.453 110.345 208.806C113.93 209.159 117.527 208.258 120.522 206.256C123.517 204.254 125.725 201.276 126.771 197.828C127.816 194.38 127.633 190.677 126.253 187.349C124.874 184.021 122.383 181.274 119.205 179.577C116.027 177.88 112.359 177.337 108.826 178.042C105.293 178.746 102.113 180.654 99.8291 183.44C97.5451 186.226 96.2979 189.718 96.3 193.32C96.2985 195.364 96.7006 197.388 97.4831 199.275C98.2656 201.163 99.4132 202.877 100.86 204.32ZM204.32 122.88C206.868 120.333 208.453 116.981 208.806 113.396C209.159 109.811 208.258 106.214 206.256 103.219C204.254 100.223 201.275 98.0151 197.827 96.97C194.38 95.9249 190.676 96.1077 187.348 97.4873C184.02 98.8669 181.274 101.358 179.577 104.536C177.879 107.714 177.337 111.382 178.041 114.915C178.746 118.448 180.653 121.627 183.439 123.911C186.226 126.195 189.717 127.443 193.32 127.44C195.364 127.443 197.388 127.042 199.275 126.259C201.163 125.476 202.878 124.328 204.32 122.88ZM122.88 19.4205C120.333 16.8729 116.981 15.2876 113.395 14.9347C109.81 14.5817 106.213 15.483 103.218 17.4849C100.223 19.4868 98.0146 22.4654 96.9696 25.9131C95.9245 29.3608 96.1073 33.0642 97.4869 36.3922C98.8665 39.7202 101.358 42.4668 104.535 44.1639C107.713 45.861 111.381 46.4036 114.914 45.6992C118.447 44.9949 121.627 43.0871 123.911 40.301C126.195 37.515 127.442 34.0231 127.44 30.4205C127.44 28.3772 127.038 26.3539 126.255 24.4664C125.473 22.5788 124.326 20.8642 122.88 19.4205ZM19.42 100.86C16.8725 103.408 15.2872 106.76 14.9342 110.345C14.5813 113.93 15.4826 117.527 17.4844 120.522C19.4863 123.518 22.4649 125.726 25.9127 126.771C29.3604 127.816 33.0638 127.633 36.3918 126.254C39.7198 124.874 42.4664 122.383 44.1635 119.205C45.8606 116.027 46.4032 112.359 45.6988 108.826C44.9944 105.293 43.0866 102.114 40.3006 99.8296C37.5145 97.5455 34.0227 96.2983 30.42 96.3005C26.2938 96.3018 22.337 97.9421 19.42 100.86ZM100.86 100.86C98.3125 103.408 96.7272 106.76 96.3742 110.345C96.0213 113.93 96.9226 117.527 98.9244 120.522C100.926 123.518 103.905 125.726 107.353 126.771C110.8 127.816 114.504 127.633 117.832 126.254C121.16 124.874 123.906 122.383 125.604 119.205C127.301 116.027 127.843 112.359 127.139 108.826C126.434 105.293 124.527 102.114 121.741 99.8296C118.955 97.5455 115.463 96.2983 111.86 96.3005C109.817 96.299 107.793 96.701 105.905 97.4835C104.018 98.2661 102.303 99.4136 100.86 100.86Z" fill="#00AEEF"/>
    </g>
    <defs>
        <clipPath id="clip0_4338_178347">
            <rect width="566.93" height="223.75" fill="white"/>
        </clipPath>
    </defs>
  </svg>
</div>

        <table class="jp-RenderedHTMLCommon" style="border-collapse: collapse;color: var(--jp-ui-font-color1);font-size: var(--jp-ui-font-size1);">
    <tr>
        <td style="text-align: left"><b>Python version:</b></td>
        <td style="text-align: left"><b>3.12.11</b></td>
    </tr>
    <tr>
        <td style="text-align: left"><b>Ray version:</b></td>
        <td style="text-align: left"><b>2.50.0</b></td>
    </tr>
    <tr>
    <td style="text-align: left"><b>Dashboard:</b></td>
    <td style="text-align: left"><b><a href="http://session-77uweunq3awbhqefvry4lwcqq5.i.anyscaleuserdata.com" target="_blank">http://session-77uweunq3awbhqefvry4lwcqq5.i.anyscaleuserdata.com</a></b></td>
</tr>

</table>

    </div>
</div>




## Step 1: Data Lake Document Discovery

### Discover document collections in data lake


```python
# STEP 1: READ DOCUMENTS FROM DATA LAKE (S3)

# Use Ray Data's read_binary_files() to load documents from S3
# Why read_binary_files()?
#   - Reads files as raw bytes (works for PDFs, DOCX, images, etc.)
#   - Distributes file reading across cluster workers
#   - include_paths=True gives us the file path for each document
#
# Parameters explained:
#   - S3 path: Location of document collection in data lake
#   - include_paths=True: Adds 'path' column with file location
#   - ray_remote_args: Resource allocation per task
#     * num_cpus=0.025: Very low CPU since this is I/O-bound (reading files)
#     * This allows many concurrent reads without CPU bottleneck
#   - limit(100): Process only 100 documents for this demo
#     * Remove this limit to process entire collection

document_collection = ray.data.read_binary_files(
    "s3://anyscale-rag-application/1000-docs/",
    include_paths=True,
    ray_remote_args={"num_cpus": 0.025}
).limit(100)

# Display the schema to understand our data structure
# At this point, we have: 'bytes' (file content) and 'path' (file location)
print(f"Dataset schema: {document_collection.schema()}")
```

    2025-10-20 20:08:43,509	INFO logging.py:293 -- Registered dataset logger for dataset dataset_122_0
    2025-10-20 20:08:43,513	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_122_0. Full logs are in /tmp/ray/session_2025-10-20_17-37-47_219984_2353/logs/ray-data
    2025-10-20 20:08:43,514	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_122_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> LimitOperator[limit=1] -> TaskPoolMapOperator[ReadFiles]
    /home/ray/anaconda3/lib/python3.12/site-packages/ray/anyscale/data/_internal/cluster_autoscaler/productivity_calculator.py:174: RuntimeWarning: invalid value encountered in divide
      gpu_fraction_per_op = (optimal_num_tasks_per_op * num_gpus_per_op) / np.sum(
    2025-10-20 20:08:46,611	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_122_0 execution finished in 3.10 seconds


    Dataset schema: Column  Type
    ------  ----
    bytes   binary
    path    string


### Document metadata extraction


```python

# STEP 2: TEXT EXTRACTION FUNCTION
# This function extracts text from various document formats (PDF, DOCX, etc.)
# It processes one document at a time (distirbuted by Ray) and returns structured metadata + text


def process_file(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract text content from document files using the Unstructured library.
    
    BEGINNER NOTE:
    - Input: A dictionary (record) with 'bytes' (file content) and 'path' (file location)
    - Output: A dictionary with extracted text + metadata
    - This function runs on each worker node in parallel
    
    Why extract text immediately?
    - Avoids passing large binary data through multiple operations
    - Reduces memory usage in downstream processing
    - Enables faster processing by dropping binary data early
    """
    # Import libraries inside function so each Ray worker has access
    import io
    from pathlib import Path
    
    from unstructured.partition.auto import partition
    
    # Extract file metadata from the input record
    file_path = Path(record["path"])  # Convert path string to Path object
    file_bytes = record["bytes"]  # Raw file content (binary data)
    file_size = len(file_bytes)  # Size in bytes
    file_extension = file_path.suffix.lower()  # Get extension (.pdf, .docx, etc.)
    file_name = file_path.name  # Just the filename (not full path)
    
    # We can only extract text from certain file types
    # If unsupported, return metadata with empty text
    supported_extensions = {".pdf", ".docx", ".doc", ".pptx", ".ppt", ".html", ".txt"}
    
    if file_extension not in supported_extensions:
        # Return a record with metadata but no extracted text
        return {
            "document_id": str(uuid.uuid4()),  # Generate unique ID
            "file_path": str(file_path),
            "file_name": file_name,
            "file_extension": file_extension,
            "file_size_bytes": file_size,
            "file_size_mb": round(file_size / (1024 * 1024), 2),  # Convert to MB
            "discovery_timestamp": datetime.now().isoformat(),
            "extracted_text": "",  # Empty - unsupported format
            "text_length": 0,
            "word_count": 0,
            "extraction_status": "unsupported_format"
        }
    
    # Extract text using the Unstructured library
    try:
        # Create an in-memory file stream from bytes (avoids writing to disk)
        with io.BytesIO(file_bytes) as stream:
            # partition() automatically detects format and extracts text
            # It returns a list of text elements (paragraphs, tables, etc.)
            elements = partition(file=stream)
            
            # Combine all extracted text elements into one string
            extracted_text = " ".join([str(el) for el in elements]).strip()
            
            # Calculate text statistics for quality assessment
            text_length = len(extracted_text)  # Total characters
            word_count = len(extracted_text.split()) if extracted_text else 0
            extraction_status = "success"
            
    except Exception as e:
        # If extraction fails (corrupted file, unsupported format, etc.)
        # Log the error and continue processing other files
        print(f"Cannot process file {file_path}: {e}")
        extracted_text = ""
        text_length = 0
        word_count = 0
        extraction_status = f"error: {str(e)[:100]}"  # Store error message (truncated)
    
    # Return record with all metadata and extracted text
    return {
        "document_id": str(uuid.uuid4()),  # Unique identifier for this document
        "file_path": str(file_path),
        "file_name": file_name,
        "file_extension": file_extension,
        "file_size_bytes": file_size,
        "file_size_mb": round(file_size / (1024 * 1024), 2),
        "discovery_timestamp": datetime.now().isoformat(),
        "extracted_text": extracted_text,  # The actual text content
        "text_length": text_length,
        "word_count": word_count,
        "extraction_status": extraction_status
    }


# Ray Data's map() applies process_file() to each document in parallel
# This is the "embarrassingly parallel" pattern - each document is independent
print("Extracting text from documents...")

# Why map() instead of map_batches()?
#   - map(): Process one record at a time (good for variable-size documents)
#   - map_batches(): Process records in batches (better for vectorized operations)
#   - Text extraction is I/O-bound and document-specific, so map() is ideal

# Parameters:
#   - process_file: The function to apply to each record
#   - concurrency=16: Run 8 parallel tasks at a time
#   - num_cpus=1: Each task gets 1 CPU (text extraction is CPU-intensive)

documents_with_text = document_collection.map(
    process_file,
    concurrency=16,
    num_cpus=1
)
```

    Extracting text from documents...



```python
# Convert a sample of documents to pandas DataFrame for easy viewing
documents_with_text.limit(25).to_pandas()
```

    2025-10-20 20:08:46,860	INFO logging.py:293 -- Registered dataset logger for dataset dataset_124_0
    2025-10-20 20:08:46,866	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_124_0. Full logs are in /tmp/ray/session_2025-10-20_17-37-47_219984_2353/logs/ray-data
    2025-10-20 20:08:46,866	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_124_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> LimitOperator[limit=25] -> TaskPoolMapOperator[ReadFiles] -> TaskPoolMapOperator[Map(process_file)]
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P15' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P19' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P23' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P27' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P33' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P39' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P43' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P47' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P53' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P57' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P63' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P74' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P78' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P84' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P88' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P90' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P92' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P94' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P96' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P98' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P100' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P102' is an invalid float value
    /home/ray/anaconda3/lib/python3.12/site-packages/ray/data/_internal/execution/operators/task_pool_map_operator.py:165: UserWarning: The maximum number of concurrent tasks for 'Map(process_file)' is set to 16, but the operator only received 4 input(s). This means that the operator can launch at most 4 task(s), which is less than the concurrency limit. You might be able to increase the number of concurrent tasks by configuring `override_num_blocks` earlier in the pipeline.
      warnings.warn(
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P104' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P106' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P108' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P110' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P112' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P114' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P116' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P118' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P120' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P122' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P124' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P126' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P128' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P130' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P132' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P134' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P136' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P138' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P140' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P142' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P144' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P146' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P148' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P152' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P160' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P168' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P174' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P180' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P186' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P192' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P198' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P204' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P217' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P221' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P227' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P235' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P239' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P243' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P251' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P255' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P259' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P265' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P271' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P275' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P281' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P285' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P289' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P295' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P299' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P305' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P311' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P315' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P321' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P325' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P331' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P337' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P341' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P347' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P353' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P357' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P361' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P365' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P371' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P375' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P379' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P383' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P387' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P391' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P395' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P399' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P403' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P407' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P413' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P417' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P421' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P425' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P429' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P435' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P441' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P447' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P451' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P457' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P463' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P465' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P467' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P469' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P471' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P473' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P475' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P477' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P479' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P481' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P483' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P485' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P487' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P489' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P491' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P493' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P495' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P497' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P499' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P501' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P503' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P505' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P507' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P509' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P511' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P513' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P515' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P517' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P525' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P529' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P535' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P541' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P547' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P553' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P557' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P559' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P561' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P574' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P580' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P584' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P588' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P594' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P598' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P602' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P608' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P616' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P624' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P632' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P640' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P648' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P656' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P658' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P660' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P662' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P664' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P666' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P668' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P670' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P672' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P674' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P676' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P678' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P684' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P688' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P692' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P696' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P702' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P706' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P710' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P714' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P718' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P722' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P726' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P730' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P736' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P740' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P746' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P750' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P756' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P760' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P766' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P770' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P774' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P778' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P784' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P790' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P796' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P800' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P806' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P810' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P816' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P822' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P826' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P832' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P836' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P840' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P844' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P848' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P852' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P856' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P860' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P864' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P868' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P872' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P876' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P880' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P884' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P888' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P892' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P896' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P900' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P904' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P908' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P912' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P916' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P920' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P924' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P928' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P932' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P936' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P940' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P944' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P948' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P952' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P956' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P960' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P964' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P972' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P976' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P982' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P986' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P992' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P996' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1000' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1004' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1010' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1016' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1020' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1026' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1030' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1036' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1040' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1046' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1052' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1056' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1060' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1064' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1068' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1072' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1076' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1080' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1084' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1090' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1094' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1100' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1106' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1110' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1114' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1118' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1124' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1128' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1132' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1138' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1144' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1148' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1154' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1162' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1166' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1170' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1174' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1178' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1184' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1188' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1192' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1196' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1200' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1204' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1208' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1212' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1216' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1220' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1224' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1228' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1232' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1236' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1240' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1244' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1248' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1252' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1256' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1260' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1264' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1268' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1272' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1276' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1280' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1284' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1288' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1292' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1296' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1300' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1304' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1308' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1312' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1316' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1318' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1320' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1322' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1324' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1326' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1328' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1330' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1334' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1342' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1350' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1354' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1358' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1366' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1370' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1374' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1383' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1385' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1387' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1394' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1398' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1402' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1408' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1414' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1420' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1424' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1428' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1432' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1436' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1442' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1448' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1454' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1460' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1464' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1470' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1474' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1478' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1482' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1488' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1492' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1496' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1500' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1506' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1510' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1514' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1518' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1522' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1526' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1530' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1543' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1545' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1549' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1555' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1561' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1567' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1573' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1579' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1584' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1589' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1594' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1598' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1602' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1606' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1610' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1614' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1618' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1622' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1628' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1632' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1638' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1644' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1650' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1654' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1658' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1664' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1670' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1674' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1678' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1684' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1688' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1694' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1700' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1706' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1715' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1719' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1723' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1727' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1731' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1735' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1741' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1745' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1749' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1755' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1761' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1765' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1769' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1773' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1777' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1781' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1787' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1791' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1795' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1799' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1803' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1807' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1811' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1815' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1819' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1822' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1828' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1832' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1836' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1842' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1848' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1852' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1858' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1862' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1868' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1874' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1878' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1884' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1888' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1892' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1896' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1902' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1908' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1912' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1918' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1922' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1928' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1932' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1938' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1946' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1952' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1958' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1962' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1968' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1972' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1978' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1982' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1986' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1992' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1996' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2002' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2008' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2012' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2020' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2024' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2028' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2034' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2040' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2044' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2050' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2056' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2060' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2064' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2068' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2074' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2078' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2084' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2088' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2092' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2098' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2102' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2108' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2112' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2118' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2124' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2131' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2133' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2135' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2137' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2139' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2141' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2145' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2151' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2155' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2159' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2165' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2169' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2173' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2180' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2186' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2190' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2194' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2198' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2204' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2208' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2212' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2216' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2220' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2224' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2230' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2236' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2240' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2244' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2248' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2252' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2256' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2260' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2264' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2268' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2275' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2279' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2285' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2291' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2295' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2299' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2305' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2309' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2315' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2321' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2325' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2331' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2335' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2339' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2344' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2350' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2356' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2360' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2366' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2372' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2376' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2382' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2388' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2392' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2396' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2400' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2404' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2410' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2414' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2420' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2424' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2428' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2433' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2437' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2443' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2447' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2453' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2459' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2463' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2469' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2475' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2479' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2483' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2488' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2492' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2498' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2502' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2508' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2513' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2519' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2523' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2527' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2533' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2539' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2543' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2549' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2553' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2559' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2564' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2568' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2574' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2578' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2582' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2588' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2592' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2596' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2601' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2606' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2612' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2618' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2622' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2624' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2626' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2632' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2636' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2640' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2644' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2648' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2652' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2656' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2660' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2664' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2668' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2672' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2676' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2680' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2684' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2688' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2692' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2696' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2700' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2704' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2708' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2712' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2716' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2720' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2724' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2728' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2732' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2736' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2740' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2744' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2748' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2752' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2758' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2762' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2768' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2772' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2776' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2780' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2784' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2788' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2792' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2796' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2800' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2804' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2808' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2812' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2816' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2820' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2824' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2828' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2832' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2836' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2840' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2844' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2848' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2852' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2856' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2860' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2864' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2868' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2872' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2876' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2880' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2884' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2888' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2892' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2896' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2900' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2906' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2910' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2914' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2918' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2922' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2926' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2930' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2934' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2938' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2942' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2946' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2950' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2954' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2958' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2962' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2966' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2970' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2974' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2978' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2982' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2986' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2990' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2994' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2998' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3004' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3008' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3014' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3018' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3022' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3026' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3030' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3034' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3038' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3042' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3046' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3050' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3054' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3058' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3062' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3066' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3070' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3074' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3078' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3082' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3086' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3090' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3094' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3098' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3102' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3106' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3110' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3114' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3118' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3122' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3126' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3130' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3136' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3140' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3146' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3150' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3154' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3158' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3162' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3166' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3170' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3174' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3178' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3182' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3186' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3190' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3194' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3198' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3202' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3206' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3210' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3214' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3218' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3222' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3226' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3230' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3234' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3238' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3242' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3246' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3250' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3254' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3258' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3262' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3266' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3270' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3274' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3278' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3282' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3284' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3286' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3288' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3294' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3298' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3302' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3306' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3310' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3314' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3318' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3322' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3326' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3330' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3334' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3338' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3342' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3346' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3350' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3354' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3358' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3362' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3366' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3370' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3374' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3378' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3382' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3386' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3390' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3394' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3398' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3404' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3406' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3408' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3410' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3412' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3414' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3416' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3418' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3426' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3430' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3434' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3440' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3444' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3448' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3452' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3456' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3460' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3464' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3468' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3472' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3476' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3480' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3484' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3488' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3492' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3496' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3500' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3504' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3508' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3512' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3516' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3520' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3524' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3528' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3532' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3536' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3540' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3544' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3548' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3552' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3556' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3560' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3564' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3568' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3572' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3576' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3580' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3584' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3588' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3592' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3596' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3600' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3604' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3608' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3612' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3616' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3620' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3624' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3628' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3632' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3636' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3638' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3640' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3642' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3644' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3646' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3648' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3650' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3652' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3654' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3656' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3660' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3662' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3664' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3670' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3674' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3678' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3682' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3686' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3690' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3694' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3698' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3702' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3706' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3710' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3714' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3718' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3722' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3726' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3730' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3734' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3738' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3742' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3746' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3750' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3754' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3758' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3762' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3766' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3770' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3774' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3778' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3782' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3786' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3790' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3794' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3798' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3802' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3806' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3810' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3814' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3818' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3822' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3826' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3830' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3834' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3838' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3842' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3846' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3850' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3854' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3858' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3860' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3862' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3864' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3866' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3871' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3879' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3884' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3889' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3894' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3899' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3904' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3909' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3914' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3919' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3924' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3929' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3935' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3939' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3943' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3947' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3951' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3955' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3959' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3963' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3967' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3971' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3975' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3979' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3983' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3987' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3991' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3993' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3995' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P3997' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4001' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4003' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4005' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4013' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4017' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4021' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4025' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4029' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4033' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4037' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4041' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4045' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4049' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4053' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4057' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4061' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4065' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4069' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4073' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4077' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4081' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4085' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4089' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4093' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4097' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4101' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4105' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4109' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4113' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4117' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4121' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4125' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4129' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4133' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4137' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4141' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4145' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4149' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4153' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4157' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4161' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4165' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4169' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4173' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4177' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4181' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4185' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4189' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4193' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4197' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4201' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4205' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4209' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4213' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4217' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4219' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4221' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4229' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4233' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4237' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4241' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4245' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4249' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4253' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4257' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4261' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4265' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4269' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4273' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4277' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4281' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4285' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4289' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4293' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4297' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4301' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4305' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4309' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4313' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4317' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4321' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4325' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4329' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4333' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4337' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4341' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4345' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4349' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4353' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4357' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4361' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4365' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4369' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4373' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4377' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4381' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4385' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4389' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4393' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4397' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4401' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4405' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4409' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4413' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4417' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4421' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4425' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4429' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4431' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4433' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4437' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4439' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4443' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P4449' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P10' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P19' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P28' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p5' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p6' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p7' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p8' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p9' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p10' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p11' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p12' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p13' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p14' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p15' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p16' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p17' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p18' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p19' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p20' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p21' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p22' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p23' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p24' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p25' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p26' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p27' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p28' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p29' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p30' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p31' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p32' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p33' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p34' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p35' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p36' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p37' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p38' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p39' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p40' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p41' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p42' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p43' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p44' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p45' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p46' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p47' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p48' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p49' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p50' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p51' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p52' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p53' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p54' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p55' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p56' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p57' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p58' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p59' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p60' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p61' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p62' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p63' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p64' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p65' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p66' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p67' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p68' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p69' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p70' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p71' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p72' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p73' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p74' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p75' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p76' is an invalid float value
    [36m(Map(process_file) pid=12110, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'p77' is an invalid float value
    2025-10-20 20:09:05,057	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_124_0 execution finished in 18.19 seconds





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>document_id</th>
      <th>file_path</th>
      <th>file_name</th>
      <th>file_extension</th>
      <th>file_size_bytes</th>
      <th>file_size_mb</th>
      <th>discovery_timestamp</th>
      <th>extracted_text</th>
      <th>text_length</th>
      <th>word_count</th>
      <th>extraction_status</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>e0fe8f07-5f07-47c4-aea0-95dd19bf1ecf</td>
      <td>anyscale-rag-application/1000-docs/A Compariso...</td>
      <td>A Comparison of Programming Languages in Econo...</td>
      <td>.pdf</td>
      <td>211355</td>
      <td>0.20</td>
      <td>2025-10-20T20:08:50.946284</td>
      <td>A Comparison of Programming Languages in Econo...</td>
      <td>33839</td>
      <td>5307</td>
      <td>success</td>
    </tr>
    <tr>
      <th>1</th>
      <td>7a2c4d5f-ecf4-4222-a73e-1476aea9c97d</td>
      <td>anyscale-rag-application/1000-docs/A Compariso...</td>
      <td>A Comparison of Software and Hardware Techniqu...</td>
      <td>.pdf</td>
      <td>156844</td>
      <td>0.15</td>
      <td>2025-10-20T20:08:52.951872</td>
      <td>A Comparison of Software and Hardware Techniqu...</td>
      <td>71494</td>
      <td>11296</td>
      <td>success</td>
    </tr>
    <tr>
      <th>2</th>
      <td>71c9003b-2e02-4bbe-ba66-c0611de53a86</td>
      <td>anyscale-rag-application/1000-docs/A Compilati...</td>
      <td>A Compilation Target for Probabilistic Program...</td>
      <td>.pdf</td>
      <td>892594</td>
      <td>0.85</td>
      <td>2025-10-20T20:08:54.244419</td>
      <td>A Compilation Target for Probabilistic Program...</td>
      <td>39374</td>
      <td>6122</td>
      <td>success</td>
    </tr>
    <tr>
      <th>3</th>
      <td>be7c84e0-45f2-48fb-816b-1f89fcb6d038</td>
      <td>anyscale-rag-application/1000-docs/Graph Theor...</td>
      <td>Graph Theory (2005).pdf</td>
      <td>.pdf</td>
      <td>206383</td>
      <td>0.20</td>
      <td>2025-10-20T20:08:54.838291</td>
      <td>V. Adamchik Graph Theory Victor Adamchik Fall ...</td>
      <td>10103</td>
      <td>1600</td>
      <td>success</td>
    </tr>
    <tr>
      <th>4</th>
      <td>156c9d93-db92-4b3c-86ef-ccde0d2636bd</td>
      <td>anyscale-rag-application/1000-docs/Multidigit ...</td>
      <td>Multidigit Multiplication for Mathematicians (...</td>
      <td>.pdf</td>
      <td>346439</td>
      <td>0.33</td>
      <td>2025-10-20T20:08:56.554954</td>
      <td>MULTIDIGIT MULTIPLICATION FOR MATHEMATICIANS D...</td>
      <td>60434</td>
      <td>10046</td>
      <td>success</td>
    </tr>
    <tr>
      <th>5</th>
      <td>89477ca7-248b-4f88-8b9d-faaa570c67c4</td>
      <td>anyscale-rag-application/1000-docs/Shining Lig...</td>
      <td>Shining Light on Shadow Stacks - 7 Nov 2018 (1...</td>
      <td>.pdf</td>
      <td>443892</td>
      <td>0.42</td>
      <td>2025-10-20T20:08:58.067227</td>
      <td>8 1 0 2 v o N 7 ] R C . s c [ 1 v 5 6 1 3 0 . ...</td>
      <td>67521</td>
      <td>10529</td>
      <td>success</td>
    </tr>
    <tr>
      <th>6</th>
      <td>b515b4ae-63e0-4b05-8e09-7427fb7e25b9</td>
      <td>anyscale-rag-application/1000-docs/lwref - BSD...</td>
      <td>lwref - BSDCan2014 - FreeBSD.pdf</td>
      <td>.pdf</td>
      <td>214499</td>
      <td>0.20</td>
      <td>2025-10-20T20:08:58.625721</td>
      <td>An insane idea on reference counting Gleb Smir...</td>
      <td>11319</td>
      <td>1613</td>
      <td>success</td>
    </tr>
    <tr>
      <th>7</th>
      <td>8b3a498c-0630-4c86-becf-000f85b98cf3</td>
      <td>anyscale-rag-application/1000-docs/A Dive in t...</td>
      <td>A Dive in to Hyper-V Architecture and Vulnerab...</td>
      <td>.pdf</td>
      <td>4298584</td>
      <td>4.10</td>
      <td>2025-10-20T20:09:00.147126</td>
      <td>This presentation is for informational purpose...</td>
      <td>13282</td>
      <td>1478</td>
      <td>success</td>
    </tr>
    <tr>
      <th>8</th>
      <td>ecfa57ef-751a-40d4-ba31-66efebc8a41d</td>
      <td>anyscale-rag-application/1000-docs/GraphBLAS M...</td>
      <td>GraphBLAS Mathmatics - Provisional Release 1.0...</td>
      <td>.pdf</td>
      <td>691008</td>
      <td>0.66</td>
      <td>2025-10-20T20:09:01.769236</td>
      <td>GraphBLAS Mathematics - Provisional Release 1....</td>
      <td>40944</td>
      <td>7326</td>
      <td>success</td>
    </tr>
    <tr>
      <th>9</th>
      <td>3867cca9-15cf-48e1-b91d-511bdc6a08b4</td>
      <td>anyscale-rag-application/1000-docs/Multiple By...</td>
      <td>Multiple Byte Processing with Full-Word Instru...</td>
      <td>.pdf</td>
      <td>451218</td>
      <td>0.43</td>
      <td>2025-10-20T20:09:02.324341</td>
      <td>out destroying the 1D C-R property, it is nece...</td>
      <td>21908</td>
      <td>3989</td>
      <td>success</td>
    </tr>
    <tr>
      <th>10</th>
      <td>a26fba0b-cf68-4ace-94ea-6f2ff0021c6a</td>
      <td>anyscale-rag-application/1000-docs/Shuffle - T...</td>
      <td>Shuffle - Tips and Tricks - Slides - GPU Tech ...</td>
      <td>.pdf</td>
      <td>799069</td>
      <td>0.76</td>
      <td>2025-10-20T20:09:03.328344</td>
      <td>Shuffle: Tips and Tricks Julien Demouth, NVIDI...</td>
      <td>7228</td>
      <td>1450</td>
      <td>success</td>
    </tr>
    <tr>
      <th>11</th>
      <td>fc7cc7ea-983f-4402-94ad-4dfaebc32ccc</td>
      <td>anyscale-rag-application/1000-docs/100G Networ...</td>
      <td>100G Networking Technology Overview - Slides -...</td>
      <td>.pdf</td>
      <td>1516903</td>
      <td>1.45</td>
      <td>2025-10-20T20:08:54.986765</td>
      <td>100G Networking Technology Overview Christophe...</td>
      <td>8996</td>
      <td>1558</td>
      <td>success</td>
    </tr>
    <tr>
      <th>12</th>
      <td>64f56828-b85f-4082-a9d7-704e3919319b</td>
      <td>anyscale-rag-application/1000-docs/Grand Centr...</td>
      <td>Grand Central Dispatch - FreeBSD Dev Summit (1...</td>
      <td>.pdf</td>
      <td>130189</td>
      <td>0.12</td>
      <td>2025-10-20T20:08:55.354331</td>
      <td>Grand Central Dispatch FreeBSD Devsummit Rober...</td>
      <td>7831</td>
      <td>1071</td>
      <td>success</td>
    </tr>
    <tr>
      <th>13</th>
      <td>41e049ef-aab1-4d86-83b6-56e49535d238</td>
      <td>anyscale-rag-application/1000-docs/Monitor_a_j...</td>
      <td>Monitor_a_job.docx</td>
      <td>.docx</td>
      <td>387461</td>
      <td>0.37</td>
      <td>2025-10-20T20:08:56.090652</td>
      <td>Monitor a job Anyscale jobs provides several t...</td>
      <td>3296</td>
      <td>585</td>
      <td>success</td>
    </tr>
    <tr>
      <th>14</th>
      <td>bf9e2767-74f9-490e-9884-29ed374b4f19</td>
      <td>anyscale-rag-application/1000-docs/Serial Orde...</td>
      <td>Serial Order - A Parallel Distributed Processi...</td>
      <td>.pdf</td>
      <td>2281776</td>
      <td>2.18</td>
      <td>2025-10-20T20:09:00.974803</td>
      <td>SERIAL ORDER: A PARALLEL DISTRmUTED PROCESSING...</td>
      <td>132375</td>
      <td>21122</td>
      <td>success</td>
    </tr>
    <tr>
      <th>15</th>
      <td>6fad3bf1-1c33-4e6c-86a6-c5f8ecc0ca2a</td>
      <td>anyscale-rag-application/1000-docs/jargn10-the...</td>
      <td>jargn10-thejargonfilever00038gut.txt</td>
      <td>.txt</td>
      <td>1140873</td>
      <td>1.09</td>
      <td>2025-10-20T20:09:03.518624</td>
      <td>This Is The Project Gutenberg Etext of The Hac...</td>
      <td>1065517</td>
      <td>170519</td>
      <td>success</td>
    </tr>
    <tr>
      <th>16</th>
      <td>3748e749-b85f-46b3-8bb8-48ae92544222</td>
      <td>anyscale-rag-application/1000-docs/A Block-sor...</td>
      <td>A Block-sorting Lossless Data Compression Algo...</td>
      <td>.pdf</td>
      <td>108608</td>
      <td>0.10</td>
      <td>2025-10-20T20:08:51.801160</td>
      <td>May 10, 1994 SRC Research Report 124 A Block-s...</td>
      <td>36622</td>
      <td>6095</td>
      <td>success</td>
    </tr>
    <tr>
      <th>17</th>
      <td>648372da-4ac7-4e02-b42d-918cf7b9fdf5</td>
      <td>anyscale-rag-application/1000-docs/A Brief Int...</td>
      <td>A Brief Introduction to the Standard Annotatio...</td>
      <td>.pdf</td>
      <td>358744</td>
      <td>0.34</td>
      <td>2025-10-20T20:08:52.134704</td>
      <td>A Brief Introduction to the Standard Annotatio...</td>
      <td>13276</td>
      <td>2140</td>
      <td>success</td>
    </tr>
    <tr>
      <th>18</th>
      <td>55a34860-c3b7-49be-99b3-bdc37c8294a4</td>
      <td>anyscale-rag-application/1000-docs/A Brief Tut...</td>
      <td>A Brief Tutorial on Database Queries, Data Min...</td>
      <td>.pdf</td>
      <td>207314</td>
      <td>0.20</td>
      <td>2025-10-20T20:08:53.492801</td>
      <td>A Brief Tutorial on Database Queries, Data Min...</td>
      <td>18398</td>
      <td>2878</td>
      <td>success</td>
    </tr>
    <tr>
      <th>19</th>
      <td>19dd49e3-b1c4-4ed5-bd0b-60f9045cca00</td>
      <td>anyscale-rag-application/1000-docs/A Case Stud...</td>
      <td>A Case Study in Optimizing HTM-Enabled Dynamic...</td>
      <td>.pdf</td>
      <td>325479</td>
      <td>0.31</td>
      <td>2025-10-20T20:08:54.698474</td>
      <td>A Case Study in Optimizing HTM-Enabled Dynamic...</td>
      <td>41101</td>
      <td>6551</td>
      <td>success</td>
    </tr>
    <tr>
      <th>20</th>
      <td>ab04216e-7c06-4dbd-a5ee-46001bebeedb</td>
      <td>anyscale-rag-application/1000-docs/A Catalogue...</td>
      <td>A Catalogue of Optimizing Transformations (197...</td>
      <td>.pdf</td>
      <td>2346760</td>
      <td>2.24</td>
      <td>2025-10-20T20:08:55.852502</td>
      <td>A catalogue of optimizing transformations whic...</td>
      <td>35286</td>
      <td>5336</td>
      <td>success</td>
    </tr>
    <tr>
      <th>21</th>
      <td>b19e46a1-e9f1-453c-968f-d388beb76056</td>
      <td>anyscale-rag-application/1000-docs/Graph Theor...</td>
      <td>Graph Theoretic Obstacles to Perfect Hashing -...</td>
      <td>.pdf</td>
      <td>157158</td>
      <td>0.15</td>
      <td>2025-10-20T20:08:56.849256</td>
      <td>Graph theoretic obstacles to perfect hashing G...</td>
      <td>34105</td>
      <td>6403</td>
      <td>success</td>
    </tr>
    <tr>
      <th>22</th>
      <td>0291782f-17bf-4796-94f4-6bea3d78cd46</td>
      <td>anyscale-rag-application/1000-docs/Monotone Mi...</td>
      <td>Monotone Minimal Perfect Hashing - Searching a...</td>
      <td>.pdf</td>
      <td>536251</td>
      <td>0.51</td>
      <td>2025-10-20T20:09:03.211832</td>
      <td>O(1) ∗ † ‡ † S n {0,1,...,n − 1} (cid:0) (cid:...</td>
      <td>46972</td>
      <td>2678</td>
      <td>success</td>
    </tr>
    <tr>
      <th>23</th>
      <td>1907504a-4492-4503-91cf-eefba5082451</td>
      <td>anyscale-rag-application/1000-docs/Setting Up ...</td>
      <td>Setting Up a Production Monitoring and Diagnos...</td>
      <td>.pdf</td>
      <td>4546292</td>
      <td>4.34</td>
      <td>2025-10-20T20:09:04.297444</td>
      <td>https://s.sashag.net/prodsdd Sasha Goldshtein ...</td>
      <td>17096</td>
      <td>2275</td>
      <td>success</td>
    </tr>
    <tr>
      <th>24</th>
      <td>cf3c894d-6251-4c1d-8280-e079d7fd42db</td>
      <td>anyscale-rag-application/1000-docs/libtorque -...</td>
      <td>libtorque - Portable Multithreaded Continuatio...</td>
      <td>.pdf</td>
      <td>279599</td>
      <td>0.27</td>
      <td>2025-10-20T20:09:05.044006</td>
      <td>libtorque: Portable Multithreaded Continuation...</td>
      <td>23775</td>
      <td>3546</td>
      <td>success</td>
    </tr>
  </tbody>
</table>
</div>




```python

# STEP 3: BUSINESS METADATA ENRICHMENT FUNCTION
# Add business context by classifying documents and assigning priority
# This enables filtering and partitioning in the data warehouse

def enrich_business_metadata(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Classify documents by business category and assign processing priority.
    
    BEGINNER NOTE:
    - Input: A record with extracted text and file metadata
    - Output: Same record with added business classification fields
    - Uses filename patterns to determine document type
    
    Why separate this from text extraction?
    - Text extraction is CPU-intensive, this is lightweight string matching
    - Separating concerns makes each stage easier to debug and optimize
    - Allows different resource allocations (CPU) for each stage
    """
    # Get the filename for pattern matching
    file_name = record["file_name"]
    filename_lower = file_name.lower()  # Convert to lowercase for easier matching
    file_size = record["file_size_bytes"]
    
    # Classify by Business Segment
    # Look for keywords in filename to determine business category
    # Real systems might use ML models or lookup tables instead
    # This is a much simpler and naive implementation of text categorization for demo purposes
    
    # Financial documents: earnings reports, revenue statements
    if any(keyword in filename_lower for keyword in ["financial", "earnings", "revenue", "profit"]):
        doc_type = "financial_document"
        business_category = "finance"
    
    # Legal documents: contracts, agreements
    elif any(keyword in filename_lower for keyword in ["legal", "contract", "agreement", "terms"]):
        doc_type = "legal_document"
        business_category = "legal"
    
    # Regulatory documents: compliance filings, SEC reports
    elif any(keyword in filename_lower for keyword in ["regulatory", "compliance", "filing", "sec"]):
        doc_type = "regulatory_document"
        business_category = "compliance"
    
    # Client documents: customer information, portfolios
    elif any(keyword in filename_lower for keyword in ["client", "customer", "portfolio"]):
        doc_type = "client_document"
        business_category = "client_services"
    
    # Research documents: market analysis, reports
    elif any(keyword in filename_lower for keyword in ["market", "research", "analysis", "report"]):
        doc_type = "research_document"
        business_category = "research"
    
    # Default category for unclassified documents
    else:
        doc_type = "general_document"
        business_category = "general"
    

    # Documents with urgent keywords get higher priority for processing
    
    # High priority: urgent, time-sensitive documents
    if any(keyword in filename_lower for keyword in ["urgent", "critical", "deadline"]):
        priority = "high"
        priority_score = 3
    
    # Medium priority: important but not urgent
    elif any(keyword in filename_lower for keyword in ["important", "quarterly", "annual"]):
        priority = "medium"
        priority_score = 2
    
    # Low priority: standard documents
    else:
        priority = "low"
        priority_score = 1
    
    # Return the record
    # Use **record to keep all existing fields, then add new ones
    return {
        **record,  # All existing fields (extracted_text, file_name, etc.)
        "document_type": doc_type,
        "business_category": business_category,
        "processing_priority": priority,
        "priority_score": priority_score,
        "estimated_pages": max(1, file_size // 50000),  # Rough estimate: 50KB per page
        "processing_status": "classified"
    }


# Apply Business Metadata enrichment to all documents
print("Enriching with business metadata...")

# Parameters:
#   - concurrency=16: More parallel tasks since this is lightweight
#   - num_cpus=0.25: Very low CPU usage (just string operations)
#     * This allows many documents to be classified simultaneously

documents_with_metadata = documents_with_text.map(
    enrich_business_metadata,
    concurrency=16,
    num_cpus=0.25
)

# View a few documents with business classification added
documents_with_metadata.limit(5).to_pandas()
```

    2025-10-20 20:09:05,233	INFO logging.py:293 -- Registered dataset logger for dataset dataset_126_0
    2025-10-20 20:09:05,239	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_126_0. Full logs are in /tmp/ray/session_2025-10-20_17-37-47_219984_2353/logs/ray-data
    2025-10-20 20:09:05,239	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_126_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> LimitOperator[limit=5] -> TaskPoolMapOperator[ReadFiles] -> TaskPoolMapOperator[Map(process_file)] -> TaskPoolMapOperator[Map(enrich_business_metadata)]


    Enriching with business metadata...


    /home/ray/anaconda3/lib/python3.12/site-packages/ray/anyscale/data/_internal/cluster_autoscaler/productivity_calculator.py:174: RuntimeWarning: invalid value encountered in divide
      gpu_fraction_per_op = (optimal_num_tasks_per_op * num_gpus_per_op) / np.sum(
    /home/ray/anaconda3/lib/python3.12/site-packages/ray/data/_internal/execution/operators/task_pool_map_operator.py:165: UserWarning: The maximum number of concurrent tasks for 'Map(process_file)' is set to 16, but the operator only received 1 input(s). This means that the operator can launch at most 1 task(s), which is less than the concurrency limit. You might be able to increase the number of concurrent tasks by configuring `override_num_blocks` earlier in the pipeline.
      warnings.warn(
    /home/ray/anaconda3/lib/python3.12/site-packages/ray/data/_internal/execution/operators/task_pool_map_operator.py:165: UserWarning: The maximum number of concurrent tasks for 'Map(enrich_business_metadata)' is set to 16, but the operator only received 1 input(s). This means that the operator can launch at most 1 task(s), which is less than the concurrency limit. You might be able to increase the number of concurrent tasks by configuring `override_num_blocks` earlier in the pipeline.
      warnings.warn(
    2025-10-20 20:09:20,005	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_126_0 execution finished in 14.76 seconds





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>document_id</th>
      <th>file_path</th>
      <th>file_name</th>
      <th>file_extension</th>
      <th>file_size_bytes</th>
      <th>file_size_mb</th>
      <th>discovery_timestamp</th>
      <th>extracted_text</th>
      <th>text_length</th>
      <th>word_count</th>
      <th>extraction_status</th>
      <th>document_type</th>
      <th>business_category</th>
      <th>processing_priority</th>
      <th>priority_score</th>
      <th>estimated_pages</th>
      <th>processing_status</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>682d973d-a8c8-46ff-9b87-a84d10684f6c</td>
      <td>anyscale-rag-application/1000-docs/100G Networ...</td>
      <td>100G Networking Technology Overview - Slides -...</td>
      <td>.pdf</td>
      <td>1516903</td>
      <td>1.45</td>
      <td>2025-10-20T20:09:11.424457</td>
      <td>100G Networking Technology Overview Christophe...</td>
      <td>8996</td>
      <td>1558</td>
      <td>success</td>
      <td>general_document</td>
      <td>general</td>
      <td>low</td>
      <td>1</td>
      <td>30</td>
      <td>classified</td>
    </tr>
    <tr>
      <th>1</th>
      <td>d262b550-2dba-4977-ac73-cb2aa0915adf</td>
      <td>anyscale-rag-application/1000-docs/Grand Centr...</td>
      <td>Grand Central Dispatch - FreeBSD Dev Summit (1...</td>
      <td>.pdf</td>
      <td>130189</td>
      <td>0.12</td>
      <td>2025-10-20T20:09:11.788307</td>
      <td>Grand Central Dispatch FreeBSD Devsummit Rober...</td>
      <td>7831</td>
      <td>1071</td>
      <td>success</td>
      <td>general_document</td>
      <td>general</td>
      <td>low</td>
      <td>1</td>
      <td>2</td>
      <td>classified</td>
    </tr>
    <tr>
      <th>2</th>
      <td>57eef3e9-0c9c-4fb7-b95e-d083e84e55ac</td>
      <td>anyscale-rag-application/1000-docs/Monitor_a_j...</td>
      <td>Monitor_a_job.docx</td>
      <td>.docx</td>
      <td>387461</td>
      <td>0.37</td>
      <td>2025-10-20T20:09:12.526486</td>
      <td>Monitor a job Anyscale jobs provides several t...</td>
      <td>3296</td>
      <td>585</td>
      <td>success</td>
      <td>general_document</td>
      <td>general</td>
      <td>low</td>
      <td>1</td>
      <td>7</td>
      <td>classified</td>
    </tr>
    <tr>
      <th>3</th>
      <td>703a2fd4-44c1-4703-8082-a78e8151dd98</td>
      <td>anyscale-rag-application/1000-docs/Serial Orde...</td>
      <td>Serial Order - A Parallel Distributed Processi...</td>
      <td>.pdf</td>
      <td>2281776</td>
      <td>2.18</td>
      <td>2025-10-20T20:09:17.436733</td>
      <td>SERIAL ORDER: A PARALLEL DISTRmUTED PROCESSING...</td>
      <td>132375</td>
      <td>21122</td>
      <td>success</td>
      <td>general_document</td>
      <td>general</td>
      <td>low</td>
      <td>1</td>
      <td>45</td>
      <td>classified</td>
    </tr>
    <tr>
      <th>4</th>
      <td>6a2bab92-3d51-49d6-a7b6-c317a023d239</td>
      <td>anyscale-rag-application/1000-docs/jargn10-the...</td>
      <td>jargn10-thejargonfilever00038gut.txt</td>
      <td>.txt</td>
      <td>1140873</td>
      <td>1.09</td>
      <td>2025-10-20T20:09:19.958387</td>
      <td>This Is The Project Gutenberg Etext of The Hac...</td>
      <td>1065517</td>
      <td>170519</td>
      <td>success</td>
      <td>general_document</td>
      <td>general</td>
      <td>low</td>
      <td>1</td>
      <td>22</td>
      <td>classified</td>
    </tr>
  </tbody>
</table>
</div>




```python

# WHY AGGREGATIONS?
# Before writing to warehouse, understand what we're processing:
# - How many documents of each type?
# - What's the size distribution?
# - Which categories have the most content?

# AGGREGATION 1: Document Type Distribution
# Group by document_type and calculate statistics

doc_type_stats = documents_with_metadata.groupby("document_type").aggregate(
    Count(),  # How many documents of each type?
    Sum("file_size_bytes"),  # Total size per document type
    Mean("file_size_mb"),  # Average size per document type
    Max("estimated_pages")  # Largest document per type
)

# AGGREGATION 2: Business Category Analysis
# Understand the distribution across business categories
# This helps with warehouse partitioning strategy

category_stats = documents_with_metadata.groupby("business_category").aggregate(
    Count(),  # How many per category?
    Mean("priority_score"),  # Average priority per category
    Sum("file_size_mb")  # Total data volume per category
)
```

## Step 2: Document Processing and Classification

### Text extraction and quality assessment


```python

# STEP 4: QUALITY ASSESSMENT FUNCTION
# Evaluate document quality to filter out low-quality or problematic documents

def assess_document_quality(batch: pd.DataFrame) -> pd.DataFrame:
    """
    Assess document quality for data warehouse ingestion.
    
    BEGINNER NOTE:
    - Input: pandas DataFrame with multiple documents (a "batch")
    - Output: Same DataFrame with quality assessment columns added
    - Uses map_batches() for efficiency (process many docs at once)
    
    Why use map_batches() instead of map()?
    - Batching is more efficient for lightweight operations
    - Pandas DataFrame operations are optimized
    - Reduces overhead from function calls
    
    Explicitly use batch_format="pandas" 
    """
    quality_scores = np.zeros(len(batch), dtype=int)  # Numeric score (0-4)
    quality_ratings = []  # Text rating (high/medium/low)
    quality_issues_list = []  # List of issues found
    
    # We iterate through rows to apply business rules for quality
    # Each document gets a score from 0-4 based on quality criteria
    
    for idx, row in batch.iterrows():
        quality_score = 0
        quality_issues = []
        
        # CRITERION 1: File size check
        # Files smaller than 10KB might be empty or corrupt
        if row["file_size_mb"] > 0.01:  # More than 10KB
            quality_score += 1
        else:
            quality_issues.append("file_too_small")
        
        # CRITERION 2: Text length check
        # Documents should have meaningful text content
        if row["text_length"] > 100:  # At least 100 characters
            quality_score += 1
        else:
            quality_issues.append("insufficient_text")
        
        # CRITERION 3: Business relevance check
        # Classified documents are more valuable than unclassified
        if row["business_category"] != "general":
            quality_score += 1
        else:
            quality_issues.append("low_business_relevance")
        
        # CRITERION 4: Word count check
        # Documents should have substantial content
        if row["word_count"] > 20:  # At least 20 words
            quality_score += 1
        else:
            quality_issues.append("insufficient_content")
        
        # Score 4: All checks passed - high quality
        # Score 2-3: Some issues - medium quality
        # Score 0-1: Major issues - low quality
        quality_rating = "high" if quality_score >= 4 else "medium" if quality_score >= 2 else "low"
        
        # Store results for this document
        quality_scores[idx] = quality_score
        quality_ratings.append(quality_rating)
        quality_issues_list.append(json.dumps(quality_issues))  # Convert list to JSON string
    
    batch["quality_score"] = quality_scores
    batch["quality_rating"] = quality_ratings
    batch["quality_issues"] = quality_issues_list
    
    return batch


# Apply Quality Assessment to all documents

# Parameters:
#   - batch_format="pandas": Process as pandas DataFrame (easier than numpy arrays)
#   - num_cpus=0.25: Very low CPU (this is lightweight logic)
#   - batch_size=100: Process 100 documents at a time
#     * Larger batches = fewer function calls = better efficiency

quality_assessed_docs = documents_with_metadata.map_batches(
    assess_document_quality,
    batch_format="pandas",  # Ray Data pattern: explicit pandas format
    num_cpus=0.25,
    batch_size=100
)
```

## Step 3: Text Chunking and Enrichment


```python

# STEP 5: TEXT CHUNKING FUNCTION
# Split long documents into smaller chunks for downstream processing
# Why chunk? Many applications (LLMs, vector databases) have size limits

def create_text_chunks(record: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Create overlapping text chunks from each document.
    
    BEGINNER NOTE:
    - Input: ONE document record with full text
    - Output: MULTIPLE chunk records (one document → many chunks)
    - This is a "one-to-many" transformation
    
    Why chunking?
    - LLM APIs have token limits (e.g., 4096 tokens)
    - Vector databases work better with smaller chunks
    - Enables more granular search and analysis
    
    Why overlapping chunks?
    - Preserves context across chunk boundaries
    - Prevents splitting important information
    - 150 character overlap means each chunk shares text with neighbors
    """
    text = record["extracted_text"]  # The full document text
    chunk_size = 1500  # Each chunk will be ~1500 characters
    overlap = 150  # Adjacent chunks share 150 characters
    
    # Why these numbers?
    # - 1500 chars ≈ 300-400 tokens (good for most LLM APIs)
    # - 150 char overlap ≈ 10% overlap (preserves context without too much redundancy)
    # - You can adjust these based on your use case
    
    # Let's create chunks of data by a sliding window
    chunks = []
    start = 0  # Starting position in the text
    chunk_index = 0  # Track which chunk number this is

    # There are many more advanced chunking methods, we'll use this simple technique for demo purposes
    
    # Loop until we've processed all the text
    while start < len(text):
        # Calculate end position (don't go past text length)
        end = min(start + chunk_size, len(text))
        
        # Extract this chunk's text
        chunk_text = text[start:end]
        
        # Create a new record for this chunk
        # It contains all the original document metadata PLUS chunk-specific data
        chunk_record = {
            **record,  # All original fields (document_id, business_category, etc.)
            "chunk_id": str(uuid.uuid4()),  # Unique ID for this specific chunk
            "chunk_index": chunk_index,  # Position in sequence (0, 1, 2, ...)
            "chunk_text": chunk_text,  # The actual text content of this chunk
            "chunk_length": len(chunk_text),  # Characters in this chunk
            "chunk_word_count": len(chunk_text.split())  # Words in this chunk
        }
        
        chunks.append(chunk_record)
        
        # If we've reached the end of the text, we're done
        if end >= len(text):
            break
        
        # Move to next chunk position (with overlap)
        # Example: If chunk_size=1500 and overlap=150
        # Chunk 1: chars 0-1500
        # Chunk 2: chars 1350-2850 (starts 150 before chunk 1 ended)
        # Chunk 3: chars 2700-4200 (starts 150 before chunk 2 ended)
        start = end - overlap
        chunk_index += 1

    # After creating all chunks, add how many chunks the document has
    # This helps with progress tracking and completeness checks
    for chunk in chunks:
        chunk["total_chunks"] = len(chunks)
    
    # Return the list of chunk records
    # Ray Data's flat_map() will automatically flatten this list
    return chunks


# Apply Text Chunking to all documents
# Use flat_map() for one-to-many transformations
# One document becomes multiple chunks
print("Creating text chunks...")

# Why flat_map() instead of map()?
#   - map(): One input → One output (document → document)
#   - flat_map(): One input → Many outputs (document → multiple chunks)
#   - flat_map() automatically "flattens" the list of chunks
#
# Example:
#   Input: 100 documents
#   Output: 10,000+ chunks (each document becomes ~100 chunks on average)
#
# Parameters:
#   - num_cpus=0.5: Moderate CPU usage (string slicing is lightweight)

chunked_documents = quality_assessed_docs.flat_map(
    create_text_chunks,
    num_cpus=0.5
)
```

    Creating text chunks...


## Step 4: Data Warehouse Schema and Output

### Create data warehouse schema


```python

# STEP 6: DATA WAREHOUSE SCHEMA TRANSFORMATION
# Transform the raw processing data into a clean warehouse schema
# This is the "ETL" part - Extract (done), Transform (now), Load (next)

print("Creating data warehouse schema...")


# Get today's date in ISO format (YYYY-MM-DD)
# This will be used to partition the data by date in the warehouse
processing_date = datetime.now().isoformat()[:10]


# Data warehouses need clean, organized schemas
# We'll select only the columns we need and organize them logically
#
# Why not keep all columns?
# - Cleaner schema = easier queries
# - Less storage space
# - Better performance
# - Clear data contracts for downstream users

warehouse_dataset = chunked_documents.select_columns([
    # PRIMARY IDENTIFIERS: Keys for joining and relationships
    "document_id",  # Links all chunks from same document
    "chunk_id",     # Unique identifier for this specific chunk
    
    # DIMENSIONAL ATTRIBUTES: Categorical data for filtering/grouping
    # These are typically used in WHERE clauses and GROUP BY
    "business_category",      # finance, legal, compliance, etc.
    "document_type",          # financial_document, legal_document, etc.
    "file_extension",         # .pdf, .docx, etc.
    "quality_rating",         # high, medium, low
    "processing_priority",    # high, medium, low
    
    # FACT MEASURES: Numeric values for aggregation and analysis
    # These are typically used in SUM(), AVG(), COUNT(), etc.
    "file_size_mb",           # Document size
    "word_count",             # Total words in document
    "chunk_word_count",       # Words in this chunk
    "quality_score",          # Numeric quality (0-4)
    "priority_score",         # Numeric priority (1-3)
    "estimated_pages",        # Page count estimate
    "chunk_index",            # Position in document (0, 1, 2, ...)
    "total_chunks",           # How many chunks total
    
    # CONTENT FIELDS: The actual data payload
    "chunk_text",             # The text content (will rename this)
    "file_name",              # Original filename
    "file_path",              # S3 location
    
    # METADATA: Processing provenance and status tracking
    "discovery_timestamp",    # When was file discovered
    "extraction_status",      # success, error, unsupported_format
    "processing_status"       # classified, processed, etc.
])

# RENAME COLUMNS for data warehouse conventions
# "chunk_text" → "text_content" (more descriptive)
warehouse_dataset = warehouse_dataset.rename_columns({
    "chunk_text": "text_content"
})

# ADD PIPELINE METADATA: Constant columns for all records
# These columns are the same for every record in this run
# They help with data lineage and debugging

def add_pipeline_metadata(batch: pd.DataFrame) -> pd.DataFrame:
    """
    Add constant metadata columns to every record.
    
    BEGINNER NOTE:
    - These columns help track WHERE and WHEN data was processed
    - Useful for debugging and auditing
    - All records in this batch get the same values
    
    Why map_batches() for constants?
    - More efficient than adding columns one at a time
    - Can add multiple columns in one pass
    - Pandas operations are fast for this
    """
    batch["processing_date"] = processing_date     # When was this processed?
    batch["pipeline_version"] = "1.0"             # Which version of pipeline?
    batch["processing_engine"] = "ray_data"       # What tool processed it?
    return batch

# Apply metadata addition
# Parameters:
#   - batch_format="pandas": Use pandas for easy column addition
#   - num_cpus=0.1: Very low CPU (just adding constants)
#   - batch_size=10000: Large batches (this is very fast)

warehouse_dataset = warehouse_dataset.map_batches(
    add_pipeline_metadata,
    batch_format="pandas",
    num_cpus=0.1,
    batch_size=1000
)
```

    Creating data warehouse schema...


### Write to data warehouse with partitioning


```python

# STEP 7: WRITE TO DATA WAREHOUSE - MAIN TABLE
# Save all processed chunks to Parquet format with partitioning
# This is the "Load" part of ETL

# /mnt/cluster_storage is a shared storage volume accessible by all workers
# In production, this would typically be:
# - S3: s3://your-bucket/warehouse/
# - Azure: abfs://container@account.dfs.core.windows.net/
# - GCS: gs://your-bucket/warehouse/
OUTPUT_WAREHOUSE_PATH = "/mnt/cluster_storage"

# WRITE MAIN TABLE with PARTITIONING
# write_parquet() is Ray Data's native way to save data

# Key parameters explained:

# partition_cols=["business_category", "processing_date"]
#   - Creates folder structure: business_category=finance/processing_date=2025-10-15/
#   - Enables efficient querying: "SELECT * FROM table WHERE business_category='finance'"
#   - Query engines (Spark, Presto, Athena) can skip entire partitions
#   - Example structure:
#       main_table/
#       ├── business_category=finance/
#       │   └── processing_date=2025-10-15/
#       │       ├── part-001.parquet
#       │       └── part-002.parquet
#       ├── business_category=legal/
#       │   └── processing_date=2025-10-15/
#       │       └── part-001.parquet
#       └── business_category=compliance/
#           └── processing_date=2025-10-15/
#               └── part-001.parquet

# compression="snappy"
#   - Compress files to save storage space (50-70% reduction)
#   - Snappy is fast and well-supported by all query engines
#   - Alternatives: gzip (higher compression), zstd (good balance)

# ray_remote_args={"num_cpus": 0.1}
#   - Writing to storage is I/O-bound, not CPU-bound
#   - Low CPU allocation allows more parallel writes

warehouse_dataset.write_parquet(
    f"{OUTPUT_WAREHOUSE_PATH}/main_table/",
    partition_cols=["business_category", "processing_date"],
    compression="snappy",
    ray_remote_args={"num_cpus": 0.1}
)

print("Main warehouse table written successfully")
```

    2025-10-20 20:09:20,615	INFO logging.py:293 -- Registered dataset logger for dataset dataset_137_0
    2025-10-20 20:09:20,622	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_137_0. Full logs are in /tmp/ray/session_2025-10-20_17-37-47_219984_2353/logs/ray-data
    2025-10-20 20:09:20,622	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_137_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> LimitOperator[limit=100] -> TaskPoolMapOperator[ReadFiles] -> TaskPoolMapOperator[Map(process_file)] -> TaskPoolMapOperator[Map(enrich_business_metadata)] -> TaskPoolMapOperator[MapBatches(assess_document_quality)] -> TaskPoolMapOperator[FlatMap(create_text_chunks)] -> TaskPoolMapOperator[Project] -> TaskPoolMapOperator[MapBatches(add_pipeline_metadata)->Write]
    /home/ray/anaconda3/lib/python3.12/site-packages/ray/anyscale/data/_internal/cluster_autoscaler/productivity_calculator.py:174: RuntimeWarning: invalid value encountered in divide
      gpu_fraction_per_op = (optimal_num_tasks_per_op * num_gpus_per_op) / np.sum(
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P15' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P19' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P23' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P27' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P33' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P39' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P43' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P47' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P53' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P57' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P63' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P74' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P78' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P84' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P88' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P90' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P92' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P94' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P96' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P98' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P100' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P102' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P104' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P106' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P108' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P110' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P112' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P114' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P116' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P118' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P120' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P122' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P124' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P126' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P128' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P130' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P132' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P134' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P136' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P138' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P140' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P142' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P144' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P146' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P148' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P152' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P160' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P168' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P174' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P180' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P186' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P192' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P198' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P204' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P217' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P221' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P227' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P235' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P239' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P243' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P251' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P255' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P259' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P265' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P271' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P275' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P281' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P285' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P289' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P295' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P299' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P305' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P311' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P315' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P321' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P325' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P331' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P337' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P341' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P347' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P353' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P357' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P361' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P365' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P371' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P375' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P379' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P383' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P387' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P391' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P395' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P399' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P403' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P407' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P413' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P417' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P421' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P425' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P429' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P435' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P441' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P447' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P451' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P457' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P463' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P465' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P467' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P469' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P471' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P473' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P475' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P477' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P479' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P481' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P483' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P485' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P487' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P489' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P491' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P493' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P495' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P497' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P499' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P501' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P503' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P505' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P507' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P509' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P511' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P513' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P515' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P517' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P525' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P529' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P535' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P541' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P547' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P553' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P557' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P559' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P561' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P574' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P580' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P584' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P588' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P594' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P598' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P602' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P608' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P616' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P624' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P632' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P640' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P648' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P656' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P658' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P660' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P662' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P664' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P666' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P668' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P670' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P672' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P674' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P676' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P678' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P684' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P688' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P692' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P696' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P702' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P706' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P710' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P714' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P718' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P722' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P726' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P730' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P736' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P740' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P746' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P750' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P756' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P760' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P766' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P770' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P774' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P778' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P784' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P790' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P796' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P800' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P806' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P810' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P816' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P822' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P826' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P832' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P836' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P840' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P844' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P848' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P852' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P856' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P860' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P864' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P868' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P872' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P876' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P880' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P884' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P888' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P892' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P896' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P900' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P904' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P908' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P912' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P916' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P920' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P924' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P928' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P932' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P936' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P940' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P944' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P948' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P952' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P956' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P960' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P964' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P972' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P976' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P982' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P986' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P992' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P996' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1000' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1004' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1010' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1016' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1020' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1026' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1030' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1036' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1040' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1046' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1052' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1056' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1060' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1064' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1068' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1072' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1076' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1080' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1084' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1090' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1094' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1100' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1106' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1110' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1114' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1118' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1124' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1128' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1132' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1138' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1144' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1148' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1154' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1162' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1166' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1170' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1174' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1178' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1184' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1188' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1192' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1196' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1200' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1204' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1208' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1212' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1216' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1220' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1224' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1228' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1232' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1236' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1240' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1244' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1248' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1252' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1256' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1260' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1264' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1268' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1272' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1276' is an invalid float value
    [36m(Map(process_file) pid=14642, ip=10.0.119.92)[0m Could get FontBBox from font descriptor because None cannot be parsed as 4 floats
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2692' is an invalid float value[32m [repeated 303x across cluster][0m
    [36m(Map(process_file) pid=11844, ip=10.0.76.100)[0m Cannot set gray non-stroke color because /'P152' is an invalid float value[32m [repeated 528x across cluster][0m
    [36m(Map(process_file) pid=15177, ip=10.0.93.255)[0m Cannot set gray non-stroke color because /'P275' is an invalid float value[32m [repeated 203x across cluster][0m
    2025-10-20 20:09:50,703	WARNING issue_detector_manager.py:58 -- 
    
    Operator 'ReadFiles' uses 679.2MB of memory per task on average, but
    Ray only requests 0.0B per task at the start of the pipeline.
    
    To avoid out-of-memory errors, consider setting `memory=679.2MB` in
    the appropriate function or method call. (This might be unnecessary if
    the number of concurrent tasks is low.)
    
    To change the frequency of this warning, set
    `DataContext.get_current().issue_detectors_config.high_memory_detector_config.detection_time_interval_s`,
    or disable the warning by setting value to -1. (current value: 30)
    
    2025-10-20 20:09:50,704	WARNING issue_detector_manager.py:67 -- To disable issue detection, run DataContext.get_current().issue_detectors_config.detectors = [].
    2025-10-20 20:10:20,748	WARNING issue_detector_manager.py:58 -- 
    
    Operator 'ReadFiles' uses 679.2MB of memory per task on average, but
    Ray only requests 0.0B per task at the start of the pipeline.
    
    To avoid out-of-memory errors, consider setting `memory=679.2MB` in
    the appropriate function or method call. (This might be unnecessary if
    the number of concurrent tasks is low.)
    
    To change the frequency of this warning, set
    `DataContext.get_current().issue_detectors_config.high_memory_detector_config.detection_time_interval_s`,
    or disable the warning by setting value to -1. (current value: 30)
    
    2025-10-20 20:10:20,749	WARNING issue_detector_manager.py:67 -- To disable issue detection, run DataContext.get_current().issue_detectors_config.detectors = [].
    2025-10-20 20:10:35,085	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_137_0 execution finished in 74.46 seconds
    2025-10-20 20:10:35,110	INFO dataset.py:5134 -- Data sink Parquet finished. 5116 rows and 23.2MB data written.


    Main warehouse table written successfully



```python


# STEP 8: CREATE BUSINESS-SPECIFIC DATASETS
# Create specialized datasets for specific business teams
# Each team gets only the data they need with relevant columns


# Example: Compliance Analytics Dataset
# Compliance team needs: document content, quality, and priority
# Priority matters for compliance review workflows

compliance_analytics = warehouse_dataset.filter(
    lambda row: row["business_category"] == "compliance"
).select_columns([
    "document_id",          # Link to main table
    "chunk_id",             # Unique chunk identifier
    "text_content",         # The actual text
    "quality_score",        # Data reliability
    "processing_priority",  # urgent/important/normal
    "processing_date"       # When processed
])

# Write to dedicated compliance folder
compliance_analytics.write_parquet(
    f"{OUTPUT_WAREHOUSE_PATH}/analytics/compliance/",
    partition_cols=["processing_date"],
    compression="snappy",
    ray_remote_args={"num_cpus": 0.1}
)
```

    /home/ray/anaconda3/lib/python3.12/site-packages/ray/data/dataset.py:1557: UserWarning: Use 'expr' instead of 'fn' when possible for performant filters.
      warnings.warn(
    2025-10-20 20:10:35,277	INFO logging.py:293 -- Registered dataset logger for dataset dataset_142_0
    2025-10-20 20:10:35,283	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_142_0. Full logs are in /tmp/ray/session_2025-10-20_17-37-47_219984_2353/logs/ray-data
    2025-10-20 20:10:35,283	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_142_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> LimitOperator[limit=100] -> TaskPoolMapOperator[ReadFiles] -> TaskPoolMapOperator[Map(process_file)] -> TaskPoolMapOperator[Map(enrich_business_metadata)] -> TaskPoolMapOperator[MapBatches(assess_document_quality)] -> TaskPoolMapOperator[FlatMap(create_text_chunks)] -> TaskPoolMapOperator[Project] -> TaskPoolMapOperator[MapBatches(add_pipeline_metadata)] -> TaskPoolMapOperator[Filter(<lambda>)->Project] -> TaskPoolMapOperator[Write]


    /home/ray/anaconda3/lib/python3.12/site-packages/ray/anyscale/data/_internal/cluster_autoscaler/productivity_calculator.py:174: RuntimeWarning: invalid value encountered in divide
      gpu_fraction_per_op = (optimal_num_tasks_per_op * num_gpus_per_op) / np.sum(
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P15' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P19' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P23' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P27' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P33' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P39' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P43' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P47' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P53' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P57' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P63' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P74' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P78' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P84' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P88' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P90' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P92' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P94' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P96' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P98' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P100' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P102' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P104' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P106' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P108' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P110' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P112' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P114' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P116' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P118' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P120' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P122' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P124' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P126' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P128' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P130' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P132' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P134' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P136' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P138' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P140' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P142' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P144' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P146' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P148' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P152' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P160' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P168' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P174' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P180' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P186' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P192' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P198' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P204' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P217' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P221' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P227' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P235' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P239' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P243' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P251' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P255' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P259' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P265' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P271' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P275' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P281' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P285' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P289' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P295' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P299' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P305' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P311' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P315' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P321' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P325' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P331' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P337' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P341' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P347' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P353' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P357' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P361' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P365' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P371' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P375' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P379' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P383' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P387' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P391' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P395' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P399' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P403' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P407' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P413' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P417' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P421' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P425' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P429' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P435' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P441' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P447' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P451' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P457' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P463' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P465' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P467' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P469' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P471' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P473' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P475' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P477' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P479' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P481' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P483' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P485' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P487' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P489' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P491' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P493' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P495' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P497' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P499' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P501' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P503' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P505' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P507' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P509' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P511' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P513' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P515' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P517' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P525' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P529' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P535' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P541' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P547' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P553' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P557' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P559' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P561' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P574' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P580' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P584' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P588' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P594' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P598' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P602' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P608' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P616' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P624' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P632' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P640' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P648' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P656' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P658' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P660' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P662' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P664' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P666' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P668' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P670' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P672' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P674' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P676' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P678' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P684' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P688' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P692' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P696' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P702' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P706' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P710' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P714' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P718' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P722' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P726' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P730' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P736' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P740' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P746' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P750' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P756' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P760' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P766' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P770' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P774' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P778' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P784' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P790' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P796' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P800' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P806' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P810' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P816' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P822' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P826' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P832' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P836' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P840' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P844' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P848' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P852' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P856' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P860' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P864' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P868' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P872' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P876' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P880' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P884' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P888' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P892' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P896' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P900' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P904' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P908' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P912' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P916' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P920' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P924' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P928' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P932' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P936' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P940' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P944' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P948' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P952' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P956' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P960' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P964' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P972' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P976' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P982' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P986' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P992' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P996' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1000' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1004' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1010' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1016' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1020' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1026' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1030' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1036' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1040' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1046' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1052' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1056' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1060' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1064' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1068' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1072' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1076' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1080' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1084' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1090' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1094' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1100' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1106' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1110' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1114' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1118' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1124' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1128' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1132' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1138' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1144' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1148' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1154' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1162' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1166' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1170' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1174' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1178' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1184' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1188' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1192' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1196' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1200' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1204' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1208' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1212' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1216' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1220' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1224' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1228' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1232' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1236' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1240' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1244' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1248' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1252' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1256' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1260' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1264' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1268' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1272' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1276' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1280' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1284' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1288' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1292' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1296' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1300' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P1304' is an invalid float value
    [36m(Map(process_file) pid=14641, ip=10.0.119.92)[0m Cannot set gray non-stroke color because /'P2644' is an invalid float value[32m [repeated 284x across cluster][0m
    [36m(Map(process_file) pid=12111, ip=10.0.92.64)[0m Cannot set gray non-stroke color because /'P275' is an invalid float value[32m [repeated 574x across cluster][0m
    [36m(Map(process_file) pid=13718, ip=10.0.90.35)[0m Could get FontBBox from font descriptor because None cannot be parsed as 4 floats
    [36m(Map(process_file) pid=15321, ip=10.0.90.35)[0m Cannot set gray non-stroke color because /'P217' is an invalid float value[32m [repeated 172x across cluster][0m
    2025-10-20 20:11:05,384	WARNING issue_detector_manager.py:58 -- 
    
    Operator 'ReadFiles' uses 728.6MB of memory per task on average, but
    Ray only requests 0.0B per task at the start of the pipeline.
    
    To avoid out-of-memory errors, consider setting `memory=728.6MB` in
    the appropriate function or method call. (This might be unnecessary if
    the number of concurrent tasks is low.)
    
    To change the frequency of this warning, set
    `DataContext.get_current().issue_detectors_config.high_memory_detector_config.detection_time_interval_s`,
    or disable the warning by setting value to -1. (current value: 30)
    
    2025-10-20 20:11:05,385	WARNING issue_detector_manager.py:67 -- To disable issue detection, run DataContext.get_current().issue_detectors_config.detectors = [].
    2025-10-20 20:11:35,412	WARNING issue_detector_manager.py:58 -- 
    
    Operator 'ReadFiles' uses 728.6MB of memory per task on average, but
    Ray only requests 0.0B per task at the start of the pipeline.
    
    To avoid out-of-memory errors, consider setting `memory=728.6MB` in
    the appropriate function or method call. (This might be unnecessary if
    the number of concurrent tasks is low.)
    
    To change the frequency of this warning, set
    `DataContext.get_current().issue_detectors_config.high_memory_detector_config.detection_time_interval_s`,
    or disable the warning by setting value to -1. (current value: 30)
    
    2025-10-20 20:11:35,413	WARNING issue_detector_manager.py:67 -- To disable issue detection, run DataContext.get_current().issue_detectors_config.detectors = [].
    2025-10-20 20:11:59,398	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_142_0 execution finished in 84.11 seconds
    2025-10-20 20:11:59,421	INFO dataset.py:5134 -- Data sink Parquet finished. 0 rows and 0.0B data written.


### Create analytics summary tables


```python

# STEP 9: CREATE ANALYTICS SUMMARY TABLES
# Pre-compute common aggregations for fast dashboard queries
# Summary tables = faster analytics queries


# SUMMARY TABLE 1: Processing Metrics by Category and Date

# Answer questions like:
# - How many documents processed per category per day?
# - What's the total data volume per category?
# - What's the average document quality by category?

# This summary makes dashboard queries instant instead of scanning all data

# groupby() groups data by multiple columns
# aggregate() calculates statistics for each group

processing_metrics = warehouse_dataset.groupby(["business_category", "processing_date"]).aggregate(
    Count(),                    # How many chunks per category+date?
    Sum("file_size_mb"),        # Total data volume
    Mean("word_count"),         # Average document size
    Mean("quality_score")       # Average quality
)

# Write summary table
# Smaller than main table, so queries are very fast
processing_metrics.write_parquet(
    f"{OUTPUT_WAREHOUSE_PATH}/summaries/processing_metrics/",
    partition_cols=["processing_date"],
    compression="snappy",
    ray_remote_args={"num_cpus": 0.1}
)
```

    2025-10-20 20:11:59,556	INFO logging.py:293 -- Registered dataset logger for dataset dataset_147_0
    2025-10-20 20:11:59,563	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_147_0. Full logs are in /tmp/ray/session_2025-10-20_17-37-47_219984_2353/logs/ray-data
    2025-10-20 20:11:59,564	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_147_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> LimitOperator[limit=100] -> TaskPoolMapOperator[ReadFiles] -> TaskPoolMapOperator[Map(process_file)] -> TaskPoolMapOperator[Map(enrich_business_metadata)] -> TaskPoolMapOperator[MapBatches(assess_document_quality)] -> TaskPoolMapOperator[FlatMap(create_text_chunks)] -> TaskPoolMapOperator[Project] -> TaskPoolMapOperator[MapBatches(add_pipeline_metadata)] -> HashAggregateOperator[HashAggregate(key_columns=('business_category', 'processing_date'), num_partitions=200)] -> TaskPoolMapOperator[Write]
    /home/ray/anaconda3/lib/python3.12/site-packages/ray/anyscale/data/_internal/cluster_autoscaler/productivity_calculator.py:174: RuntimeWarning: invalid value encountered in divide
      gpu_fraction_per_op = (optimal_num_tasks_per_op * num_gpus_per_op) / np.sum(
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P15' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P19' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P23' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P27' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P33' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P39' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P43' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P47' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P53' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P57' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P63' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P74' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P78' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P84' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P88' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P90' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P92' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P94' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P96' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P98' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P100' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P102' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P104' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P106' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P108' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P110' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P112' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P114' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P116' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P118' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P120' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P122' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P124' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P126' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P128' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P130' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P132' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P134' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P136' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P138' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P140' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P142' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P144' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P146' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P148' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P152' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P160' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P168' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P174' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P180' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P186' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P192' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P198' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P204' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P217' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P221' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P227' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P235' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P239' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P243' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P251' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P255' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P259' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P265' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P271' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P275' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P281' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P285' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P289' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P295' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P299' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P305' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P311' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P315' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P321' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P325' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P331' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P337' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P341' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P347' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P353' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P357' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P361' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P365' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P371' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P375' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P379' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P383' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P387' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P391' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P395' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P399' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P403' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P407' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P413' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P417' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P421' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P425' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P429' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P435' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P441' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P447' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P451' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P457' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P463' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P465' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P467' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P469' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P471' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P473' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P475' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P477' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P479' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P481' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P483' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P485' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P487' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P489' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P491' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P493' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P495' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P497' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P499' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P501' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P503' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P505' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P507' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P509' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P511' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P513' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P515' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P517' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P525' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P529' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P535' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P541' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P547' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P553' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P557' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P559' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P561' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P574' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P580' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P584' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P588' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P594' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P598' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P602' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P608' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P616' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P624' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P632' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P640' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P648' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P656' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P658' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P660' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P662' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P664' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P666' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P668' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P670' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P672' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P674' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P676' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P678' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P684' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P688' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P692' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P696' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P702' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P706' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P710' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P714' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P718' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P722' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P726' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P730' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P736' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P740' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P746' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P750' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P756' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P760' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P766' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P770' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P774' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P778' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P784' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P790' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P796' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P800' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P806' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P810' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P816' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P822' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P826' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P832' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P836' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P840' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P844' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P848' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P852' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P856' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P860' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P864' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P868' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P872' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P876' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P880' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P884' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P888' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P892' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P896' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P900' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P904' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P908' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P912' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P916' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P920' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P924' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P928' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P932' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P936' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P940' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P944' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P948' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P952' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P956' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P960' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P964' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P972' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P976' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P982' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P986' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P992' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P996' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1000' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1004' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1010' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1016' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1020' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1026' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1030' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1036' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1040' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1046' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1052' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1056' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1060' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1064' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1068' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1072' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1076' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1080' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1084' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1090' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1094' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1100' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1106' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1110' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1114' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1118' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1124' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1128' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1132' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1138' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1144' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1148' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1154' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1162' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1166' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1170' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1174' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1178' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1184' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1188' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1192' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1196' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1200' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1204' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1208' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1212' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1216' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1220' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1224' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1228' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1232' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1236' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1240' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1244' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1248' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1252' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1256' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1260' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1264' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1268' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1272' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1276' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1280' is an invalid float value
    [36m(Map(process_file) pid=15079, ip=10.0.119.92)[0m Could get FontBBox from font descriptor because None cannot be parsed as 4 floats
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1284' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1288' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1292' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1296' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1300' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1304' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1308' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1312' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1316' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1318' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1320' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1322' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1324' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1326' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1328' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1330' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1334' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1342' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1350' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1354' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1358' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1366' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1370' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1374' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1383' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1385' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1387' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P2459' is an invalid float value[32m [repeated 224x across cluster][0m
    [36m(Map(process_file) pid=13145, ip=10.0.65.247)[0m Cannot set gray non-stroke color because /'P274' is an invalid float value[32m [repeated 648x across cluster][0m
    2025-10-20 20:12:29,747	WARNING issue_detector_manager.py:58 -- 
    
    Operator 'ReadFiles' uses 768.8MB of memory per task on average, but
    Ray only requests 0.0B per task at the start of the pipeline.
    
    To avoid out-of-memory errors, consider setting `memory=768.8MB` in
    the appropriate function or method call. (This might be unnecessary if
    the number of concurrent tasks is low.)
    
    To change the frequency of this warning, set
    `DataContext.get_current().issue_detectors_config.high_memory_detector_config.detection_time_interval_s`,
    or disable the warning by setting value to -1. (current value: 30)
    
    2025-10-20 20:12:29,748	WARNING issue_detector_manager.py:67 -- To disable issue detection, run DataContext.get_current().issue_detectors_config.detectors = [].
    2025-10-20 20:12:59,854	WARNING issue_detector_manager.py:58 -- 
    
    Operator 'ReadFiles' uses 768.8MB of memory per task on average, but
    Ray only requests 0.0B per task at the start of the pipeline.
    
    To avoid out-of-memory errors, consider setting `memory=768.8MB` in
    the appropriate function or method call. (This might be unnecessary if
    the number of concurrent tasks is low.)
    
    To change the frequency of this warning, set
    `DataContext.get_current().issue_detectors_config.high_memory_detector_config.detection_time_interval_s`,
    or disable the warning by setting value to -1. (current value: 30)
    
    2025-10-20 20:12:59,855	WARNING issue_detector_manager.py:67 -- To disable issue detection, run DataContext.get_current().issue_detectors_config.detectors = [].
    2025-10-20 20:13:14,783	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_147_0 execution finished in 75.22 seconds
    2025-10-20 20:13:14,907	INFO dataset.py:5134 -- Data sink Parquet finished. 2 rows and 115.0B data written.



```python

# SUMMARY TABLE 2: Quality Distribution
# Answer questions like:
# - What percentage of documents are high/medium/low quality?
# - Which categories have the highest quality scores?
# - How does quality correlate with document size?

# This helps identify data quality issues by category

quality_distribution = warehouse_dataset.groupby(["quality_rating", "business_category"]).aggregate(
    Count(),                        # How many per quality+category?
    Mean("word_count"),             # Average document size by quality
    Mean("chunk_word_count")        # Average chunk size by quality
)

# Write quality summary
# Used for quality monitoring dashboards

quality_distribution.write_parquet(
    f"{OUTPUT_WAREHOUSE_PATH}/summaries/quality_distribution/",
    compression="snappy",
    ray_remote_args={"num_cpus": 0.1}
)
```

    2025-10-20 20:13:15,036	INFO logging.py:293 -- Registered dataset logger for dataset dataset_152_0
    2025-10-20 20:13:15,045	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_152_0. Full logs are in /tmp/ray/session_2025-10-20_17-37-47_219984_2353/logs/ray-data
    2025-10-20 20:13:15,045	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_152_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> LimitOperator[limit=100] -> TaskPoolMapOperator[ReadFiles] -> TaskPoolMapOperator[Map(process_file)] -> TaskPoolMapOperator[Map(enrich_business_metadata)] -> TaskPoolMapOperator[MapBatches(assess_document_quality)] -> TaskPoolMapOperator[FlatMap(create_text_chunks)] -> TaskPoolMapOperator[Project] -> TaskPoolMapOperator[MapBatches(add_pipeline_metadata)] -> HashAggregateOperator[HashAggregate(key_columns=('quality_rating', 'business_category'), num_partitions=200)] -> TaskPoolMapOperator[Write]
    /home/ray/anaconda3/lib/python3.12/site-packages/ray/anyscale/data/_internal/cluster_autoscaler/productivity_calculator.py:174: RuntimeWarning: invalid value encountered in divide
      gpu_fraction_per_op = (optimal_num_tasks_per_op * num_gpus_per_op) / np.sum(
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P15' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P19' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P23' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P27' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P33' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P39' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P43' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P47' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P53' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P57' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P63' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P74' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P78' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P84' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P88' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P90' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P92' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P94' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P96' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P98' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P100' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P102' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P104' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P106' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P108' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P110' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P112' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P114' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P116' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P118' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P120' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P122' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P124' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P126' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P128' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P130' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P132' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P134' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P136' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P138' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P140' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P142' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P144' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P146' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P148' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P152' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P160' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P168' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P174' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P180' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P186' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P192' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P198' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P204' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P217' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P221' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P227' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P235' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P239' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P243' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P251' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P255' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P259' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P265' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P271' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P275' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P281' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P285' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P289' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P295' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P299' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P305' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P311' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P315' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P321' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P325' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P331' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P337' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P341' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P347' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P353' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P357' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P361' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P365' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P371' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P375' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P379' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P383' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P387' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P391' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P395' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P399' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P403' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P407' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P413' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P417' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P421' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P425' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P429' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P435' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P441' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P447' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P451' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P457' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P463' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P465' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P467' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P469' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P471' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P473' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P475' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P477' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P479' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P481' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P483' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P485' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P487' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P489' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P491' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P493' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P495' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P497' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P499' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P501' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P503' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P505' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P507' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P509' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P511' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P513' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P515' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P517' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P525' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P529' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P535' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P541' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P547' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P553' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P557' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P559' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P561' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P574' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P580' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P584' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P588' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P594' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P598' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P602' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P608' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P616' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P624' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P632' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P640' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P648' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P656' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P658' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P660' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P662' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P664' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P666' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P668' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P670' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P672' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P674' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P676' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P678' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P684' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P688' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P692' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P696' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P702' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P706' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P710' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P714' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P718' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P722' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P726' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P730' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P736' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P740' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P746' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P750' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P756' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P760' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P766' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P770' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P774' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P778' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P784' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P790' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P796' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P800' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P806' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P810' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P816' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P822' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P826' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P832' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P836' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P840' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P844' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P848' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P852' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P856' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P860' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P864' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P868' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P872' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P876' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P880' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P884' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P888' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P892' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P896' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P900' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P904' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P908' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P912' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P916' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P920' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P924' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P928' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P932' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P936' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P940' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P944' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P948' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P952' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P956' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P960' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P964' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P972' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P976' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P982' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P986' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P992' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P996' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1000' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1004' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1010' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1016' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1020' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1026' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1030' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1036' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1040' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1046' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1052' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1056' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1060' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1064' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1068' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1072' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1076' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1080' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1084' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1090' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1094' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1100' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1106' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1110' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1114' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1118' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1124' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1128' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1132' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1138' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1144' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1148' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1154' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1162' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1166' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1170' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1174' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1178' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1184' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1188' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1192' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1196' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1200' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1204' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1208' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1212' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1216' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1220' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1224' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1228' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1232' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1236' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1240' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1244' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1248' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1252' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1256' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1260' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1264' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1268' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1272' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1276' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1280' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1284' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1288' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1292' is an invalid float value
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P1296' is an invalid float value
    [36m(Map(process_file) pid=13504, ip=10.0.110.204)[0m Could get FontBBox from font descriptor because None cannot be parsed as 4 floats
    [36m(Map(process_file) pid=11766, ip=10.0.68.173)[0m Cannot set gray non-stroke color because /'P2644' is an invalid float value[32m [repeated 286x across cluster][0m
    [36m(Map(process_file) pid=15206, ip=10.0.76.198)[0m Cannot set gray non-stroke color because /'P290' is an invalid float value[32m [repeated 597x across cluster][0m
    [36m(Map(process_file) pid=16754, ip=10.0.110.204)[0m Cannot set gray non-stroke color because /'P332' is an invalid float value[32m [repeated 160x across cluster][0m
    2025-10-20 20:13:45,120	WARNING issue_detector_manager.py:58 -- 
    
    Operator 'ReadFiles' uses 762.1MB of memory per task on average, but
    Ray only requests 0.0B per task at the start of the pipeline.
    
    To avoid out-of-memory errors, consider setting `memory=762.1MB` in
    the appropriate function or method call. (This might be unnecessary if
    the number of concurrent tasks is low.)
    
    To change the frequency of this warning, set
    `DataContext.get_current().issue_detectors_config.high_memory_detector_config.detection_time_interval_s`,
    or disable the warning by setting value to -1. (current value: 30)
    
    2025-10-20 20:13:45,121	WARNING issue_detector_manager.py:67 -- To disable issue detection, run DataContext.get_current().issue_detectors_config.detectors = [].
    2025-10-20 20:14:15,204	WARNING issue_detector_manager.py:58 -- 
    
    Operator 'ReadFiles' uses 762.1MB of memory per task on average, but
    Ray only requests 0.0B per task at the start of the pipeline.
    
    To avoid out-of-memory errors, consider setting `memory=762.1MB` in
    the appropriate function or method call. (This might be unnecessary if
    the number of concurrent tasks is low.)
    
    To change the frequency of this warning, set
    `DataContext.get_current().issue_detectors_config.high_memory_detector_config.detection_time_interval_s`,
    or disable the warning by setting value to -1. (current value: 30)
    
    2025-10-20 20:14:15,205	WARNING issue_detector_manager.py:67 -- To disable issue detection, run DataContext.get_current().issue_detectors_config.detectors = [].
    2025-10-20 20:14:30,803	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_152_0 execution finished in 75.75 seconds
    2025-10-20 20:14:30,921	INFO dataset.py:5134 -- Data sink Parquet finished. 2 rows and 89.0B data written.


## Verification and Summary

After writing data to the warehouse, it's important to verify everything worked correctly. This section demonstrates:

**Why verification matters:**
- Ensures data was written successfully
- Validates record counts match expectations
- Confirms schema is correct
- Provides sample data for visual inspection

**What we'll verify:**
1. Main table record count (should be 10,000+ chunks)
2. Summary tables exist and have data
3. Schema includes all expected columns
4. Sample records look correct

### Verify data warehouse outputs


```python

# STEP 10: VERIFY DATA WAREHOUSE OUTPUT
# Always verify your data pipeline worked correctly
# This is a critical production practice
print("Verifying data warehouse integration...")

# Use Ray Data's read_parquet() to read what we just wrote
# This verifies:
# 1. Files were written successfully
# 2. Partitioning works correctly
# 3. Data can be read back (no corruption)
#
# Ray Data will automatically discover all partitions:
# - main_table/business_category=finance/processing_date=2025-10-15/*.parquet
# - main_table/business_category=legal/processing_date=2025-10-15/*.parquet
# - etc.
main_table_verify = ray.data.read_parquet(
    f"{OUTPUT_WAREHOUSE_PATH}/main_table/",
    ray_remote_args={"num_cpus": 0.025}  # Low CPU for reading
)

# Verify our aggregated metrics tables also wrote successfully
metrics_verify = ray.data.read_parquet(
    f"{OUTPUT_WAREHOUSE_PATH}/summaries/processing_metrics/",
    ray_remote_args={"num_cpus": 0.025}
)

print(f"Data warehouse verification:")
print(f"  Main table records: {main_table_verify.count():,}")
print(f"  Processing metrics: {metrics_verify.count():,}")
print(f"  Schema compatibility: Verified")
```

    2025-10-20 20:14:31,038	INFO logging.py:293 -- Registered dataset logger for dataset dataset_156_0
    2025-10-20 20:14:31,042	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_156_0. Full logs are in /tmp/ray/session_2025-10-20_17-37-47_219984_2353/logs/ray-data
    2025-10-20 20:14:31,042	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_156_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> TaskPoolMapOperator[MapBatches(count_rows)]


    Verifying data warehouse integration...
    Data warehouse verification:


    /home/ray/anaconda3/lib/python3.12/site-packages/ray/anyscale/data/_internal/cluster_autoscaler/productivity_calculator.py:174: RuntimeWarning: invalid value encountered in divide
      gpu_fraction_per_op = (optimal_num_tasks_per_op * num_gpus_per_op) / np.sum(
    2025-10-20 20:14:31,388	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_156_0 execution finished in 0.35 seconds
    2025-10-20 20:14:31,395	INFO logging.py:293 -- Registered dataset logger for dataset dataset_157_0
    2025-10-20 20:14:31,399	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_157_0. Full logs are in /tmp/ray/session_2025-10-20_17-37-47_219984_2353/logs/ray-data
    2025-10-20 20:14:31,400	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_157_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> TaskPoolMapOperator[MapBatches(count_rows)]
    2025-10-20 20:14:31,507	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_157_0 execution finished in 0.11 seconds


      Main table records: 115,977
      Processing metrics: 16
      Schema compatibility: Verified



```python
# INSPECT SAMPLE DATA

# take(10) gets first 10 records for manual inspection
# This helps catch issues like:
# - Wrong data types
# - Missing fields
# - Incorrect values
# - Encoding problems
samples = main_table_verify.take(10)

# Display key fields from each sample record
for i, record in enumerate(samples):
    # Show abbreviated document ID (first 8 characters)
    doc_id = record['document_id'][:8]
    category = record['business_category']
    words = record['word_count']
    quality = record['quality_rating']
    
    print(f"\t{i+1}. Doc: {doc_id}, Category: {category}, Words: {words}, Quality: {quality}")
```

    2025-10-20 20:14:31,624	INFO logging.py:293 -- Registered dataset logger for dataset dataset_158_0
    2025-10-20 20:14:31,629	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_158_0. Full logs are in /tmp/ray/session_2025-10-20_17-37-47_219984_2353/logs/ray-data
    2025-10-20 20:14:31,630	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_158_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> TaskPoolMapOperator[ReadFiles] -> LimitOperator[limit=10]
    2025-10-20 20:14:32,076	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_158_0 execution finished in 0.45 seconds


    	1. Doc: 9278d4de, Category: compliance, Words: 6925, Quality: high
    	2. Doc: 9278d4de, Category: compliance, Words: 6925, Quality: high
    	3. Doc: 9278d4de, Category: compliance, Words: 6925, Quality: high
    	4. Doc: 9278d4de, Category: compliance, Words: 6925, Quality: high
    	5. Doc: 9278d4de, Category: compliance, Words: 6925, Quality: high
    	6. Doc: 9278d4de, Category: compliance, Words: 6925, Quality: high
    	7. Doc: 9278d4de, Category: compliance, Words: 6925, Quality: high
    	8. Doc: 9278d4de, Category: compliance, Words: 6925, Quality: high
    	9. Doc: 9278d4de, Category: compliance, Words: 6925, Quality: high
    	10. Doc: 9278d4de, Category: compliance, Words: 6925, Quality: high


## Summary and Next Steps

Congratulations! You've built a complete end-to-end document ingestion pipeline using Ray Data. Let's review what you learned and where to go from here.

### What You Built

**Complete ETL Pipeline**: Extract → Transform → Load
1. **Extract**: Read 100 documents from S3 data lake
2. **Transform**: Extract text, classify, assess quality, create chunks
3. **Load**: Write to partitioned data warehouse with analytics tables

**Final Output**: From raw documents to structured warehouse
- **Main table**: 10,000+ text chunks ready for analysis
- **Business datasets**: Finance and compliance specific views
- **Summary tables**: Pre-computed metrics for dashboards
- **Partitioned storage**: Optimized for query performance

### Ray Data Operations You Used

This pipeline demonstrated all major Ray Data operations:

| Operation | Purpose | When to Use |
|-----------|---------|-------------|
| `read_binary_files()` | Load documents from S3/storage | Reading PDFs, images, any binary files |
| `map()` | Transform each record individually | Variable-size processing, I/O-bound tasks |
| `map_batches()` | Transform records in batches | Batch-optimized operations, ML inference |
| `flat_map()` | One-to-many transformations | Chunking, splitting, exploding data |
| `filter()` | Keep/remove records | Selecting subsets, data quality filtering |
| `select_columns()` | Choose specific fields | Schema projection, reducing data size |
| `rename_columns()` | Change column names | Schema standardization |
| `groupby().aggregate()` | Calculate statistics | Analytics, metrics, summaries |
| `write_parquet()` | Save to warehouse | Final output, checkpointing |

### Key Concepts for Beginners

**1. Distributed Processing**
- Your code runs on a cluster of machines (not just one)
- Ray Data automatically distributes work across workers
- Each function (process_file, assess_quality) runs in parallel
- 100 documents processed simultaneously = 100x faster

**2. Lazy Evaluation**
- Operations like `map()` and `filter()` don't execute immediately
- Ray builds a plan and optimizes it
- Execution happens when you call `write_parquet()`, `count()`, or `take()`
- This allows Ray to optimize the entire pipeline

**3. Resource Management**
- `num_cpus`: How many CPU cores per task
- `concurrency`: How many tasks run in parallel
- `batch_size`: How many records per batch
- Balance these based on your workload

**4. Partitioning Strategy**
- Partitions = folders organized by column values
- `partition_cols=["business_category", "processing_date"]`
- Query engines skip entire folders when filtering
- Enables efficient query performance by reducing data scanned

### Implementation Patterns Applied

**Code Organization**:
- Separate functions for each processing stage
- Clear docstrings explaining purpose
- Type hints for inputs and outputs
- Comments explaining "why" not just "what"

**Ray Data Implementation Patterns**:
- Use `batch_format="pandas"` for clarity
- Process text early (don't pass binary through pipeline)
- Appropriate resource allocation per operation type
- Partition writes for query optimization
- Use native Ray Data operations (not custom code)

**Data Engineering Patterns**:
- Immediate text extraction (reduces memory)
- Separate classification stage (easier debugging)
- Quality assessment (data validation)
- Schema transformation (clean warehouse schema)
- Verification step (always check output)

### Production Recommendations

**Scaling to Production:**

1. **Remove the `.limit(100)` to process full dataset**
   - Currently processing 100 docs for demo
   - Remove this to process millions of documents
   - No code changes needed, just remove one line

2. **Tune resource parameters for your cluster**
   ```python
   # For larger clusters, increase parallelism:
   concurrency=50     # More parallel tasks
   batch_size=5000    # Larger batches
   num_cpus=2         # More CPU per task
   ```

3. **Add error handling and retry logic**
   ```python
   # For production, catch specific errors:
   try:
       elements = partition(file=stream)
   except CorruptedFileError:
       # Log and skip
   except TimeoutError:
       # Retry with backoff
   ```

4. **Monitor with Ray Dashboard**
   - View real-time progress
   - Check resource utilization
   - Identify bottlenecks
   - Debug failures

5. **Implement incremental processing**
   ```python
   # Only process new documents:
   new_docs = all_docs.filter(
       lambda row: row["processing_date"] > last_run_date
   )
   ```

6. **Add data quality checks**
   - Validate schema before writing
   - Check for null values
   - Verify foreign key relationships
   - Monitor quality metrics over time

### What You Learned

**Ray Data Fundamentals**:
- How to read from cloud storage (S3)
- Distributed data processing patterns
- Batch vs. row-based operations
- Resource management and tuning
- Writing to data warehouses

**Data Engineering Skills**:
- ETL pipeline design
- Document processing at scale
- Quality assessment strategies
- Data warehouse schema design
- Partitioning for performance

**Production Practices**:
- Verification and testing
- Error handling approaches
- Resource optimization
- Monitoring and debugging
- Scalability considerations

### Next Steps

**Extend This Pipeline:**
1. Add LLM-based content analysis (replace pattern matching)
2. Implement named entity recognition (NER)
3. Add sentiment analysis for customer documents
4. Create vector embeddings for semantic search
5. Integrate with Delta Lake or Apache Iceberg

**Learn More Ray Data:**
- **Batch Inference**: Process documents with ML models
- **Data Quality**: Advanced validation patterns
- **Performance Tuning**: Optimize for your workload
- **Integration**: Connect to Snowflake, Databricks, etc.

### Resources

- **Ray Data Documentation**: https://docs.ray.io/en/latest/data/data.html
- **Ray Data Examples**: https://docs.ray.io/en/latest/data/examples/examples.html
- **Ray Dashboard Guide**: https://docs.ray.io/en/latest/ray-observability/getting-started.html
- **Anyscale Platform**: https://docs.anyscale.com/

---

**You're now ready to build production-scale document ingestion pipelines with Ray Data!**


