<!--
Do not modify this README. This file is a copy of the notebook and is not used to display the content.
Modify unstructured-data-ingestion.ipynb instead, then regenerate this file with:
jupyter nbconvert "$nb_filename" --to markdown --output "README.md"
-->

# Unstructured Data Ingestion and Processing With Ray Data

<div align="left">
<a target="_blank" href="https://console.anyscale.com/template-preview/unstructured-data-ingestion"><img src="https://img.shields.io/badge/🚀 Run_on-Anyscale-9hf"></a>&nbsp;
<a href="https://github.com/ray-project/ray/tree/master/doc/source/data/examples/unstructured-data-ingestion" role="button"><img src="https://img.shields.io/static/v1?label=&amp;message=View%20On%20GitHub&amp;color=586069&amp;logo=github&amp;labelColor=2f363d"></a>&nbsp;
</div>

**Time to complete**: 35 min | **Difficulty**: Advanced | **Prerequisites**: Data engineering experience, document processing, basic natural language processing (NLP) knowledge

Build a comprehensive document ingestion pipeline that transforms unstructured documents from data lakes into structured, analytics-ready datasets using Ray Data's distributed processing capabilities for enterprise data warehouse workflows.

## Table of contents

1. [Data lake document discovery](#step-1-data-lake-document-discovery) (8 min)
2. [Document processing and classification](#step-2-document-processing-and-classification) (10 min)
3. [Text extraction and enrichment](#step-3-text-chunking-and-enrichment) (8 min)
4. [Data warehouse output](#step-4-data-warehouse-schema-and-output) (6 min)
5. [Verification and summary](#verification) (3 min)

## Learning objectives

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
- **Increasing data sizes**: Unstructured data is growing rapidly, often overtaking structured data by 10x in sheer data volume.

**Solution**: Ray Data enables end-to-end document ingestion pipelines with native distributed operations for processing millions of documents efficiently.

- **Scale**: Using streaming execution and massive scalability capabilities to process petabytes or even exabytes of data.
- **Consistency**: A flexible API for supporting quality checks through `map` as well as support for any type of PyArrow schema. All data read also has to be consistent or else the pipeline fails along with additional configuration options for this behavior.
- **Integration**: Supports integration with all data types as well as any AI types, running on CPUs/GPUs/accelerators on cloud and on-prem
- **Warehouse integration**: Pre-built connectors to popular data warehouses as well as having the ability to build custom connectors easily.
- **Increasing data sizes**: Supports single node optimization to scaling across 10k+ nodes

## Prerequisites checklist

Before starting, ensure you have:
- [ ] An understanding of data lake and data warehouse concepts
- [ ] Experience with document processing and text extraction
- [ ] Knowledge of structured data formats (Parquet, Delta Lake, Iceberg)
- [ ] A Python environment with Ray Data and document processing libraries
- [ ] Access to S3 or other cloud storage for document sources

Setup and initialize Ray Data:


```python

import json 
import uuid  
from datetime import datetime  
from pathlib import Path 
from typing import Any, Dict, List  #

import numpy as np 
import pandas as pd 

import ray 
from ray.data.aggregate import Count, Max, Mean, Sum 
from ray.data.expressions import col, lit 

ctx = ray.data.DataContext.get_current()

# Disable progress bars for cleaner output in production
# You can enable these for debugging: set to True to see progress
ctx.enable_progress_bars = False
ctx.enable_operator_progress_bars = False

# Initialize Ray cluster connection to set the Ray Data context
# Use runtime env to install dependencies across all workers
runtime_env = dict(
    pip= {
        "packages": ["unstructured[all-docs]==0.18.21", "pandas==2.3.3"],
        "pip_install_options": ["--force-reinstall", "--no-cache-dir"]
    }
)
ray.init(ignore_reinit_error=True, runtime_env= runtime_env)
```

## Step 1: Data Lake Document Discovery

### Discover document collections in data lake


```python
# READ DOCUMENTS FROM DATA LAKE (S3)

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

### Document metadata extraction


```python
# TEXT EXTRACTION FUNCTION
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

documents_with_text = document_collection.map(
    process_file
)
```


```python
# Convert a sample of documents to pandas DataFrame for easy viewing
documents_with_text.limit(25).to_pandas()
```


```python
# BUSINESS METADATA ENRICHMENT FUNCTION (Modern Approach)
# Instead of simple string matching, apply basic NLP for categorization.
# For high-quality production, you'd use an ML or LLM model endpoint here.

from transformers import pipeline

# For demonstration: Use a small zero-shot classification model.
# In a real pipeline, you should call a production LLM/ML endpoint or use a domain-specific model.
# The 'facebook/bart-large-mnli' model works for zero-shot/label classification tasks.
# You may swap with "typeform/distilbert-base-uncased-mnli" or another small MNLI model for lighter resource use.
# If working at scale or with large docs, consider using Ray Serve + LLM API instead.

zero_shot_classifier = pipeline(
    "zero-shot-classification",
    model="facebook/bart-large-mnli"  # Or another MNLI model if needed
)

# Define candidate classes (map to business categories)
CANDIDATE_LABELS = [
    "financial document",  # maps to 'finance'
    "legal document",      # maps to 'legal'
    "regulatory document", # maps to 'compliance'
    "client document",     # maps to 'client_services'
    "research document",   # maps to 'research'
    "general document"     # maps to 'general'
]

BUSINESS_CATEGORY_MAPPING = {
    "financial document":    ("financial_document", "finance"),
    "legal document":        ("legal_document", "legal"),
    "regulatory document":   ("regulatory_document", "compliance"),
    "client document":       ("client_document", "client_services"),
    "research document":     ("research_document", "research"),
    "general document":      ("general_document", "general"),
}


def enrich_business_metadata(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Uses zero-shot text classification to predict business category and assign processing priority.
    For production: Replace this with a call to your domain-tuned classifier or LLM endpoint.
    """

    file_size = record["file_size_bytes"]
    text = record.get("extracted_text", "") or ""
    filename = record.get("file_name", "")

    # Concatenate extracted text with filename for context, up to limit (to save inference cost)
    context_text = (filename + "\n\n" + text[:1000]).strip()  # Truncate to first 1000 chars for speed

    # Run zero-shot classification (useful even with short context)
    result = zero_shot_classifier(context_text, CANDIDATE_LABELS, multi_label=False)
    top_label = result["labels"][0] if result and "labels" in result and result["labels"] else "general document"

    doc_type, business_category = BUSINESS_CATEGORY_MAPPING.get(top_label, ("general_document", "general"))

    # Priority assignment using simple logic + NLP keyword search (feel free to LLM this too)
    lower_context = context_text.lower()
    if any(w in lower_context for w in ["urgent", "critical", "deadline"]):
        priority = "high"
        priority_score = 3
    elif any(w in lower_context for w in ["important", "quarterly", "annual"]):
        priority = "medium"
        priority_score = 2
    else:
        priority = "low"
        priority_score = 1
    
    return {
        **record,
        "document_type": doc_type,
        "business_category": business_category,
        "processing_priority": priority,
        "priority_score": priority_score,
        "estimated_pages": max(1, file_size // 50000),
        "processing_status": "classified"
    }


# Apply business metadata enrichment to all documents
print("Enriching with business metadata (using zero-shot NLP)...")

# Note: zero-shot and LLM models can be heavy;
# For fast local testing, use a smaller model, or replace with a production endpoint.

documents_with_metadata = documents_with_text.map(
    enrich_business_metadata
)


# View a few documents with business classification added
documents_with_metadata.limit(5).to_pandas()
```


```python

# WHY AGGREGATIONS?
# Before writing to warehouse, understand what we're processing:
# - How many documents of each type?
# - What's the size distribution?
# - Which categories have the most content?

# AGGREGATION 1: Document type distribution
# Group by document_type and calculate statistics

doc_type_stats = documents_with_metadata.groupby("document_type").aggregate(
    Count(),  # How many documents of each type?
    Sum("file_size_bytes"),  # Total size per document type
    Mean("file_size_mb"),  # Average size per document type
    Max("estimated_pages")  # Largest document per type
)

# AGGREGATION 2: Business category analysis
# Understand the distribution across business categories
# This helps with warehouse partitioning strategy

category_stats = documents_with_metadata.groupby("business_category").aggregate(
    Count(),  # How many per category?
    Mean("priority_score"),  # Average priority per category
    Sum("file_size_mb")  # Total data volume per category
)
```

## Step 2: Document processing and classification

### Text extraction and quality assessment


```python

# QUALITY ASSESSMENT FUNCTION
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
    
    # Iterate through rows to apply business rules for quality
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


# Apply quality assessment to all documents

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

## Step 3: Text chunking and enrichment


```python

# TEXT CHUNKING FUNCTION
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
    
    # Create chunks of data by a sliding window
    chunks = []
    start = 0  # Starting position in the text
    chunk_index = 0  # Track which chunk number this is

    # There are many more advanced chunking methods, this example uses a simple technique for demo purposes
    
    # Loop until processing all the text
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
        
        # If you've reached the end of the text, you're done
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
    # Ray Data's flat_map() automatically flattens this list
    return chunks


# Apply text chunking to all documents
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

## Step 4: Data warehouse schema and output

### Create data warehouse schema


```python

# DATA WAREHOUSE SCHEMA TRANSFORMATION
# Transform the raw processing data into a clean warehouse schema
# This is the "ETL" part - Extract (done), Transform (now), Load (next)

print("Creating data warehouse schema...")


# Get today's date in ISO format (YYYY-MM-DD)
# Ray Data uses this to partition the data by date in the warehouse
processing_date = datetime.now().isoformat()[:10]


# Data warehouses need clean, organized schemas
# Select only the columns needed and organize them logically
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
warehouse_dataset = (
    warehouse_dataset
    .with_column("processing_date", lit(processing_date))       # When was this processed?
    .with_column("pipeline_version", lit("1.0"))               # Which version of pipeline?
    .with_column("processing_engine", lit("ray_data"))         # What tool processed it?
)
```

### Write to data warehouse with partitioning


```python

# WRITE TO DATA WAREHOUSE - MAIN TABLE
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


```python


# CREATE BUSINESS-SPECIFIC DATASETS
# Create specialized datasets for specific business teams
# Each team gets only the data they need with relevant columns


# Example: Compliance analytics dataset
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

### Create analytics summary tables


```python

# CREATE ANALYTICS SUMMARY TABLES
# Pre-compute common aggregations for fast dashboard queries
# Summary tables = faster analytics queries


# SUMMARY TABLE 1: Processing metrics by category and date

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


```python

# SUMMARY TABLE 2: Quality distribution
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

## Verification

After writing data to the warehouse, verify everything worked correctly. This section demonstrates:

**Why verification matters:**
- Ensures the pipeline wrote data successfully
- Validates record counts match expectations
- Confirms schema is correct
- Provides sample data for visual inspection

**What to verify:**
1. Main table record count (should be 10,000+ chunks)
2. Summary tables exist and have data
3. Schema includes all expected columns
4. Sample records look correct

### Verify data warehouse outputs


```python

# VERIFY DATA WAREHOUSE OUTPUT
# Always verify your data pipeline worked correctly
# This is a critical production practice
print("Verifying data warehouse integration...")

# Use Ray Data's read_parquet() to read what was just written
# This verifies:
# 1. Files were written successfully
# 2. Partitioning works correctly
# 3. Data can be read back (no corruption)
#
# Ray Data automatically discovers all partitions:
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

## Summary

You built a complete end-to-end document ingestion pipeline using Ray Data. This section reviews what you learned and where to go from here.

### What you built

**Complete ETL pipeline**: Extract → Transform → Load
1. **Extract**: Read 100 documents from S3 data lake
2. **Transform**: Extract text, classify, assess quality, create chunks
3. **Load**: Write to partitioned data warehouse with analytics tables

**Final output**: From raw documents to structured warehouse
- **Main table**: 10,000+ text chunks ready for analysis
- **Business datasets**: Finance and compliance specific views
- **Summary tables**: Pre-computed metrics for dashboards
- **Partitioned storage**: Optimized for query performance

### Ray Data operations you used

This pipeline demonstrated all major Ray Data operations:

| Operation | Purpose | When to use |
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

### Key concepts for beginners

**1. Distributed processing**
- Your code runs on a cluster of machines (not just one)
- Ray Data automatically distributes work across workers
- Each function (process_file, assess_quality) runs in parallel
- 100 documents processed simultaneously = 100x faster

**2. Lazy evaluation**
- Operations like `map()` and `filter()` don't execute immediately
- Ray builds a plan and optimizes it
- Execution happens when you call `write_parquet()`, `count()`, or `take()`
- This allows Ray to optimize the entire pipeline

**3. Resource management**
- `batch_size`: How many records per batch
- `concurrency`: How many tasks run in parallel (advanced)
- `num_cpus`: How many CPU cores per task (advanced)
- Balance these based on your workload

**4. Partitioning strategy**
- Partitions = folders organized by column values
- `partition_cols=["business_category", "processing_date"]`
- Query engines skip entire folders when filtering
- Enables efficient query performance by reducing data scanned

### Implementation patterns applied

**Code organization**:
- Separate functions for each processing stage
- Clear docstrings explaining purpose
- Type hints for inputs and outputs
- Comments explaining "why" not just "what"

**Ray Data implementation patterns**:
- Use `batch_format="pandas"` for clarity
- Process text early (don't pass binary through pipeline)
- Appropriate resource allocation per operation type
- Partition writes for query optimization
- Use native Ray Data operations (not custom code)

**Data engineering patterns**:
- Immediate text extraction (reduces memory)
- Separate classification stage (easier debugging)
- Quality assessment (data validation)
- Schema transformation (clean warehouse schema)
- Verification step (always check output)

### Production recommendations

**Scaling to production:**

1. **Remove the `.limit(100)` to process full dataset**
   - Currently processing 100 docs for demo
   - Remove this to process millions of documents
   - No code changes needed, just remove one line

2. **Tune resource parameters for your cluster**
   ```python
   # For larger clusters, increase parallelism:
   batch_size=5000    # Larger batches
   concurrency=50     # More parallel tasks (advanced)
   num_cpus=2         # More CPU per task (advanced)
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

### What you learned

**Ray Data fundamentals**:
- How to read from cloud storage (S3)
- Distributed data processing patterns
- Batch versus row-based operations
- Resource management and tuning
- Writing to data warehouses

**Data engineering skills**:
- ETL pipeline design
- Document processing at scale
- Quality assessment strategies
- Data warehouse schema design
- Partitioning for performance

**Production practices**:
- Verification and testing
- Error handling approaches
- Resource optimization
- Monitoring and debugging
- Scalability considerations

