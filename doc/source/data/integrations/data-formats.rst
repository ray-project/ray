.. _data-formats:

Data Formats Integration
========================

Ray Data provides native support for a wide range of data formats, enabling you to work with diverse data sources without custom connectors. This guide shows you how to efficiently read, process, and write data in various formats commonly used in enterprise environments.

**What you'll learn:**
* Work with structured data formats (CSV, Parquet, ORC, Avro)
* Process semi-structured data (JSON, XML, log files)
* Handle unstructured data (images, audio, video, documents)
* Integrate with enterprise data formats and legacy systems
* Optimize performance for different format characteristics

**Why data format integration matters:**
* **Universal compatibility**: Work with any data format your organization uses
* **Performance optimization**: Choose the right format for your workload
* **Cost efficiency**: Avoid expensive format conversion processes
* **Enterprise integration**: Connect with legacy and modern data systems
* **Future-proofing**: Handle emerging data formats and standards

What is data format integration?
--------------------------------

Data format integration enables Ray Data to work seamlessly with diverse data formats, providing:

* **Native format support**: Direct reading and writing without conversion
* **Performance optimization**: Format-specific optimizations for different workloads
* **Schema handling**: Automatic schema inference and management
* **Compression support**: Built-in compression and decompression
* **Enterprise compatibility**: Support for legacy and modern data formats

Why use Ray Data for different data formats?
--------------------------------------------

**Business Benefits:**
* **Reduced complexity**: Single platform for all data format needs
* **Cost savings**: Eliminate expensive format conversion tools
* **Performance gains**: Optimized processing for each format type
* **Simplified architecture**: Consolidate multiple format-specific tools
* **Future readiness**: Handle emerging data formats and standards

**Technical Benefits:**
* **Native performance**: Direct format support without intermediate conversions
* **Memory efficiency**: Streaming processing for large format files
* **Parallel processing**: Distributed format handling across cluster nodes
* **Schema evolution**: Handle format and schema changes gracefully
* **Error handling**: Robust processing with format-specific error recovery

How to work with different data formats
---------------------------------------

Follow this approach for successful data format integration:

1. **Assess format requirements**: Identify data formats and performance needs
2. **Choose appropriate formats**: Select formats optimized for your workload
3. **Configure processing**: Set up format-specific processing parameters
4. **Optimize performance**: Use format-specific optimization techniques
5. **Handle schema changes**: Implement schema evolution strategies
6. **Monitor performance**: Track format-specific processing metrics

.. _structured-formats:

Structured Data Formats
-----------------------

**What are structured data formats?**

Structured data formats have a fixed, predictable schema with consistent data types and relationships. These formats are ideal for business intelligence, analytics, and data warehousing workloads.

**CSV (Comma-Separated Values)**

CSV files are widely used for data exchange and simple storage. Ray Data provides efficient CSV processing with automatic schema inference.

.. code-block:: python

    import ray

    # Read CSV with automatic schema inference
    csv_data = ray.data.read_csv("s3://bucket/data.csv")

    # Read with custom delimiter
    pipe_data = ray.data.read_csv(
        "s3://bucket/data.txt",
        delimiter="|"
    )

    # Read with schema specification
    schema_csv = ray.data.read_csv(
        "s3://bucket/data.csv",
        schema={
            "customer_id": "int64",
            "name": "string",
            "email": "string",
            "signup_date": "date32"
        }
    )

    # Write to CSV
    processed_data.write_csv("s3://bucket/processed.csv")

**Parquet Format**

Parquet is a columnar storage format optimized for analytics workloads. Ray Data provides native Parquet support with excellent performance.

.. code-block:: python

    import ray

    # Read Parquet files
    parquet_data = ray.data.read_parquet("s3://bucket/data.parquet")

    # Read partitioned Parquet
    partitioned_data = ray.data.read_parquet("s3://bucket/partitioned/")

    # Read with column projection
    projected_data = ray.data.read_parquet(
        "s3://bucket/data.parquet",
        columns=["customer_id", "amount", "date"]
    )

    # Write with partitioning
    processed_data.write_parquet(
        "s3://bucket/processed/",
        partition_cols=["year", "month", "day"]
    )

    # Write with compression
    compressed_data.write_parquet(
        "s3://bucket/compressed/",
        compression="snappy"
    )

**ORC (Optimized Row Columnar)**

ORC is another columnar format optimized for Hadoop workloads. Ray Data provides native ORC support.

.. code-block:: python

    import ray

    # Read ORC files
    orc_data = ray.data.read_orc("s3://bucket/data.orc")

    # Read with predicate pushdown
    filtered_orc = ray.data.read_orc(
        "s3://bucket/data.orc",
        columns=["customer_id", "amount"],
        filters=[("amount", ">", 1000)]
    )

    # Write to ORC
    processed_data.write_orc("s3://bucket/processed.orc")

**Avro Format**

Avro is a row-based format with rich schema support. Ray Data provides native Avro integration.

.. code-block:: python

    import ray

    # Read Avro files
    avro_data = ray.data.read_avro("s3://bucket/data.avro")

    # Read with schema evolution
    evolved_avro = ray.data.read_avro(
        "s3://bucket/data.avro",
        schema_evolution=True
    )

    # Write to Avro
    processed_data.write_avro("s3://bucket/processed.avro")

.. _semi-structured-formats:

Semi-Structured Data Formats
----------------------------

**What are semi-structured data formats?**

Semi-structured data has some organization but flexible schema, making it ideal for API data, log files, and document processing.

**JSON (JavaScript Object Notation)**

JSON is widely used for API responses, configuration files, and document storage. Ray Data provides powerful JSON processing capabilities.

.. code-block:: python

    import ray

    # Read JSON files
    json_data = ray.data.read_json("s3://bucket/data.json")

    # Read JSON lines (one JSON object per line)
    jsonl_data = ray.data.read_json("s3://bucket/data.jsonl")

    # Read with schema specification
    schema_json = ray.data.read_json(
        "s3://bucket/data.json",
        schema={
            "id": "int64",
            "name": "string",
            "metadata": "map<string, string>"
        }
    )

    # Process nested JSON
    def flatten_json(batch):
        """Flatten nested JSON structures"""
        flattened = []
        for row in batch:
            flat_row = {}
            for key, value in row.items():
                if isinstance(value, dict):
                    for nested_key, nested_value in value.items():
                        flat_row[f"{key}_{nested_key}"] = nested_value
                else:
                    flat_row[key] = value
            flattened.append(flat_row)
        return flattened

    flattened_data = json_data.map_batches(flatten_json)

    # Write to JSON
    processed_data.write_json("s3://bucket/processed.json")

**XML Format**

XML is used for document storage and enterprise data exchange. Ray Data provides XML processing capabilities.

.. code-block:: python

    import ray
    import xml.etree.ElementTree as ET

    def parse_xml(batch):
        """Parse XML content"""
        parsed_data = []
        for xml_content in batch:
            try:
                root = ET.fromstring(xml_content)
                # Extract data from XML structure
                parsed_row = {
                    "id": root.find("id").text,
                    "name": root.find("name").text,
                    "value": root.find("value").text
                }
                parsed_data.append(parsed_row)
            except Exception as e:
                # Handle parsing errors
                parsed_data.append({"error": str(e)})
        return parsed_data

    # Read XML files as text
    xml_data = ray.data.read_text("s3://bucket/data.xml")
    
    # Parse XML content
    parsed_xml = xml_data.map_batches(parse_xml)

**Log Files**

Log files contain semi-structured data with varying formats. Ray Data provides flexible log processing capabilities.

.. code-block:: python

    import ray
    import re

    def parse_log_line(batch):
        """Parse log file lines"""
        parsed_logs = []
        for line in batch:
            # Example log format: [2024-01-01 10:00:00] INFO: User login successful
            match = re.match(r'\[(.*?)\] (\w+): (.+)', line)
            if match:
                parsed_logs.append({
                    "timestamp": match.group(1),
                    "level": match.group(2),
                    "message": match.group(3)
                })
            else:
                parsed_logs.append({"raw_line": line})
        return parsed_logs

    # Read log files
    log_data = ray.data.read_text("s3://bucket/application.log")
    
    # Parse log lines
    parsed_logs = log_data.map_batches(parse_log_line)

    # Filter by log level
    error_logs = parsed_logs.filter(lambda row: row["level"] == "ERROR")
    info_logs = parsed_logs.filter(lambda row: row["level"] == "INFO")

.. _unstructured-formats:

Unstructured Data Formats
-------------------------

**What are unstructured data formats?**

Unstructured data lacks predefined structure and includes images, audio, video, and text documents. Ray Data provides native support for these formats with AI/ML integration.

**Image Data**

Ray Data provides native support for image processing with GPU acceleration capabilities.

.. code-block:: python

    import ray

    # Read image datasets
    image_data = ray.data.read_images("s3://bucket/images/")

    # Read with specific format
    jpeg_data = ray.data.read_images(
        "s3://bucket/images/",
        include_paths=True,
        size=(224, 224)  # Resize images
    )

    # Process images with custom functions
    def preprocess_images(batch):
        """Preprocess images for ML models"""
        import cv2
        import numpy as np
        
        processed_images = []
        for image in batch:
            # Convert to grayscale
            gray = cv2.cvtColor(image, cv2.COLOR_RGB2GRAY)
            
            # Normalize pixel values
            normalized = gray.astype(np.float32) / 255.0
            
            processed_images.append(normalized)
        
        return processed_images

    # Apply preprocessing
    processed_images = image_data.map_batches(preprocess_images)

    # Write processed images
    processed_images.write_images("s3://bucket/processed_images/")

**Audio Data**

Ray Data supports audio file processing for speech recognition and audio analysis.

.. code-block:: python

    import ray

    # Read audio files
    audio_data = ray.data.read_audio("s3://bucket/audio/")

    # Read with specific format
    wav_data = ray.data.read_audio(
        "s3://bucket/audio/",
        include_paths=True,
        sample_rate=16000
    )

    # Process audio with custom functions
    def extract_features(batch):
        """Extract audio features"""
        import librosa
        
        features = []
        for audio in batch:
            # Extract MFCC features
            mfcc = librosa.feature.mfcc(y=audio, sr=16000)
            features.append(mfcc)
        
        return features

    # Extract audio features
    audio_features = audio_data.map_batches(extract_features)

**Video Data**

Ray Data supports video processing for computer vision and content analysis.

.. code-block:: python

    import ray

    # Read video files
    video_data = ray.data.read_videos("s3://bucket/videos/")

    # Read with specific parameters
    video_frames = ray.data.read_videos(
        "s3://bucket/videos/",
        include_paths=True,
        fps=30
    )

    # Process video frames
    def extract_frames(batch):
        """Extract key frames from videos"""
        import cv2
        
        frames = []
        for video in batch:
            # Extract every 30th frame (1 frame per second at 30fps)
            for i in range(0, len(video), 30):
                frames.append(video[i])
        
        return frames

    # Extract key frames
    key_frames = video_data.map_batches(extract_frames)

**Text Documents**

Ray Data provides comprehensive text processing capabilities for document analysis.

.. code-block:: python

    import ray

    # Read text files
    text_data = ray.data.read_text("s3://bucket/documents/")

    # Read with encoding specification
    utf8_text = ray.data.read_text(
        "s3://bucket/documents/",
        encoding="utf-8"
    )

    # Process text with custom functions
    def analyze_text(batch):
        """Analyze text content"""
        import re
        
        analysis = []
        for text in batch:
            # Count words
            word_count = len(text.split())
            
            # Count sentences
            sentence_count = len(re.split(r'[.!?]+', text))
            
            # Extract email addresses
            emails = re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', text)
            
            analysis.append({
                "word_count": word_count,
                "sentence_count": sentence_count,
                "email_count": len(emails),
                "emails": emails
            })
        
        return analysis

    # Analyze text content
    text_analysis = text_data.map_batches(analyze_text)

.. _enterprise-formats:

Enterprise Data Formats
-----------------------

**What are enterprise data formats?**

Enterprise data formats are specialized formats used in legacy systems, mainframes, and enterprise applications. Ray Data provides integration capabilities for these formats.

**Fixed-Width Files**

Fixed-width files are common in mainframe and legacy systems. Ray Data can process these with custom parsing logic.

.. code-block:: python

    import ray

    def parse_fixed_width(batch):
        """Parse fixed-width format files"""
        parsed_data = []
        
        # Define field widths (example: ID=10, Name=20, Amount=15)
        field_widths = [10, 20, 15]
        field_names = ["id", "name", "amount"]
        
        for line in batch:
            parsed_row = {}
            start_pos = 0
            
            for i, width in enumerate(field_widths):
                field_value = line[start_pos:start_pos + width].strip()
                parsed_row[field_names[i]] = field_value
                start_pos += width
            
            parsed_data.append(parsed_row)
        
        return parsed_data

    # Read fixed-width files as text
    fixed_width_data = ray.data.read_text("s3://bucket/fixed_width.txt")
    
    # Parse fixed-width format
    parsed_fixed_width = fixed_width_data.map_batches(parse_fixed_width)

**COBOL Copybook Files**

COBOL copybook files define data structures for mainframe systems. Ray Data can process these with custom parsers.

.. code-block:: python

    import ray

    def parse_cobol_copybook(batch):
        """Parse COBOL copybook format"""
        parsed_data = []
        
        for line in batch:
            # Example COBOL copybook line: 01 CUSTOMER-RECORD.
            # Parse COBOL data definitions
            if line.startswith("01 "):
                record_name = line.split()[1].rstrip(".")
                parsed_data.append({"record_type": "record", "name": record_name})
            elif "PIC" in line:
                # Parse picture clauses
                parts = line.split()
                field_name = parts[0]
                pic_clause = parts[parts.index("PIC") + 1]
                parsed_data.append({
                    "record_type": "field",
                    "name": field_name,
                    "picture": pic_clause
                })
        
        return parsed_data

    # Read COBOL copybook files
    copybook_data = ray.data.read_text("s3://bucket/copybook.cpy")
    
    # Parse copybook definitions
    parsed_copybook = copybook_data.map_batches(parse_cobol_copybook)

**EDI (Electronic Data Interchange)**

EDI files are used for business-to-business data exchange. Ray Data can process EDI formats with custom parsers.

.. code-block:: python

    import ray

    def parse_edi_x12(batch):
        """Parse EDI X12 format files"""
        parsed_data = []
        
        for edi_content in batch:
            # Split EDI content into segments
            segments = edi_content.split("~")
            
            for segment in segments:
                if segment.strip():
                    # Split segment into elements
                    elements = segment.split("*")
                    
                    if elements[0] == "ST":  # Transaction Set Header
                        parsed_data.append({
                            "segment": "ST",
                            "transaction_set_id": elements[1],
                            "control_number": elements[2]
                        })
                    elif elements[0] == "N1":  # Name
                        parsed_data.append({
                            "segment": "N1",
                            "entity_identifier": elements[1],
                            "name": elements[2]
                        })
        
        return parsed_data

    # Read EDI files
    edi_data = ray.data.read_text("s3://bucket/edi_data.txt")
    
    # Parse EDI content
    parsed_edi = edi_data.map_batches(parse_edi_x12)

Performance Optimization
------------------------

**Format-Specific Optimization**

Optimize performance for different data formats using format-specific techniques.

.. code-block:: python

    def optimize_format_performance():
        """Optimize performance for different formats"""
        
        # CSV optimization
        csv_data = ray.data.read_csv(
            "s3://bucket/large.csv",
            block_size=100 * 1024 * 1024,  # 100MB blocks
            num_parallel_reads=50
        )
        
        # Parquet optimization
        parquet_data = ray.data.read_parquet(
            "s3://bucket/large.parquet",
            columns=["needed_column1", "needed_column2"],  # Column projection
            filters=[("date", ">=", "2024-01-01")]  # Predicate pushdown
        )
        
        # JSON optimization
        json_data = ray.data.read_json(
            "s3://bucket/large.json",
            block_size=50 * 1024 * 1024,  # 50MB blocks for JSON
            num_parallel_reads=30
        )
        
        return csv_data, parquet_data, json_data

**Compression and Storage Optimization**

Optimize storage and transfer costs using appropriate compression strategies.

.. code-block:: python

    def optimize_storage():
        """Optimize storage and compression"""
        
        # Use appropriate compression for different formats
        # Parquet with Snappy for fast access
        fast_data = processed_data.write_parquet(
            "s3://bucket/fast_access/",
            compression="snappy"
        )
        
        # Parquet with GZIP for storage efficiency
        storage_efficient = processed_data.write_parquet(
            "s3://bucket/storage_efficient/",
            compression="gzip"
        )
        
        # CSV with GZIP for text data
        compressed_csv = processed_data.write_csv(
            "s3://bucket/compressed/",
            compression="gzip"
        )

Best Practices
--------------

**Format Selection Best Practices**

Choose the right format for your specific workload:

* **Analytics workloads**: Use Parquet or ORC for best performance
* **Data exchange**: Use CSV or JSON for compatibility
* **Storage efficiency**: Use Parquet with GZIP compression
* **Fast access**: Use Parquet with Snappy compression
* **Schema evolution**: Use Avro or Parquet for flexible schemas

**Performance Optimization**

Optimize performance for different formats:

* **Use column projection**: Select only needed columns for large datasets
* **Implement predicate pushdown**: Filter data at the source when possible
* **Optimize block sizes**: Use appropriate block sizes for different formats
* **Leverage parallel processing**: Use multiple parallel reads for large files
* **Monitor memory usage**: Track memory consumption for different formats

**Error Handling**

Implement robust error handling for format processing:

* **Validate data integrity**: Check format consistency and completeness
* **Handle parsing errors**: Gracefully handle malformed data
* **Implement retry logic**: Retry failed format operations
* **Log format issues**: Track format-specific problems for debugging
* **Provide fallback options**: Offer alternative processing paths for problematic data

Next Steps
----------

* Learn about :ref:`Data Warehouse Integration <data-warehouses>` for enterprise data connectivity
* Explore :ref:`Cloud Platform Integration <cloud-platforms>` for cloud-native optimization
* See :ref:`BI Tools Integration <bi-tools>` for visualization and analytics
* Review :ref:`Best Practices <best-practices>` for operational excellence
* Check out :ref:`Business Intelligence <business-intelligence>` for advanced analytics workflows
