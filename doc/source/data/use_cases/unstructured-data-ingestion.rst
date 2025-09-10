.. _unstructured-data-ingestion:

Unstructured Data Ingestion: Images, Audio, Video & Documents
=============================================================

**Keywords:** unstructured data processing, image ingestion, video processing, audio analysis, document processing, media files, content analysis, multimodal data pipeline, computer vision, NLP

**Navigation:** :ref:`Ray Data <data>` → :ref:`Use Cases <use_cases>` → Unstructured Data Ingestion

This use case demonstrates Ray Data's industry-leading capabilities for processing unstructured data at scale. Learn how to build robust pipelines for ingesting and processing images, audio, video, and documents for various business applications.

**What you'll build:**

* Large-scale media file processing pipeline
* Document analysis and text extraction workflow
* Content quality validation and metadata extraction
* Multi-format data standardization and optimization

Why Ray Data for Unstructured Data
-----------------------------------

Ray Data is uniquely positioned for unstructured data processing:

**Native Multimodal Support**
Built-in support for images, audio, video, text, and documents with unified processing APIs.

**GPU Acceleration**
Intelligent GPU resource allocation for compute-intensive media processing operations.

**Scalable Architecture**
Process datasets larger than memory with streaming execution and fault tolerance.

**Format Flexibility**
Support for 20+ media formats with automatic format detection and conversion.

**Unstructured Data Processing Capabilities**

:::list-table
   :header-rows: 1

- - **Data Type**
  - **Supported Formats**
  - **Processing Capabilities**
  - **Business Applications**
- - Images
  - JPEG, PNG, TIFF, BMP, WebP
  - Resize, normalize, augmentation, feature extraction
  - Computer vision, content moderation, product catalogs
- - Audio
  - WAV, MP3, FLAC, OGG
  - Format conversion, feature extraction, transcription prep
  - Speech analysis, audio classification, content analysis
- - Video
  - MP4, AVI, MOV, MKV
  - Frame extraction, compression, metadata analysis
  - Content analysis, surveillance, media processing
- - Documents
  - PDF, DOCX, TXT, HTML
  - Text extraction, format conversion, metadata parsing
  - Document analysis, content search, compliance

:::

Use Case 1: Media Content Processing Pipeline
----------------------------------------------

**Business Scenario:** Process large volumes of user-generated content for a social media platform, including images, videos, and audio files for content moderation and recommendation systems.

**Step 1: Load Media Files**

Ray Data provides native support for loading multiple media types simultaneously. This unified approach eliminates the need for separate processing systems.

.. code-block:: python

    import ray
    import numpy as np

    # Load different media types using Ray Data native APIs
    images = ray.data.read_images("s3://user-content/images/")
    videos = ray.data.read_videos("s3://user-content/videos/")  
    audio = ray.data.read_audio("s3://user-content/audio/")

**Why this approach works:** Ray Data's unified loading API handles format detection, parallel loading, and memory management automatically. Unlike traditional approaches requiring separate tools for each media type, Ray Data processes all formats with consistent performance characteristics.

**Step 2: Extract Image Metadata and Features**

Use `map_batches` for vectorized image processing operations that benefit from batch-level optimizations.

.. code-block:: python

    def extract_image_metadata(batch):
        """Extract metadata from image batch using vectorized operations."""
        # Use Ray Data native operations for metadata extraction
        heights = [img.shape[0] for img in batch["image"]]
        widths = [img.shape[1] for img in batch["image"]]
        
        # Vectorized brightness calculation
        brightness_values = [np.mean(img) for img in batch["image"]]
        
        # Add computed features to batch
        batch["width"] = widths
        batch["height"] = heights  
        batch["brightness"] = brightness_values
        batch["content_type"] = "image"
        
        return batch

    # Apply batch processing for efficiency
    processed_images = images.map_batches(
        extract_image_metadata,
        concurrency=4,  # Use 4 actors for parallel image processing
        num_gpus=1
    )

**Why `map_batches` here:** Image metadata extraction benefits from batch processing because it can vectorize operations across multiple images simultaneously. The GPU allocation optimizes for parallel image processing operations.

**Step 3: Process Audio Files**

For audio processing, use Ray Data's streaming capabilities to handle large audio files efficiently.

.. code-block:: python

    def extract_audio_features(batch):
        """Extract audio features using Ray Data native operations."""
        # Process audio metadata efficiently
        durations = []
        amplitudes = []
        
        for audio_array in batch["audio"]:
            # Calculate duration and amplitude using numpy
            duration = len(audio_array) / 44100  # Assume 44.1kHz
            amplitude_mean = np.mean(np.abs(audio_array))
            
            durations.append(duration)
            amplitudes.append(amplitude_mean)
        
        # Add features to batch
        batch["duration"] = durations
        batch["amplitude_mean"] = amplitudes
        batch["content_type"] = "audio"
        
        return batch

    # Process audio with CPU resources (audio processing is CPU-intensive)
    processed_audio = audio.map_batches(extract_audio_features)

**Why this approach:** Audio feature extraction is typically CPU-bound, so we avoid GPU allocation. Ray Data's batch processing ensures efficient memory usage with large audio files.

**Step 4: Combine and Finalize Processing**

Ray Data's `union` operation efficiently combines datasets from different modalities while maintaining schema consistency.

.. code-block:: python

    # Combine all processed media using Ray Data native union
    all_media = processed_images.union(processed_videos).union(processed_audio)
    
    # Add final metadata using map for row-level operations
    final_content = all_media.map(
        lambda row: {**row, "processed_at": "2024-01-01", "status": "ready"}
    )
    
    # Save using Ray Data native writer
    final_content.write_parquet("s3://processed-content/")

**Expected Output:** Unified dataset containing metadata for all media types, optimized for downstream ML model inference and business intelligence analysis.

**Key Ray Data Benefits Demonstrated:**
- **Unified API**: Single interface for all media types
- **Streaming execution**: Process datasets larger than memory  
- **GPU optimization**: Intelligent resource allocation
- **Native operations**: No external dependencies for basic processing

Use Case 2: Document Intelligence Pipeline
-------------------------------------------

**Business Scenario:** Extract insights from large volumes of business documents, contracts, and reports for compliance, search, and analytics.

**Step 1: Load Document Files**

Ray Data handles multiple document formats with a unified API, automatically managing file parsing and memory allocation.

.. code-block:: python

    import ray

    # Load different document types
    pdf_documents = ray.data.read_binary_files("s3://documents/pdfs/")
    text_documents = ray.data.read_text("s3://documents/text/")

**Why Ray Data excels:** Traditional document processing requires separate libraries for each format. Ray Data's unified API simplifies the architecture while providing better performance through distributed loading.

**Step 2: Extract Text Content and Basic Metadata**

Use `map_batches` for document processing since it handles variable-length content efficiently and allows for batch-level optimizations.

.. code-block:: python

    def extract_basic_metadata(batch):
        """Extract text and metadata using Ray Data batch processing."""
        # Calculate file sizes efficiently across the batch
        file_sizes = [len(item.get("bytes", b"")) for item in batch.to_pylist()]
        
        # Extract document categories from file paths
        categories = []
        for item in batch.to_pylist():
            path = item["path"]
            if "contract" in path.lower():
                categories.append("contract")
            elif "report" in path.lower():
                categories.append("report") 
            else:
                categories.append("general")
        
        # Add metadata to batch
        batch["file_size"] = file_sizes
        batch["document_category"] = categories
        batch["processing_timestamp"] = pd.Timestamp.now()
        
        return batch

    # Apply metadata extraction
    documents_with_metadata = pdf_documents.map_batches(extract_basic_metadata)

**Why this pattern:** Document metadata extraction benefits from batch processing because it can process multiple files simultaneously and share computation across the batch.

**Step 3: Add Content Analysis**

For content analysis that requires row-level decisions, use `map` to apply logic to individual documents.

.. code-block:: python

    def analyze_document_content(row):
        """Analyze individual document content for compliance."""
        # Determine compliance requirements based on document type
        requires_review = row["document_category"] == "contract"
        
        # Add content analysis flags
        return {
            **row,
            "requires_compliance_review": requires_review,
            "processing_priority": "high" if requires_review else "standard"
        }

    # Apply row-level analysis using map
    analyzed_documents = documents_with_metadata.map(analyze_document_content)

**Why `map` here:** Document compliance analysis requires individual document assessment, making row-level processing with `map` more appropriate than batch processing.

**Step 4: Save and Validate Results**

.. code-block:: python

    # Save processed documents with Ray Data native writer
    analyzed_documents.write_parquet("s3://processed-documents/analyzed/")
    
    # Verify processing completed successfully
    print(f"Processed {analyzed_documents.count()} documents successfully")

**Expected Output:** Processed document dataset with extracted metadata, content analysis, and compliance flags ready for search indexing and business intelligence.

Use Case 3: AI-Powered Data Quality Pipeline
---------------------------------------------

**Business Scenario:** Use machine learning models to automatically detect data quality issues, anomalies, and inconsistencies in large datasets.

.. code-block:: python

    import ray
    import numpy as np
    from ray.data.aggregate import Sum, Count, Mean, Std

    def ai_powered_data_quality_pipeline():
        """Use AI to detect data quality issues at scale."""
        
        # Load business data for quality assessment
        customer_data = ray.data.read_parquet("s3://raw-data/customers/")
        transaction_data = ray.data.read_parquet("s3://raw-data/transactions/")
        
        def detect_anomalies_with_ml(batch):
            """Use ML models to detect data anomalies."""
            # Load pre-trained anomaly detection model
            # model = load_anomaly_model()  # Your model loading
            
            anomalies = []
            for row in batch.to_pylist():
                # Simulate ML-based anomaly detection
                customer_id = row["customer_id"]
                total_spent = row.get("total_spent", 0)
                
                # Basic anomaly detection (would use actual ML model)
                is_anomaly = total_spent > 50000 or total_spent < 0
                confidence_score = 0.95 if is_anomaly else 0.05
                
                anomalies.append({
                    "customer_id": customer_id,
                    "total_spent": total_spent,
                    "is_anomaly": is_anomaly,
                    "confidence_score": confidence_score,
                    "anomaly_type": "spending_pattern" if is_anomaly else "normal",
                    "detected_at": pd.Timestamp.now()
                })
            
            return ray.data.from_pylist(anomalies)
        
        # Apply ML-based quality checks
        quality_results = customer_data.map_batches(
            detect_anomalies_with_ml,
            compute=ray.data.ActorPoolStrategy(size=4),
            num_gpus=0.5  # Use GPU for ML inference
        )
        
        # Aggregate quality metrics
        quality_summary = quality_results.groupby("anomaly_type").aggregate(
            Count("customer_id"),
            Mean("confidence_score")
        )
        
        # Flag high-confidence anomalies for review
        high_confidence_anomalies = quality_results.filter(
            lambda row: row["is_anomaly"] and row["confidence_score"] > 0.9
        )
        
        # Save quality assessment results
        quality_summary.write_csv("s3://data-quality/summary/")
        high_confidence_anomalies.write_parquet("s3://data-quality/anomalies/")
        
        return quality_summary, high_confidence_anomalies

Use Case 4: GPU-Accelerated ETL Pipeline
-----------------------------------------

**Business Scenario:** Leverage GPU acceleration for computationally intensive ETL operations, including data transformations, feature engineering, and statistical computations.

.. code-block:: python

    import ray
    import cupy as cp  # GPU arrays
    import numpy as np

    def gpu_accelerated_etl_pipeline():
        """ETL pipeline with GPU acceleration for intensive computations."""
        
        # Load large numerical dataset
        large_dataset = ray.data.read_parquet("s3://large-numerical-data/")
        
        def gpu_intensive_transformations(batch):
            """Perform GPU-accelerated mathematical operations."""
            # Convert to GPU arrays for acceleration
            gpu_data = {}
            
            for column in ["amount", "quantity", "price", "discount"]:
                if column in batch.columns:
                    # Move data to GPU
                    gpu_array = cp.array(batch[column].values)
                    
                    # GPU-accelerated computations
                    normalized = (gpu_array - cp.mean(gpu_array)) / cp.std(gpu_array)
                    log_transformed = cp.log1p(cp.maximum(gpu_array, 0))
                    squared = gpu_array ** 2
                    
                    # Move results back to CPU
                    gpu_data[f"{column}_normalized"] = cp.asnumpy(normalized)
                    gpu_data[f"{column}_log"] = cp.asnumpy(log_transformed)
                    gpu_data[f"{column}_squared"] = cp.asnumpy(squared)
            
            # Add GPU-computed features to batch
            for key, values in gpu_data.items():
                batch[key] = values
            
            # Compute complex derived features
            if "amount" in batch.columns and "quantity" in batch.columns:
                batch["unit_value"] = batch["amount"] / batch["quantity"]
                batch["value_category"] = pd.cut(
                    batch["unit_value"], 
                    bins=[0, 10, 50, 100, float('inf')],
                    labels=["low", "medium", "high", "premium"]
                )
            
            return batch
        
        # Apply GPU-accelerated transformations
        transformed_data = large_dataset.map_batches(
            gpu_intensive_transformations,
            batch_format="pandas",
            compute=ray.data.ActorPoolStrategy(size=4),
            num_gpus=1  # Allocate GPU for intensive computations
        )
        
        def gpu_aggregation_features(batch):
            """Create aggregated features using GPU acceleration."""
            # Group-level GPU computations
            gpu_amounts = cp.array(batch["amount"].values)
            
            # Statistical computations on GPU
            rolling_mean = cp.convolve(gpu_amounts, cp.ones(5)/5, mode='same')
            percentile_75 = cp.percentile(gpu_amounts, 75)
            percentile_25 = cp.percentile(gpu_amounts, 25)
            
            # Add computed features
            batch["rolling_mean_5"] = cp.asnumpy(rolling_mean)
            batch["is_high_value"] = batch["amount"] > cp.asnumpy(percentile_75)
            batch["is_low_value"] = batch["amount"] < cp.asnumpy(percentile_25)
            
            return batch
        
        # Apply GPU aggregations
        final_data = transformed_data.map_batches(
            gpu_aggregation_features,
            compute=ray.data.ActorPoolStrategy(size=2),
            num_gpus=1
        )
        
        # Save GPU-processed data
        final_data.write_parquet("s3://processed-data/gpu-enhanced/")
        
        return final_data

Use Case 5: Computer Vision Data Pipeline
------------------------------------------

**Business Scenario:** Build a computer vision pipeline for product catalog management, including image classification, object detection, and visual feature extraction.

.. code-block:: python

    import ray
    from PIL import Image
    import torch
    import torchvision.transforms as transforms

    def computer_vision_data_pipeline():
        """Complete computer vision pipeline for product catalog."""
        
        # Load product images
        product_images = ray.data.read_images("s3://product-catalog/images/")
        
        def preprocess_for_computer_vision(batch):
            """Preprocess images for computer vision models."""
            processed = []
            
            # Define image preprocessing pipeline
            transform = transforms.Compose([
                transforms.Resize((224, 224)),
                transforms.ToTensor(),
                transforms.Normalize(mean=[0.485, 0.456, 0.406], 
                                   std=[0.229, 0.224, 0.225])
            ])
            
            for item in batch.to_pylist():
                image = Image.fromarray(item["image"])
                path = item["path"]
                
                # Extract product ID from filename
                product_id = path.split("/")[-1].split(".")[0]
                
                # Preprocess image
                processed_tensor = transform(image)
                
                # Extract basic image properties
                original_size = image.size
                aspect_ratio = original_size[0] / original_size[1]
                
                processed.append({
                    "product_id": product_id,
                    "image_tensor": processed_tensor.numpy(),
                    "original_width": original_size[0],
                    "original_height": original_size[1],
                    "aspect_ratio": aspect_ratio,
                    "file_path": path
                })
            
            return ray.data.from_pylist(processed)
        
        def run_object_detection(batch):
            """Run object detection on preprocessed images."""
            # Load object detection model (cached in actor)
            # model = load_detection_model()
            
            results = []
            for item in batch.to_pylist():
                product_id = item["product_id"]
                image_tensor = item["image_tensor"]
                
                # Run object detection (simplified)
                # detections = model(image_tensor)
                
                # Simulate detection results
                detected_objects = ["product", "background"]
                confidence_scores = [0.95, 0.85]
                bounding_boxes = [[10, 10, 200, 200], [0, 0, 224, 224]]
                
                results.append({
                    "product_id": product_id,
                    "detected_objects": detected_objects,
                    "confidence_scores": confidence_scores,
                    "bounding_boxes": bounding_boxes,
                    "detection_count": len(detected_objects),
                    "max_confidence": max(confidence_scores)
                })
            
            return ray.data.from_pylist(results)
        
        def extract_visual_features(batch):
            """Extract visual features for similarity search."""
            features = []
            
            for item in batch.to_pylist():
                product_id = item["product_id"]
                
                # Extract visual features (simplified)
                # feature_vector = feature_extraction_model(image)
                feature_vector = np.random.rand(512).tolist()  # Placeholder
                
                # Color analysis
                dominant_colors = ["red", "blue", "white"]  # Simplified
                color_distribution = [0.4, 0.3, 0.3]
                
                features.append({
                    "product_id": product_id,
                    "visual_features": feature_vector,
                    "dominant_colors": dominant_colors,
                    "color_distribution": color_distribution,
                    "feature_extraction_complete": True
                })
            
            return ray.data.from_pylist(features)
        
        # Process images with GPU acceleration
        preprocessed = product_images.map_batches(
            preprocess_for_computer_vision,
            compute=ray.data.ActorPoolStrategy(size=4),
            num_gpus=1
        )
        
        # Run object detection
        detections = preprocessed.map_batches(
            run_object_detection,
            compute=ray.data.ActorPoolStrategy(size=2),
            num_gpus=1
        )
        
        # Extract visual features
        features = preprocessed.map_batches(
            extract_visual_features,
            compute=ray.data.ActorPoolStrategy(size=4),
            num_gpus=1
        )
        
        # Join detection and feature results
        complete_analysis = detections.join(features, on="product_id", how="inner")
        
        # Save computer vision results
        complete_analysis.write_parquet("s3://product-analysis/computer-vision/")
        
        return complete_analysis

Use Case 6: Large-Scale Feature Engineering
--------------------------------------------

**Business Scenario:** Create features for machine learning models from raw data, including time-series features, categorical encoding, and statistical transformations.

.. code-block:: python

    import ray
    import pandas as pd
    import numpy as np
    from ray.data.aggregate import Sum, Count, Mean, Min, Max, Std

    def large_scale_feature_engineering():
        """Create ML features from raw business data."""
        
        # Load raw transaction data
        transactions = ray.data.read_parquet("s3://raw-data/transactions/")
        customer_data = ray.data.read_parquet("s3://raw-data/customers/")
        
        def create_time_based_features(batch):
            """Create time-based features from transaction timestamps."""
            # Convert timestamp to datetime
            batch["transaction_datetime"] = pd.to_datetime(batch["timestamp"])
            
            # Extract time components
            batch["hour"] = batch["transaction_datetime"].dt.hour
            batch["day_of_week"] = batch["transaction_datetime"].dt.dayofweek
            batch["month"] = batch["transaction_datetime"].dt.month
            batch["quarter"] = batch["transaction_datetime"].dt.quarter
            batch["is_weekend"] = batch["day_of_week"].isin([5, 6])
            batch["is_business_hours"] = batch["hour"].between(9, 17)
            
            # Create time-based categorical features
            def time_period(hour):
                if 6 <= hour < 12:
                    return "morning"
                elif 12 <= hour < 18:
                    return "afternoon"
                elif 18 <= hour < 22:
                    return "evening"
                else:
                    return "night"
            
            batch["time_period"] = batch["hour"].apply(time_period)
            
            return batch
        
        def create_statistical_features(batch):
            """Create statistical features for ML models."""
            # Amount-based features
            batch["amount_log"] = np.log1p(batch["amount"])
            batch["amount_sqrt"] = np.sqrt(batch["amount"])
            batch["amount_squared"] = batch["amount"] ** 2
            
            # Categorical encoding for high-cardinality features
            # One-hot encode categories (simplified)
            category_dummies = pd.get_dummies(batch["product_category"], prefix="cat")
            
            # Add encoded features to batch
            for col in category_dummies.columns:
                batch[col] = category_dummies[col]
            
            # Create interaction features
            batch["amount_per_hour"] = batch["amount"] / (batch["hour"] + 1)
            batch["weekend_premium"] = batch["amount"] * batch["is_weekend"].astype(int)
            
            return batch
        
        # Create time-based features
        time_features = transactions.map_batches(create_time_based_features)
        
        # Create statistical features
        statistical_features = time_features.map_batches(create_statistical_features)
        
        # Create customer-level aggregated features
        customer_features = statistical_features.groupby("customer_id").aggregate(
            Sum("amount"),                    # Total spending
            Count("transaction_id"),          # Transaction frequency
            Mean("amount"),                   # Average transaction value
            Std("amount"),                    # Spending variability
            Min("amount"),                    # Minimum transaction
            Max("amount")                     # Maximum transaction
        )
        
        # Join with customer demographic data
        def add_demographic_features(batch):
            """Add demographic-based features."""
            # Age-based features
            batch["age_group"] = pd.cut(
                batch["age"], 
                bins=[0, 25, 35, 50, 65, 100],
                labels=["young", "adult", "middle_aged", "senior", "elderly"]
            )
            
            # Income-based features
            batch["income_tier"] = pd.qcut(
                batch["annual_income"], 
                q=5, 
                labels=["low", "low_mid", "middle", "high_mid", "high"]
            )
            
            return batch
        
        enriched_customers = customer_data.map_batches(add_demographic_features)
        
        # Join transaction features with customer features
        final_features = customer_features.join(
            enriched_customers, 
            on="customer_id", 
            how="inner"
        )
        
        # Save feature store
        final_features.write_parquet("s3://feature-store/customer-features/")
        
        return final_features

**Unstructured Data Processing Checklist**

Use this checklist to ensure your unstructured data pipelines follow best practices:

**Data Ingestion:**
- [ ] **Format support**: Verify Ray Data supports your media formats natively
- [ ] **Batch sizing**: Optimize batch sizes for memory usage with large media files
- [ ] **GPU allocation**: Use appropriate GPU resources for compute-intensive operations
- [ ] **Error handling**: Handle corrupted or unsupported files gracefully
- [ ] **Metadata extraction**: Capture file metadata alongside content processing

**Processing Pipeline:**
- [ ] **Streaming execution**: Use streaming for datasets larger than memory
- [ ] **Memory management**: Monitor memory usage with large binary files
- [ ] **GPU optimization**: Leverage GPU actors for parallel processing
- [ ] **Format conversion**: Standardize formats early in the pipeline
- [ ] **Quality validation**: Implement content quality checks

**AI Integration:**
- [ ] **Model loading**: Cache models in actors for efficiency
- [ ] **Batch inference**: Use appropriate batch sizes for GPU memory
- [ ] **Feature extraction**: Extract meaningful features for downstream use
- [ ] **Result validation**: Validate AI model outputs for quality
- [ ] **Performance monitoring**: Track GPU utilization and processing speed

**Output and Storage:**
- [ ] **Efficient formats**: Use Parquet for structured results
- [ ] **Partitioning**: Partition large outputs for query performance
- [ ] **Compression**: Enable compression for storage efficiency
- [ ] **Metadata preservation**: Maintain lineage and processing metadata
- [ ] **Access patterns**: Optimize storage layout for downstream access

Next Steps
----------

Explore related use cases and advanced patterns:

* **Multimodal Content Analysis**: Combine unstructured data with business data → :ref:`Multimodal Content Analysis <multimodal-content-analysis>`
* **Batch Inference**: Scale model inference → :ref:`Batch Inference <batch_inference_home>`
* **AI-Powered Data Quality**: Use ML for data validation → :ref:`AI Data Quality Pipeline <ai-data-quality-pipeline>`
* **Advanced Analytics**: Statistical analysis on processed data → :ref:`Advanced Analytics <advanced-analytics>`

For production deployment of unstructured data pipelines, see :ref:`Best Practices <best_practices>` and :ref:`Performance Optimization <performance-optimization>`.
