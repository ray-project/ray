.. _types-of-data:

Types of Data Guide: Structured, Unstructured & Multimodal Processing
=====================================================================

**Keywords:** structured data, unstructured data, multimodal data processing, CSV, JSON, Parquet, images, audio, video, text processing, data types, data formats, tabular data, semi-structured data

**Navigation:** :ref:`Ray Data <data>` → :ref:`Business Guides <business_guides>` → Types of Data Guide

**Learning Path:** This foundational guide covers Ray Data's universal data processing capabilities. After completing this guide, continue with :ref:`ETL Pipelines <etl-pipelines>` for data engineering or :ref:`Business Intelligence <business-intelligence>` for analytics.

Ray Data excels at processing any type of data - from traditional structured data to cutting-edge multimodal datasets. This guide explains Ray Data's capabilities for different data types and helps you choose the right approach for your workload.

**What you'll learn:**

* When to use Ray Data for different data types
* Performance characteristics and optimization strategies
* Integration patterns with existing data infrastructure
* Business benefits and use cases for each data type

Ray Data's Universal Data Support
---------------------------------

Unlike traditional data processing engines that excel at only one data type, Ray Data provides native excellence across all data formats within a single unified platform. This universal support enables:

* **Unified workflows**: Process multiple data types in the same pipeline
* **Simplified architecture**: Eliminate the need for separate processing systems
* **Cost optimization**: Reduce infrastructure complexity and operational overhead
* **Future-proofing**: Handle emerging data types without platform changes

Structured Data (Tabular)
--------------------------

**What it includes:** CSV, Parquet, database tables, data warehouse data, spreadsheets

Ray Data provides comprehensive support for structured data processing with performance that rivals specialized analytics engines. Built on Apache Arrow and leveraging distributed processing, Ray Data handles structured data with exceptional efficiency.

**Key capabilities:**

* **SQL-style operations**: Joins, aggregations, filtering, sorting, and window functions
* **Data warehouse integration**: Native connectivity with Snowflake, BigQuery, Redshift, Databricks
* **Schema evolution**: Handle changing schemas and data types gracefully
* **Columnar processing**: Optimized for analytical workloads with columnar data formats

**Business benefits:**

* **ETL modernization**: Replace legacy ETL tools with modern, scalable processing
* **Business intelligence**: Prepare data for BI tools and dashboards
* **Data warehousing**: Build and maintain enterprise data warehouses
* **Operational analytics**: Process transactional data for frequent insights

**Performance characteristics:**

* **Vectorized operations**: Leverage Arrow's columnar processing for high performance
* **Predicate pushdown**: Filter data at the source to minimize I/O
* **Column pruning**: Read only required columns to reduce data transfer
* **Intelligent partitioning**: Optimize data layout for query performance

**Example: Customer Analytics Pipeline**

.. code-block:: python

    import ray
    from ray.data.aggregate import Sum, Count, Mean, Max

    # Load customer and order data from data warehouse
    customers = ray.data.read_snowflake(
        "snowflake://user:pass@account/database/schema",
        "SELECT * FROM customers"
    )
    
    orders = ray.data.read_parquet("s3://data-lake/orders/")
    
    # Join and aggregate customer data
    customer_metrics = customers.join(orders, on="customer_id", how="left") \
        .groupby("customer_segment") \
        .aggregate(
            Sum("order_amount"),
            Count("order_id"), 
            Mean("days_since_last_order"),
            Max("order_date")
        )
    
    # Export for BI tool consumption
    customer_metrics.write_csv("s3://bi-data/customer-segments.csv")

**When to use Ray Data for structured data:**

* ✅ **Large datasets**: >1GB that benefit from distributed processing
* ✅ **Complex transformations**: Multi-step ETL pipelines with joins and aggregations
* ✅ **Mixed workloads**: Combining structured data with other data types
* ✅ **Python ecosystem**: Teams using Python for data processing and analytics

* ❌ **Small datasets**: <100MB may be better served by pandas or similar tools
* ❌ **Pure SQL workloads**: SQL-only analytics may be better on dedicated SQL engines

Semi-Structured Data (JSON, XML, Logs)
---------------------------------------

**What it includes:** JSON files, XML documents, log files, nested data, API responses

Semi-structured data contains some organizational structure but doesn't conform to rigid schemas. Ray Data excels at processing semi-structured data with flexible schema handling and powerful transformation capabilities.

**Key capabilities:**

* **Flexible schema handling**: Process data with varying structures and nested fields
* **JSON operations**: Native support for JSON parsing, flattening, and transformation
* **Log analysis**: Parse and analyze application logs, web logs, and system logs
* **API data processing**: Handle REST API responses and webhook data

**Business benefits:**

* **API integration**: Process data from APIs and web services
* **Log analytics**: Monitor applications and systems through log analysis
* **Document processing**: Extract insights from JSON documents and configurations
* **Event processing**: Analyze user events and behavioral data

**Performance characteristics:**

* **Streaming JSON parsing**: Handle large JSON files without loading entirely into memory
* **Schema inference**: Automatically detect and adapt to data schemas
* **Nested data operations**: Efficiently process deeply nested structures
* **Format conversion**: Convert between JSON, Parquet, and other formats

**Example: API Data Processing**

.. code-block:: python

    import ray

    # Load JSON data from API responses
    api_data = ray.data.read_json("s3://api-logs/responses/")
    
    # Extract and flatten nested data
    def extract_user_events(batch):
        """Extract user events from nested API responses."""
        events = []
        for record in batch.to_pylist():
            for event in record.get("events", []):
                events.append({
                    "user_id": record["user_id"],
                    "timestamp": event["timestamp"],
                    "event_type": event["type"],
                    "event_data": event.get("data", {}),
                    "session_id": record.get("session_id")
                })
        return {"events": events}
    
    user_events = api_data.map_batches(extract_user_events)
    
    # Aggregate events by type and user segment
    event_summary = user_events.groupby("event_type") \
        .aggregate(Count("user_id"))
    
    # Save processed data
    event_summary.write_parquet("s3://analytics/user-events/")

**When to use Ray Data for semi-structured data:**

* ✅ **Large JSON files**: Files >100MB that benefit from streaming processing
* ✅ **Complex nested structures**: Data requiring sophisticated flattening and transformation
* ✅ **Log analysis**: Processing application logs, web server logs, or system logs
* ✅ **API integration**: Handling data from REST APIs or webhook endpoints

* ❌ **Simple JSON processing**: Small files may be handled more efficiently with native Python
* ❌ **Real-time streaming**: Ray Data is batch-oriented, not real-time streaming

Unstructured Data (Images, Audio, Video, Text)
-----------------------------------------------

**What it includes:** Images, audio files, video content, text documents, PDFs, binary data

Ray Data's strongest differentiator is its native support for unstructured data processing. Built from the ground up for multimodal AI workloads, Ray Data provides unparalleled capabilities for processing images, audio, video, and text at scale.

**Key capabilities:**

* **Multimodal processing**: Handle images, audio, video, and text in unified workflows
* **GPU acceleration**: Native GPU support for compute-intensive processing
* **Format support**: Comprehensive support for media formats and encodings
* **AI/ML integration**: Seamless integration with PyTorch, TensorFlow, and HuggingFace

**Business benefits:**

* **Computer vision**: Process images for object detection, classification, and analysis
* **Content analysis**: Analyze video content for insights and automation
* **Document processing**: Extract text and insights from PDFs and documents
* **AI model training**: Prepare training data for machine learning models

**Performance characteristics:**

* **Lazy loading**: Load media files only when needed to optimize memory usage
* **GPU optimization**: Leverage GPU acceleration for image and video processing
* **Batch processing**: Process multiple files simultaneously for maximum efficiency
* **Format conversion**: Convert between different media formats and encodings

**Example: Image Processing Pipeline**

.. code-block:: python

    import ray
    from PIL import Image
    import torch

    # Load images from cloud storage
    images = ray.data.read_images("s3://media-bucket/images/")
    
    # Preprocess images for model inference
    def preprocess_image(batch):
        """Resize and normalize images for model input."""
        processed = []
        for item in batch["image"]:
            # Resize image to standard size
            img = Image.fromarray(item).resize((224, 224))
            # Convert to tensor and normalize
            tensor = torch.tensor(img).float() / 255.0
            processed.append(tensor.numpy())
        return {"processed_image": processed}
    
    # Apply preprocessing with GPU acceleration
    processed_images = images.map_batches(
        preprocess_image,
        batch_format="pandas",
        compute=ray.data.ActorPoolStrategy(size=4)  # Use GPU actors
    )
    
    # Run model inference
    def run_inference(batch):
        """Run object detection model on processed images."""
        # Load model (cached in actor)
        model = load_detection_model()  # Your model loading function
        
        predictions = []
        for image in batch["processed_image"]:
            pred = model(image)
            predictions.append(pred)
        return {"predictions": predictions}
    
    results = processed_images.map_batches(run_inference)
    
    # Save results
    results.write_json("s3://results/detections.json")

**When to use Ray Data for unstructured data:**

* ✅ **Large media datasets**: Thousands of images, videos, or audio files
* ✅ **AI/ML workloads**: Training data preparation or batch inference
* ✅ **GPU processing**: Workloads that benefit from GPU acceleration
* ✅ **Multimodal analysis**: Combining different media types in analysis

* ❌ **Single file processing**: Individual files may not benefit from distributed processing
* ❌ **Real-time processing**: Ray Data is optimized for batch processing

Multimodal Data (Mixed Types)
-----------------------------

**What it includes:** Datasets combining structured, semi-structured, and unstructured data

Modern AI and analytics workloads increasingly require processing multiple data types together. Ray Data's unique strength is its ability to handle multimodal data within unified workflows, enabling sophisticated analysis that spans data types.

**Key capabilities:**

* **Unified processing**: Handle all data types in the same pipeline
* **Cross-modal analysis**: Correlate insights across different data types
* **Flexible resource allocation**: Optimize CPU and GPU usage based on data type
* **Schema flexibility**: Adapt to mixed schemas and data structures

**Business benefits:**

* **360-degree customer view**: Combine transactional, behavioral, and content data
* **Advanced analytics**: Perform analysis that spans multiple data modalities
* **AI model enhancement**: Use multimodal data to improve model accuracy
* **Operational intelligence**: Combine structured metrics with unstructured content

**Performance characteristics:**

* **Heterogeneous compute**: Intelligent allocation of CPU and GPU resources
* **Memory optimization**: Efficient handling of mixed data types in memory
* **Pipeline optimization**: Optimize processing order based on data characteristics
* **Resource scheduling**: Balance resources across different processing requirements

**Example: Customer 360 Analysis**

.. code-block:: python

    import ray

    # Load different data types
    customer_data = ray.data.read_parquet("s3://warehouse/customers/")  # Structured
    support_logs = ray.data.read_json("s3://logs/support/")  # Semi-structured  
    product_images = ray.data.read_images("s3://media/products/")  # Unstructured
    
    # Process each data type appropriately
    # Structured data: traditional aggregations
    customer_metrics = customer_data.groupby("customer_id").aggregate(
        Sum("total_spent"),
        Count("orders"),
        Mean("satisfaction_score")
    )
    
    # Semi-structured: extract sentiment from support logs
    def analyze_sentiment(batch):
        """Extract sentiment from support interactions."""
        # Use your sentiment analysis model
        sentiments = []
        for log in batch.to_pylist():
            sentiment = analyze_text_sentiment(log["message"])  # Your function
            sentiments.append({
                "customer_id": log["customer_id"],
                "sentiment_score": sentiment["score"],
                "interaction_date": log["timestamp"]
            })
        return ray.data.from_pylist(sentiments)
    
    customer_sentiment = support_logs.map_batches(analyze_sentiment)
    
    # Unstructured: analyze product images for visual features
    def extract_visual_features(batch):
        """Extract visual features from product images."""
        features = []
        for item in batch.to_pylist():
            visual_features = extract_image_features(item["image"])  # Your function
            features.append({
                "product_id": item["path"].split("/")[-1].split(".")[0],
                "visual_features": visual_features
            })
        return ray.data.from_pylist(features)
    
    product_features = product_images.map_batches(extract_visual_features)
    
    # Combine insights across data types
    # Join structured customer data with sentiment analysis
    enriched_customers = customer_metrics.join(
        customer_sentiment.groupby("customer_id").aggregate(Mean("sentiment_score")),
        on="customer_id",
        how="left"
    )
    
    # Export comprehensive customer insights
    enriched_customers.write_snowflake(
        "snowflake://user:pass@account/database/schema",
        "customer_360_view"
    )

**When to use Ray Data for multimodal data:**

* ✅ **Customer 360 analytics**: Combining transactional, behavioral, and content data
* ✅ **AI model training**: Using diverse data types to improve model performance
* ✅ **Content recommendation**: Combining user data with content features
* ✅ **Fraud detection**: Analyzing structured transactions with unstructured behavior

* ❌ **Simple single-type analysis**: May be overkill for processing only one data type
* ❌ **Real-time requirements**: Better suited for batch processing workflows

Choosing the Right Approach
----------------------------

**Decision Framework**

Use this framework to determine when Ray Data is the right choice for your data processing needs:

**Data Volume**
* **Small** (<100MB): Consider pandas, native Python, or specialized tools
* **Medium** (100MB-10GB): Ray Data provides good value for complex processing
* **Large** (>10GB): Ray Data excels with distributed processing capabilities

**Data Complexity**
* **Simple transformations**: Native tools may be sufficient
* **Complex ETL**: Ray Data provides comprehensive transformation capabilities
* **Multimodal processing**: Ray Data's unique strength in handling mixed data types

**Infrastructure Requirements**
* **Single machine**: Consider whether distributed processing is needed
* **Cluster processing**: Ray Data provides excellent scaling capabilities
* **GPU acceleration**: Ray Data offers best-in-class GPU support

**Team Expertise**
* **Python-focused teams**: Ray Data integrates seamlessly with Python ecosystem
* **SQL-focused teams**: Consider whether Python-based processing fits workflow
* **ML/AI teams**: Ray Data provides exceptional AI/ML integration

**Performance Requirements**

.. list-table::
   :header-rows: 1
   :widths: 20 20 20 20 20

   * - Data Type
     - Throughput
     - Latency
     - Memory Efficiency
     - GPU Support
   * - Structured
     - High
     - Medium
     - High
     - Limited
   * - Semi-structured
     - High
     - Medium
     - Medium
     - Limited
   * - Unstructured
     - Very High
     - Low
     - High
     - Excellent
   * - Multimodal
     - Very High
     - Low
     - Very High
     - Excellent

**Cost Considerations**

* **Infrastructure costs**: Ray Data can reduce costs through efficient resource utilization
* **Development time**: Unified platform reduces development complexity
* **Operational overhead**: Single platform simplifies operations and maintenance
* **Scaling costs**: Linear scaling reduces per-unit processing costs

Migration Strategies
--------------------

**From Traditional ETL Tools**

If you're currently using traditional ETL tools like Informatica, DataStage, or SSIS:

1. **Start with pilot projects**: Begin with non-critical workloads to build experience
2. **Focus on pain points**: Target workloads with performance or scalability issues
3. **Leverage existing logic**: Port business logic to Python for Ray Data processing
4. **Gradual migration**: Move workloads incrementally rather than wholesale replacement

**From Big Data Platforms**

If you're using Hadoop, Spark, or similar platforms:

1. **API similarity**: Ray Data's API is similar to Spark, easing migration
2. **Performance benefits**: Ray Data often provides better performance for Python workloads
3. **Simplified operations**: Reduce operational complexity with Ray's unified platform
4. **Cost optimization**: Potentially reduce infrastructure costs through better resource utilization

**From Cloud-Native Solutions**

If you're using cloud-native data processing services:

1. **Vendor flexibility**: Reduce vendor lock-in with Ray Data's multi-cloud support
2. **Custom processing**: Handle complex transformations that cloud services can't support
3. **Cost control**: Potentially reduce costs for large-scale processing workloads
4. **Integration benefits**: Leverage Ray's broader ecosystem for ML and AI workloads

Best Practices by Data Type
----------------------------

**Structured Data Best Practices**

* **Use columnar formats**: Prefer Parquet over CSV for better performance
* **Optimize partitioning**: Partition data by frequently filtered columns
* **Leverage predicate pushdown**: Apply filters early to reduce data movement
* **Choose appropriate block sizes**: Balance memory usage with parallelization

**Semi-Structured Data Best Practices**

* **Schema inference**: Let Ray Data infer schemas for flexible data structures
* **Streaming processing**: Use streaming for large JSON files to manage memory
* **Normalize when possible**: Convert to structured formats for better performance
* **Handle missing fields**: Design transformations to handle schema variations

**Unstructured Data Best Practices**

* **Lazy loading**: Load media files only when needed to optimize memory
* **GPU acceleration**: Use GPU actors for compute-intensive processing
* **Batch sizing**: Optimize batch sizes for memory and processing efficiency
* **Format optimization**: Convert to efficient formats early in the pipeline

**Multimodal Data Best Practices**

* **Resource planning**: Allocate appropriate CPU and GPU resources for each data type
* **Processing order**: Process data types in order of computational requirements
* **Schema design**: Design schemas that accommodate multiple data types
* **Performance monitoring**: Monitor resource utilization across different data types

Next Steps
----------

Now that you understand Ray Data's capabilities across different data types, explore specific guides for your use case:

* **ETL Pipelines**: Learn how to build comprehensive ETL workflows → :ref:`etl-pipelines`
* **Business Intelligence**: Discover BI and analytics capabilities → :ref:`business-intelligence`  
* **Enterprise Integration**: Understand enterprise deployment patterns → :ref:`enterprise-integration`
* **Performance Optimization**: Optimize Ray Data for your workloads → :ref:`performance-optimization`

Data Type Coverage Quality Checklist
------------------------------------

Use this checklist to ensure your Ray Data implementations leverage the full range of data type capabilities:

**Universal Data Support**
- [ ] Does the solution handle at least 2 different data types (structured, semi-structured, unstructured)?
- [ ] Are Ray Data native APIs used for all data processing operations?
- [ ] Is the same code shown working across different data formats?
- [ ] Are performance characteristics documented for each data type?
- [ ] Is the business value clearly articulated for multimodal processing?

**Traditional and Future Workload Balance**
- [ ] Does content cover both traditional workloads (ETL, BI) and future workloads (AI, multimodal)?
- [ ] Is the 70/30 split between traditional and future content maintained?
- [ ] Are migration paths from traditional to AI-enhanced workloads documented?
- [ ] Is backward compatibility emphasized throughout?
- [ ] Are investment protection benefits clearly communicated?

**Framework-Agnostic Positioning**
- [ ] Is Ray Data positioned as enhancing rather than replacing existing tools?
- [ ] Are multiple framework approaches shown for the same data processing task?
- [ ] Can users easily switch between frameworks without changing Ray Data code?
- [ ] Are integration benefits quantified (performance, scalability, ease of use)?
- [ ] Does content avoid favoring one external framework over another?

**Market Competitiveness**
- [ ] Are the most popular tools in each data type category covered?
- [ ] Is competitive framing done via evaluation criteria, not vendor names?
- [ ] Are emerging data types and formats documented before they become mainstream?
- [ ] Is content future-proofed against rapid technology evolution?
- [ ] Are industry trends reflected in data type priorities?

For hands-on examples, check out our use case guides:

* **ETL Examples**: Practical ETL pipeline implementations → :ref:`etl-examples`
* **BI Examples**: Business intelligence and analytics examples → :ref:`bi-examples`
* **Integration Examples**: Integration patterns with external systems → :ref:`integration-examples`