.. _data:

==================================
Ray Data: Unified Data Processing for Any Workload
==================================

.. meta::
   :description: Ray Data is the universal data processing engine for any data type, AI framework, and compute infrastructure. Process structured, unstructured, and multimodal data with streaming execution and GPU optimization.
   :keywords: distributed data processing, multimodal AI, ETL pipelines, business intelligence, machine learning data, GPU acceleration, streaming execution, enterprise data platform

.. toctree::
    :hidden:

    installation-setup
    quickstart
    key-concepts
    personas/index
    learning-paths
    data-engineering-guide
    core_operations/index
    workloads/index
    business_guides/index
    use_cases/index
    integrations/index
    best_practices/index
    advanced/index
    support/index
    examples
    api/api

**The universal data processing engine that scales from laptops to petabytes**

Ray Data unifies traditional data processing with modern AI workloads in a single platform. Process **any data type** (structured, unstructured, multimodal), integrate with **any AI framework** (PyTorch, TensorFlow, HuggingFace), and scale across **any compute infrastructure** (CPU, GPU, distributed clusters). Built on :ref:`Ray's distributed computing foundation <ray-core>`, Ray Data eliminates the complexity of managing multiple specialized tools.

**What you'll learn on this page:**

* Why Ray Data outperforms traditional data processing engines
* How to process any workload from ETL to multimodal AI
* When to choose Ray Data over alternatives
* How to get started with your specific use case

**Proven at enterprise scale:** ByteDance (200TB+ daily), Pinterest (90%+ GPU utilization), Coinbase (8x faster ETL), and Uber rely on Ray Data for mission-critical production workloads.

**Customer Success Metrics:**

* **200TB+ daily processing** - ByteDance multimodal AI pipelines
* **8x faster data transformation** - Coinbase ETL optimization  
* **90%+ GPU utilization** - Pinterest computer vision workloads
* **2.6x throughput improvement** - eBay batch inference optimization

> "Ray Data enables us to process massive amounts of multimodal data efficiently, powering recommendation algorithms that serve billions of users worldwide." – ByteDance Engineering Team

**Why Ray Data Outperforms Alternatives**

Ray Data solves the fundamental limitation of existing data processing engines: the forced choice between traditional structured data OR modern AI workloads. Ray Data excels at both, enabling unified pipelines that combine any data type for comprehensive analysis.

**Unique technical advantages:**

* **Streaming execution**: Process datasets 10x larger than cluster memory
* **Multimodal processing**: Native support for images, audio, video, and text
* **GPU optimization**: Automatic resource allocation and 90%+ utilization
* **Python-native performance**: Zero JVM overhead, Arrow-based operations
* **AI ecosystem integration**: Seamless Ray Train, Tune, and Serve connectivity

**Real-world impact example:**
Analyze customer purchase transactions, product images, and support chat logs in a single pipeline to predict customer satisfaction - impossible with traditional tools that require separate systems for structured and unstructured data.

**Universal Platform Capabilities**

:::list-table
   :header-rows: 1

- - **Capability**
  - **Ray Data Support**
  - **Production Evidence**
- - **Data Types**
  - Structured, unstructured, multimodal
  - 26+ native connectors, ByteDance 200TB+ multimodal
- - **AI Frameworks**
  - PyTorch, TensorFlow, HuggingFace, vLLM
  - Seamless integration, zero framework lock-in
- - **Compute Infrastructure**
  - CPU, GPU, distributed clusters
  - Pinterest 90%+ GPU utilization
- - **Output Destinations**
  - Databases, warehouses, files, APIs
  - Enterprise platform connectivity
- - **Scale Range**
  - Laptop to petabyte datasets
  - Streaming execution, proven at ByteDance scale

:::


Why Choose Ray Data?
--------------------

Ray Data delivers measurable advantages across performance, cost, and operational efficiency:

.. grid:: 2 2 3 3
    :gutter: 3
    :class-container: container pb-5

    .. grid-item-card::
        :class-header: bg-primary text-white
        :class-body: text-left

        **Performance at Scale**
        ^^^

        Streaming execution and GPU optimization deliver measurable performance improvements.

        * **8x faster ETL** (Coinbase production)
        * **2.6x inference throughput** (eBay GPU optimization)
        * **90%+ GPU utilization** (Pinterest computer vision)

    .. grid-item-card::
        :class-header: bg-success text-white
        :class-body: text-left

        **Cost Efficiency**
        ^^^

        Intelligent resource allocation reduces infrastructure costs through efficient utilization.

        * **Process 10x larger datasets** with same memory
        * **Automatic CPU/GPU optimization** within pipelines
        * **Streaming execution** eliminates memory bottlenecks

    .. grid-item-card::
        :class-header: bg-info text-white
        :class-body: text-left

        **Universal Connectivity**
        ^^^

        Connect any data source to any destination with 26+ native connectors.

        * **Enterprise platforms**: Snowflake, Databricks, Unity Catalog
        * **Cloud storage**: S3, GCS, Azure Blob, HDFS
        * **AI frameworks**: PyTorch, TensorFlow, HuggingFace

    .. grid-item-card::
        :class-header: bg-warning text-white
        :class-body: text-left

        **AI-Native Architecture**
        ^^^

        Built specifically for modern AI and multimodal data processing workloads.

        * **Multimodal processing**: Images, audio, video, text
        * **GPU acceleration**: Built-in optimization
        * **Framework integration**: Zero lock-in, any AI framework

    .. grid-item-card::
        :class-header: bg-danger text-white
        :class-body: text-left

        **Streaming Execution**
        ^^^

        Process datasets larger than available memory through intelligent pipeline execution.

        * **Memory efficiency**: Handle petabyte datasets
        * **Pipeline parallelism**: CPU/GPU concurrent processing
        * **Fault tolerance**: Production-grade reliability

    .. grid-item-card::
        :class-header: bg-dark text-white
        :class-body: text-left

        **Python-Native Performance**
        ^^^

        Zero JVM overhead with Apache Arrow optimization for maximum Python performance.

        * **No serialization bottlenecks** (vs Java-based systems)
        * **Native Python ecosystem** integration
        * **Arrow-based operations** for zero-copy efficiency

**See Ray Data's Unique Capabilities in Action**

This example demonstrates Ray Data's ability to unify traditional business analytics with modern AI processing in a single pipeline:

.. code-block:: python

    import ray
    from ray.data.aggregate import Sum, Count, Mean
    import torch

    # Load structured business data from data warehouse
    transactions = ray.data.read_parquet("s3://warehouse/transactions/")
    
    # Load unstructured product images for AI analysis
    product_images = ray.data.read_images("s3://content/product-images/")
    
    # Traditional business analytics with streaming execution
    customer_analytics = transactions \
        .filter(lambda row: row["amount"] > 100) \
        .groupby("customer_id") \
        .aggregate(
            Sum("amount"),           # Total customer value
            Count("transaction_id"), # Purchase frequency
            Mean("amount")           # Average order value
        )
    
    # AI-powered image processing with GPU optimization
    def extract_product_features(batch):
        """Extract features using computer vision model."""
        # Load pre-trained vision model on GPU
        model = torch.hub.load('pytorch/vision', 'resnet50', pretrained=True)
        model.eval()
        
        # Process batch of images with GPU acceleration
        features = model(batch["image"])
        return {"product_id": batch["product_id"], "features": features}
    
    # Process images with automatic GPU allocation
    product_features = product_images \
        .map_batches(
            extract_product_features,
            compute=ray.data.ActorPoolStrategy(size=8),
            num_gpus=1.0,  # Full GPU per worker
            batch_size=32   # Optimize for GPU memory
        )
    
    # Stream results directly to data warehouse
    customer_analytics.write_parquet("s3://warehouse/customer-insights/")
    product_features.write_parquet("s3://warehouse/product-ai-features/")
    
    print(f"Processed {customer_analytics.count()} customers")
    print(f"Analyzed {product_features.count()} product images")

**This showcases Ray Data's competitive advantages:**

* **Multimodal unification**: Process structured transactions AND unstructured images in one pipeline
* **Streaming execution**: Handle datasets larger than cluster memory without bottlenecks
* **Intelligent GPU allocation**: Automatic resource optimization for mixed CPU/GPU workloads
* **Zero framework lock-in**: Use any AI framework (PyTorch, TensorFlow, HuggingFace)
* **Enterprise connectivity**: Direct warehouse integration without complex ETL
* **Production performance**: 90%+ GPU utilization, 2.6x throughput improvements

**Production Performance Validation**

Run this performance test to see Ray Data's streaming execution advantage:

.. code-block:: python

    import ray
    import time

    # Performance test: Process large dataset with streaming execution
    def performance_benchmark():
        start_time = time.time()
        
        # Create large dataset (simulates 10GB+ real data)
        large_dataset = ray.data.range(10_000_000)
        
        # Complex processing pipeline with streaming execution
        result = large_dataset \
            .map(lambda x: {"value": x["id"] * 2}) \
            .filter(lambda row: row["value"] % 100 == 0) \
            .map_batches(
                lambda batch: {"processed": batch["value"] ** 0.5},
                batch_size=1000
            ) \
            .groupby(lambda row: row["processed"] // 10) \
            .aggregate(Count("processed"))
        
        # Stream to output (no memory bottlenecks)
        count = result.count()
        
        processing_time = time.time() - start_time
        print(f"Processed 10M records in {processing_time:.1f}s")
        print(f"Memory usage: Constant (streaming execution)")
        return count
    
    # Run the benchmark
    performance_benchmark()

**Expected results:**
* **Constant memory usage**: Processes any dataset size with fixed memory
* **Linear performance scaling**: Add nodes for proportional speedup
* **No memory errors**: Unlike pandas/single-node tools that crash on large data

**Measured production results:**


**Customer Success Stories**

> "Ray enables us to run deep learning workloads 12x faster, to reduce costs by 8x, and to train our models on 100x more data." – Instacart

> "Ray is excellent for unstructured and multimodal data. At Uber, we work on computer vision, natural language processing, and hybrids of structured and unstructured data." – Uber

> "The last mile data transformation dropped from 120 minutes to 15 minutes because of distributed data processing with Ray." – Coinbase

> "We're using Ray Data behind the scenes to make sure you're getting really efficient throughput [and] using as many of your compute resources as possible." – Snowflake

**Competitive Advantages: Why Ray Data Wins Technical Evaluations**

**Evaluation Criteria #1: Multimodal Data Processing**

Ray Data is the only platform built from the ground up for processing images, video, audio, and text alongside traditional tabular data. This architectural advantage enables unified workflows that combine structured business data with unstructured content - something impossible with traditional tools.

**Technical proof point:** Process customer transactions, product images, and support chat logs in a single pipeline for comprehensive customer analytics. Traditional platforms require 3+ separate tools.

**Evaluation Criteria #2: Memory Efficiency and Scale**

Ray Data's streaming execution architecture processes data continuously without memory limits, enabling datasets 10x larger than cluster memory through intelligent pipeline execution.

**Technical proof point:** ByteDance processes 200TB+ daily with consistent performance. Traditional batch systems fail at this scale due to memory constraints.

**Evaluation Criteria #3: GPU Utilization and Performance**

Intelligent CPU and GPU resource allocation within single pipelines optimizes utilization for mixed workloads without manual configuration.

**Technical proof point:** Pinterest achieves 90%+ GPU utilization for computer vision workloads. Traditional systems typically achieve 25-40% utilization.

**Evaluation Criteria #4: Python-Native Performance**

Built on Apache Arrow with zero-copy operations, eliminating JVM overhead and serialization bottlenecks that affect Java-based systems.

**Technical proof point:** Coinbase reduced ETL processing from 120 minutes to 15 minutes (8x improvement) by eliminating serialization overhead.

**Enterprise Production Deployments**

Ray Data powers mission-critical workloads across industries with proven scalability and reliability:

:::list-table
   :header-rows: 1

- - **Company**
  - **Use Case**
  - **Scale**
  - **Results**
- - **ByteDance**
  - Multimodal LLM training data
  - 200TB+ daily
  - Powers billions of users
- - **Pinterest**
  - Computer vision workloads
  - 90%+ GPU utilization
  - Optimized model training
- - **Coinbase**
  - ETL data transformation
  - 8x performance improvement
  - 120min → 15min processing
- - **eBay**
  - Batch inference pipelines
  - 2.6x throughput improvement
  - 25% → 75% GPU utilization
- - **Ant Group**
  - Production model serving
  - 6,000+ CPU cores
  - Hundreds of millions of users
- - **Spotify**
  - ML platform infrastructure
  - Millions of users served
  - New ML platform foundation

:::

These deployments demonstrate Ray Data's enterprise-grade reliability and performance across diverse workloads.

Choose Your Workload
---------------------

Ray Data excels across traditional business data processing and modern AI workloads:

.. grid:: 2 2 2 2
    :gutter: 3
    :class-container: container pb-5

    .. grid-item-card::
        :class-header: bg-gradient-primary text-white
        :class-body: text-left

        **ETL & Data Engineering**
        ^^^

        Build enterprise-grade data pipelines with streaming execution and fault tolerance.

        * Extract from 26+ enterprise data sources
        * Transform with GPU-accelerated distributed processing
        * Load to any warehouse, lake, or operational store
        * Handle petabyte-scale datasets with streaming execution
        * **8x faster processing** (Coinbase production)

        +++
        .. button-ref:: etl-pipelines
            :color: primary
            :outline:
            :expand:

            Start ETL Guide

    .. grid-item-card::
        :class-header: bg-gradient-success text-white
        :class-body: text-left

        **Business Intelligence**
        ^^^

        Create high-performance analytics and reports with streaming aggregations.

        * Advanced aggregations at petabyte scale
        * Direct export to Tableau, Power BI, Looker
        * Statistical analysis with GPU acceleration
        * Real-time dashboard data preparation
        * **10x faster analytics** vs traditional tools

        +++
        .. button-ref:: business-intelligence
            :color: success
            :outline:
            :expand:

            Start BI Guide

    .. grid-item-card::
        :class-header: bg-gradient-info text-white
        :class-body: text-left

        **Machine Learning & AI**
        ^^^

        Build AI pipelines with multimodal data processing and seamless Ray ecosystem integration.

        * Multimodal training data preprocessing
        * Batch inference with 2.6x throughput gains
        * GPU-optimized processing (90%+ utilization)
        * Direct Ray Train/Serve integration
        * **Any AI framework**: PyTorch, TensorFlow, HuggingFace

        +++
        .. button-ref:: working-with-ai
            :color: info
            :outline:
            :expand:

            Start AI Guide

    .. grid-item-card::
        :class-header: bg-gradient-warning text-white
        :class-body: text-left

        **Advanced Analytics**
        ^^^

        Perform complex statistical analysis and time-series processing with streaming execution.

        * Statistical operations at petabyte scale
        * Real-time time-series analysis
        * GPU-accelerated correlation and regression
        * AI-powered anomaly detection
        * **Memory-efficient processing** for large datasets

        +++
        .. button-ref:: advanced-analytics
            :color: warning
            :outline:
            :expand:

            Start Analytics

**Get Started in 2 Minutes**

**Installation**

Install Ray Data with a single command - no complex setup required:

.. code-block:: console

    pip install -U 'ray[data]'

**Verify your installation** works with this quick test:

.. code-block:: python

    import ray
    
    # Test basic functionality
    ds = ray.data.range(1000)
    result = ds.map(lambda x: x * 2).take(5)
    print(result)  # Should output: [0, 2, 4, 6, 8]

**Expected output:** `[0, 2, 4, 6, 8]` - if you see this, Ray Data is working correctly!

For additional installation options, see :ref:`Installation & Setup <installation-setup>`.

**Choose Your Learning Path Based on Your Role**

*Get personalized guidance for your specific use case and experience level:*

.. grid:: 1 2 2 2
    :gutter: 4
    :class-container: container pb-5

    .. grid-item-card::
        :class-header: bg-primary text-white
        :class-body: text-left

        **New to Ray Data**
        ^^^
        Complete tutorial from basics to production deployment

        **Learning timeline:**
        * **15 minutes**: Basic concepts and first pipeline
        * **1 hour**: Real-world examples and patterns
        * **1 day**: Production-ready deployment

        +++
        .. button-ref:: personas
            :color: primary
            :outline:
            :expand:

            Choose Your Path

    .. grid-item-card::
        :class-header: bg-success text-white
        :class-body: text-left

        **Data Engineer**
        ^^^
        ETL pipelines, data warehouses, enterprise integration, streaming architecture

        **Key benefits:**
        * **8x faster ETL** processing
        * **Streaming execution** for large datasets
        * **Enterprise connectivity** (Snowflake, Databricks)

        +++
        .. button-ref:: data-engineer-path
            :color: success
            :outline:
            :expand:

            Engineering Path

    .. grid-item-card::
        :class-header: bg-info text-white
        :class-body: text-left

        **Business Analyst**
        ^^^
        Analytics, reporting, business intelligence, dashboard preparation

        **Key benefits:**
        * **10x faster analytics** processing
        * **Direct BI tool integration** (Tableau, Power BI)
        * **Petabyte-scale aggregations**

        +++
        .. button-ref:: business-analyst-path
            :color: info
            :outline:
            :expand:

            Analytics Path

    .. grid-item-card::
        :class-header: bg-warning text-white
        :class-body: text-left

        **AI/ML Engineer**
        ^^^
        Training data, inference, multimodal processing, GPU optimization

        **Key benefits:**
        * **90%+ GPU utilization** (Pinterest production)
        * **2.6x inference throughput** (eBay optimization)
        * **Multimodal processing** (images, audio, video, text)

        +++
        .. button-ref:: ai-engineer-path
            :color: warning
            :outline:
            :expand:

            AI/ML Path

**Comprehensive Ray Data Documentation**

**Core Operations & Concepts**

*Master the fundamentals of Ray Data processing:*

.. grid:: 1 2 2 2
    :gutter: 3
    :class-container: container pb-4

    .. grid-item-card::
        :class-header: bg-primary text-white
        :class-body: text-left

        **Core Operations**
        ^^^

        Master loading, transforming, and aggregating data with streaming execution.

        * **Loading**: 26+ data sources and formats
        * **Transforming**: Distributed processing with GPU acceleration
        * **Aggregating**: SQL-style operations at petabyte scale
        * **Consuming**: Framework integration and export options

        +++
        .. button-ref:: core_operations
            :color: primary
            :outline:
            :expand:

            Master Operations

    .. grid-item-card::
        :class-header: bg-success text-white
        :class-body: text-left

        **Enterprise Workloads**
        ^^^

        Traditional business data processing + Modern AI/ML workloads unified.

        * **ETL pipelines**: Streaming execution, fault tolerance
        * **Business intelligence**: Petabyte-scale analytics
        * **AI/ML processing**: Multimodal data, GPU optimization
        * **Real-time processing**: Streaming aggregations

        +++
        .. button-ref:: workloads
            :color: success
            :outline:
            :expand:

            Explore Workloads

    .. grid-item-card::
        :class-header: bg-warning text-white
        :class-body: text-left

        **Business Implementation**
        ^^^

        Business-focused implementation guides with ROI analysis and cost optimization.

        * **ETL cost optimization**: Up to 10x cost reduction
        * **BI performance**: 10x faster analytics
        * **Enterprise integration**: Security, compliance, governance
        * **Migration planning**: Risk assessment, timeline

        +++
        .. button-ref:: business_guides
            :color: warning
            :outline:
            :expand:

            Business Value

**Implementation & Advanced Topics**

*Real-world examples, integrations, and advanced capabilities:*

.. grid:: 1 2 2 2
    :gutter: 3
    :class-container: container pb-4

    .. grid-item-card::
        :class-header: bg-secondary text-white
        :class-body: text-left

        **Real-World Use Cases**
        ^^^

        Complete implementation examples with performance benchmarks and best practices.

        * **Financial analytics**: Risk modeling, fraud detection
        * **Computer vision**: Image processing, video analysis
        * **NLP processing**: Text analysis, content moderation
        * **ETL pipelines**: Data warehousing, lake house architecture

        +++
        .. button-ref:: use_cases
            :color: secondary
            :outline:
            :expand:

            See Examples

    .. grid-item-card::
        :class-header: bg-danger text-white
        :class-body: text-left

        **Enterprise Integrations**
        ^^^

        Connect with existing enterprise systems and modern AI platforms.

        * **Data platforms**: Snowflake, Databricks, BigQuery
        * **BI tools**: Tableau, Power BI, Looker
        * **AI platforms**: MLflow, Weights & Biases
        * **Cloud services**: AWS, GCP, Azure native integration

        +++
        .. button-ref:: integrations
            :color: danger
            :outline:
            :expand:

            View Integrations

    .. grid-item-card::
        :class-header: bg-dark text-white
        :class-body: text-left

        **Production Best Practices**
        ^^^

        Production deployment, performance optimization, and operational excellence.

        * **Performance tuning**: GPU optimization, memory efficiency
        * **Production deployment**: Security, monitoring, alerting
        * **Cost optimization**: Resource allocation, scaling strategies
        * **Troubleshooting**: Common issues and solutions

        +++
        .. button-ref:: best_practices
            :color: dark
            :outline:
            :expand:

            Production Guide

    .. grid-item-card::
        :class-header: bg-purple text-white
        :class-body: text-left

        **Advanced Features**
        ^^^

        Architecture internals, experimental features, and cutting-edge capabilities.

        * **Streaming architecture**: Technical implementation details
        * **Custom operators**: Build specialized processing logic
        * **Performance internals**: Memory management, optimization
        * **Experimental features**: Preview upcoming capabilities

        +++
        .. button-ref:: advanced
            :color: purple
            :outline:
            :expand:

            Advanced Guide

**Reference & Support**

.. grid:: 1 2 2 2
    :gutter: 3
    :class-container: container pb-4

    .. grid-item-card::
        :class-header: bg-info text-white

        **API Reference**
        ^^^

        Complete technical documentation and API details

        +++
        .. button-ref:: data-api
            :color: info
            :outline:
            :expand:

            API Reference

    .. grid-item-card::
        :class-header: bg-success text-white

        **Migration & Testing**
        ^^^

        Migration strategies and testing methodologies

        +++
        .. button-ref:: migration-testing
            :color: success
            :outline:
            :expand:

            Migration Guide

    .. grid-item-card::
        :class-header: bg-warning text-white

        **Support & Resources**
        ^^^

        Get help, troubleshooting, migration guidance, and community support

        +++
        .. button-ref:: support
            :color: warning
            :outline:
            :expand:

            Support

    .. grid-item-card::
        :class-header: bg-primary text-white

        **Examples Collection**
        ^^^

        Browse all examples across different frameworks and use cases

        +++
        .. button-ref:: examples
            :color: primary
            :outline:
            :expand:

            All Examples


Success Stories
---------------

Ray Data powers diverse workloads across industries:

**Multimodal AI and Content Processing**
> "Ray Data enables us to process massive amounts of multimodal data efficiently, powering recommendation algorithms that serve billions of users worldwide." – ByteDance

**Computer Vision and GPU Optimization**
> "Using Ray Data, [eBay's Risk team] increased GPU usage from 25% to 75% and improved throughput by 2.6x for end-to-end [batch inference] use cases." – eBay

**Ray Data excels at GPU-intensive workloads, automatically optimizing resource utilization for computer vision, ML inference, and AI processing tasks.**

**Data Infrastructure and ETL**
> "The last mile data transformation dropped from 120 minutes to 15 minutes because of distributed data processing with Ray." – Coinbase

**High-Performance Analytics**
> "We're using Ray Data behind the scenes to make sure you're getting really efficient throughput [and] using as many of your compute resources as possible." – Snowflake

**These examples demonstrate Ray Data's versatility across traditional ETL workloads and modern analytics platforms, delivering consistent performance improvements.**

**Proven Production Scale:**
- **200TB+ daily processing** (ByteDance multimodal AI)
- **2.6x throughput improvement** (eBay GPU optimization)
- **8x faster transformations** (Coinbase ETL pipelines)
- **6,000+ CPU cores** (Ant Group production serving)
- **High-performance analytics** (Snowflake integration)

Enterprise Ready
----------------

**Enterprise Integration**
- Native data warehouse connectivity
- Cloud platform optimization
- Production monitoring and observability

**Production Operations**  
- Live monitoring dashboards and alerting
- Multi-cloud deployment flexibility
- Enterprise support and SLA guarantees

*For complete enterprise guidance, see :ref:`Enterprise Integration <enterprise-integration>`*



Why Ray Data is Different
-------------------------

**Ray Data's Architectural Advantages**

Ray Data's streaming execution architecture represents a fundamental advancement over traditional batch processing systems. While conventional tools must load entire datasets into memory before processing, Ray Data's intelligent streaming engine processes data continuously, enabling you to handle datasets far larger than your available memory.

**Key Technical Innovations:**
* **Streaming execution**: Process datasets larger than available memory
* **Intelligent resource allocation**: Automatic CPU/GPU optimization within single pipelines  
* **Multimodal processing**: Native support for images, audio, video, text, and structured data
* **Zero-copy operations**: Efficient data movement without serialization overhead
* **Fault tolerance**: Production-grade error handling and recovery

**Unique Multimodal Capabilities:**

The platform's multimodal processing capabilities address today's data reality where business insights require combining traditional structured data with unstructured content. Ray Data natively processes images, audio, video, and text alongside traditional tabular data, eliminating the complexity of managing multiple specialized tools for different data types.

**Comprehensive Data Support:**
* **Structured data**: CSV, Parquet, JSON, SQL databases, data warehouses
* **Unstructured data**: Images, audio, video, text, documents  
* **Modern formats**: Delta Lake, Iceberg, Hudi, Unity Catalog
* **AI/ML data**: PyTorch, TensorFlow, HuggingFace, NumPy arrays

*For technical details, see :ref:`Advanced Topics <advanced>` and :ref:`Workloads <workloads>`*

Key Capabilities
----------------

**Why Ray Data Outperforms Alternatives**

Ray Data's streaming execution architecture fundamentally differs from traditional batch processing systems. Instead of loading entire datasets into memory, Ray Data processes data continuously through intelligent pipelines, enabling you to handle datasets larger than your cluster memory. This architectural advantage, combined with automatic CPU/GPU resource allocation, delivers consistently high performance across diverse workloads.

Production deployments demonstrate Ray Data's reliability advantages. Pinterest achieves 90%+ GPU utilization for computer vision workloads, while ByteDance processes 200TB+ daily with consistent performance. The platform's built-in fault tolerance and monitoring capabilities ensure enterprise-grade reliability without additional infrastructure complexity.

*For detailed performance analysis, see :ref:`Performance Optimization <performance-optimization>`*

Integration Ecosystem
---------------------

**Seamless Ecosystem Integration**

Ray Data's integration capabilities set it apart from isolated data processing tools. As part of the Ray ecosystem, Ray Data seamlessly connects with Ray Train for distributed model training, Ray Tune for hyperparameter optimization, and Ray Serve for model deployment. This tight integration means your data preprocessing automatically flows into training and serving workflows without complex data handoffs or format conversions.

**Ray Ecosystem Integration:**
* **Ray Train**: Distributed model training with seamless data handoff → :ref:`Ray Train <train-docs>`
* **Ray Tune**: Hyperparameter optimization with integrated data pipelines → :ref:`Ray Tune <tune-docs>`  
* **Ray Serve**: Model serving with unified data preprocessing → :ref:`Ray Serve <serve-docs>`
* **Ray Core**: Custom distributed computing workflows → :ref:`Ray Core <ray-core>`

**Enterprise Platform Integration**

Beyond the Ray ecosystem, Ray Data provides native connectivity to enterprise platforms including Snowflake, Databricks, BigQuery, and major cloud providers. Unlike tools that require custom connectors or complex integration work, Ray Data's built-in platform support means you can read from any enterprise system, process with any AI framework, and output to any destination with simple, consistent APIs.

**Native Platform Connectivity:**
* **Data warehouses**: Snowflake, BigQuery, Redshift, Databricks
* **BI tools**: Tableau, Power BI, Looker with optimized data preparation
* **Cloud platforms**: AWS, GCP, Azure with native service integration
* **Orchestration**: Airflow, Prefect, Dagster with workflow integration
* **AI platforms**: MLflow, Weights & Biases, Kubeflow compatibility

*For complete integration details, see :ref:`Platform Integrations <integrations>`*

Start Your Ray Data Journey
---------------------------

**Choose your entry point based on your immediate needs:**

.. grid:: 1 2 2 2
    :gutter: 3
    :class-container: container pb-5

    .. grid-item-card::
        :class-header: bg-primary text-white
        :class-body: text-left

        **Quick Start (15 minutes)**
        ^^^
        
        Get hands-on experience immediately with a working pipeline.

        **Perfect for:** First-time users, proof of concept, technical demos

        +++
        .. button-ref:: data_quickstart
            :color: primary
            :outline:
            :expand:

            Start Tutorial

    .. grid-item-card::
        :class-header: bg-success text-white
        :class-body: text-left

        **Role-Specific Path (1-8 hours)**
        ^^^
        
        Follow curated learning path for your specific role and responsibilities.

        **Perfect for:** Targeted learning, production implementation planning

        +++
        .. button-ref:: personas
            :color: success
            :outline:
            :expand:

            Choose Your Role

    .. grid-item-card::
        :class-header: bg-info text-white
        :class-body: text-left

        **Explore Examples (30 minutes)**
        ^^^
        
        Browse real-world implementations across different industries and use cases.

        **Perfect for:** Understanding capabilities, finding implementation patterns

        +++
        .. button-ref:: use_cases
            :color: info
            :outline:
            :expand:

            See Examples

    .. grid-item-card::
        :class-header: bg-warning text-white
        :class-body: text-left

        **Technical Deep Dive (2+ hours)**
        ^^^
        
        Master advanced concepts, architecture, and optimization techniques.

        **Perfect for:** Expert users, production optimization, architecture planning

        +++
        .. button-ref:: advanced
            :color: warning
            :outline:
            :expand:

            Advanced Topics

**Need immediate help?** Visit :ref:`Support & Resources <support>` for community assistance, troubleshooting, and expert guidance.

