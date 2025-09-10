.. _workloads:

Workloads: Data Processing by Use Case
======================================

**Keywords:** Ray Data workloads, any data type, any AI framework, any compute, universal data processing, multimodal processing

Process **any data type** with **any AI framework** on **any compute infrastructure**. This section organizes Ray Data guides by workload type, making it easy to find guidance for your specific data processing needs - whether traditional business data or cutting-edge AI.

.. toctree::
   :maxdepth: 2

   working-with-ai
   working-with-pytorch
   working-with-llms
   batch_inference
   working-with-tabular-data
   working-with-images
   working-with-text
   working-with-tensors
   working-with-audio
   working-with-video
   working-with-time-series
   working-with-graphs
   working-with-lakehouse-formats

Overview
--------

Ray Data handles **any workload** - from traditional business analytics to cutting-edge multimodal AI. Use **any AI framework**, process **any data type**, deploy on **any compute infrastructure**. These guides help you optimize Ray Data for your specific needs.

**Workload Categories**

**AI & Machine Learning Workloads**
Process data for training, inference, and AI applications with GPU optimization and framework integration.

**Structured Data Workloads**
Handle traditional business data, analytics, and reporting with SQL-style operations and business intelligence patterns.

**Unstructured Data Workloads**
Process images, audio, video, and text with specialized algorithms and multimodal capabilities.

**Specialized Workloads**
Work with scientific data, time series, graphs, and modern data formats with optimized processing patterns.

**Workload Selection Guide**

Choose your workload guide based on your primary data processing goals:

:::list-table
   :header-rows: 1

- - **Your Goal**
  - **Primary Data Types**
  - **Recommended Guide**
  - **Key Capabilities**
- - **AI Model Training**
  - Images, text, structured data
  - :ref:`Working with AI <working-with-ai>`
  - Multimodal processing, GPU optimization
- - **PyTorch Integration**
  - Tensors, images, structured data
  - :ref:`Working with PyTorch <working-with-pytorch>`
  - Distributed training, tensor processing
- - **Large Language Models**
  - Text, documents, conversations
  - :ref:`Working with LLMs <working-with-llms>`
  - Text processing, inference optimization
- - **Batch Model Inference**
  - Any data type for ML models
  - :ref:`Batch Inference <batch_inference>`
  - High-throughput model serving
- - **Business Analytics**
  - Tabular, CSV, database data
  - :ref:`Working with Tabular Data <working-with-tabular-data>`
  - SQL-style operations, aggregations
- - **Computer Vision**
  - Images, videos, visual data
  - :ref:`Working with Images <working-with-images>`
  - Image processing, computer vision
- - **Natural Language Processing**
  - Text, documents, language data
  - :ref:`Working with Text <working-with-text>`
  - Text processing, NLP pipelines
- - **Scientific Computing**
  - Numerical arrays, scientific data
  - :ref:`Working with Tensors <working-with-tensors>`
  - NumPy integration, mathematical operations
- - **Audio Processing**
  - Audio files, speech, music
  - :ref:`Working with Audio <working-with-audio>`
  - Audio analysis, speech processing
- - **Video Analysis**
  - Video files, streaming media
  - :ref:`Working with Video <working-with-video>`
  - Video processing, frame analysis
- - **Time Series Analytics**
  - Time-stamped data, forecasting
  - :ref:`Working with Time Series <working-with-time-series>`
  - Temporal analysis, forecasting
- - **Graph Analytics**
  - Network data, relationships
  - :ref:`Working with Graphs <working-with-graphs>`
  - Graph algorithms, network analysis
- - **Modern Data Formats**
  - Delta Lake, Iceberg, Hudi
  - :ref:`Working with Lakehouse Formats <working-with-lakehouse-formats>`
  - ACID transactions, time travel

:::

**Quick Start by Workload Type**

**AI & ML Workloads (15-30 minutes):**
- **New to AI**: Start with :ref:`Working with AI <working-with-ai>` for multimodal processing
- **PyTorch users**: Try :ref:`Working with PyTorch <working-with-pytorch>` for distributed training
- **LLM focus**: Explore :ref:`Working with LLMs <working-with-llms>` for text processing

**Business & Analytics Workloads (15-20 minutes):**
- **Business data**: Start with :ref:`Working with Tabular Data <working-with-tabular-data>`
- **Time series**: Try :ref:`Working with Time Series <working-with-time-series>`
- **Modern formats**: Explore :ref:`Working with Lakehouse Formats <working-with-lakehouse-formats>`

**Media & Content Workloads (20-30 minutes):**
- **Images**: Start with :ref:`Working with Images <working-with-images>`
- **Text**: Try :ref:`Working with Text <working-with-text>`
- **Audio/Video**: Explore :ref:`Working with Audio <working-with-audio>` or :ref:`Working with Video <working-with-video>`

**Workload Optimization Patterns**

**High-Performance Workloads:**
- **GPU acceleration**: Use `num_gpus=1` for AI, image, and video processing
- **Large batch sizes**: Optimize batch sizes for vectorized operations
- **Resource allocation**: Match CPU/GPU resources to workload characteristics
- **Memory optimization**: Configure block sizes for optimal performance

**Cost-Optimized Workloads:**
- **Resource efficiency**: Use appropriate instance types and scaling
- **Batch processing**: Optimize for throughput over latency
- **Storage optimization**: Use efficient formats and compression
- **Scheduling optimization**: Use spot instances and off-peak processing

**Scalable Workloads:**
- **Horizontal scaling**: Design for linear scaling across nodes
- **Streaming execution**: Handle datasets larger than cluster memory
- **Fault tolerance**: Implement appropriate error handling and recovery
- **Monitoring**: Use Ray Dashboard and metrics for performance tracking

Next Steps
----------

**Choose Your Workload:**

**For AI & ML:**
→ Start with :ref:`Working with AI <working-with-ai>` for comprehensive AI capabilities

**For Business Data:**
→ Begin with :ref:`Working with Tabular Data <working-with-tabular-data>` for analytics

**For Unstructured Data:**
→ Try :ref:`Working with Images <working-with-images>` or :ref:`Working with Text <working-with-text>`

**For Production:**
→ Apply workload knowledge to :ref:`Best Practices <best_practices>` for deployment

**For Advanced Optimization:**
→ Explore :ref:`Performance Optimization <performance-optimization>` for your workload type