.. _monitoring-overview:

Monitoring & Observability: Ray Dashboard and Metrics
=====================================================

**Keywords:** Ray Data monitoring, Ray Dashboard, Grafana metrics, performance monitoring, workload observability, pipeline debugging

Monitor and debug Ray Data workloads using the Ray Dashboard, Grafana metrics, and Anyscale's advanced Ray Data Dashboard. This guide covers the built-in monitoring capabilities available for Ray Data performance optimization and troubleshooting.

**What you'll learn:**

* Monitor Ray Data workloads using the Ray Dashboard
* Use Grafana dashboards for Ray Data metrics visualization
* Debug performance issues with built-in observability tools
* Access advanced monitoring capabilities on Anyscale platform

Ray Dashboard for Ray Data Monitoring
-------------------------------------

**The Ray Dashboard provides built-in monitoring for all Ray Data workloads running on Ray clusters.**

**Accessing the Ray Dashboard:**

The Ray Dashboard is automatically available when you start a Ray cluster:

.. code-block:: python

    import ray

    # Ray Dashboard available at http://localhost:8265 by default
    ray.init()
    
    # Run your Ray Data workload
    ds = ray.data.read_parquet("s3://data/")
    result = ds.map_batches(transform_function).materialize()

**Key Ray Dashboard Features for Ray Data:**

**1. Dataset Overview**
- **Progress tracking**: Real-time progress of data processing operations
- **Resource utilization**: CPU, GPU, and memory usage across cluster
- **Block statistics**: Number of blocks processed and remaining
- **Throughput metrics**: Rows per second and data processing rates

**2. Operator-Level Metrics**
- **Individual operator performance**: Processing time for each operation
- **Resource allocation**: CPU/GPU usage per operator
- **Queue status**: Input and output queue lengths
- **Backpressure indicators**: When operators are throttled or stalled

**3. Cluster Resource Monitoring**
- **Node health**: Status of all cluster nodes
- **Resource availability**: Available CPU, GPU, memory across cluster
- **Task distribution**: How Ray Data tasks are distributed across nodes
- **Object store usage**: Memory usage in Ray's distributed object store

**Basic Ray Dashboard Usage:**

.. code-block:: python

    # Monitor a Ray Data pipeline through the dashboard
    import ray
    
    # Start Ray with dashboard (default port 8265)
    ray.init(include_dashboard=True)
    
    # Run workload - monitor at http://localhost:8265
    large_dataset = ray.data.read_parquet("s3://large-data/")
    processed = large_dataset.map_batches(expensive_transform)
    result = processed.write_parquet("s3://output/")

**What to monitor in the Ray Dashboard:**
- **Progress bars**: Overall pipeline progress and completion estimates
- **Resource graphs**: CPU/GPU utilization to identify bottlenecks
- **Memory usage**: Object store usage to prevent spilling
- **Task timeline**: Task execution patterns and scheduling efficiency

Grafana Dashboards for Ray Data
-------------------------------

**Ray provides pre-built Grafana dashboards for comprehensive Ray Data monitoring.**

**Ray Data Grafana Dashboard Features:**

**1. Dataset-Level Metrics**
- **Output metrics**: Rows, blocks, bytes processed over time
- **Iteration metrics**: Dataset iteration performance and efficiency
- **Throughput tracking**: Processing rates and performance trends

**2. Operator-Level Metrics**
- **Internal queues**: Queue lengths and processing bottlenecks
- **Object store usage**: Memory usage patterns and optimization opportunities
- **Resource utilization**: CPU/GPU usage per operator over time

**3. System-Level Monitoring**
- **Cluster health**: Overall cluster performance and resource utilization
- **Network metrics**: Data transfer patterns and network bottlenecks
- **Storage metrics**: I/O performance and storage access patterns

**Setting up Grafana for Ray Data:**

.. code-block:: bash

    # Ray automatically exposes metrics for Grafana
    # Default metrics endpoint: http://localhost:8080/metrics
    
    # Configure Grafana to scrape Ray metrics
    # Add Ray cluster as Prometheus target in Grafana

**Key Metrics to Monitor:**
- **`ray_data_output_rows_total`**: Total rows processed by datasets
- **`ray_data_output_bytes_total`**: Total bytes processed
- **`ray_data_cpu_usage`**: CPU utilization for Ray Data operations
- **`ray_data_gpu_usage`**: GPU utilization for AI/ML workloads

Performance Monitoring Best Practices
------------------------------------

**Use the Ray Dashboard for real-time monitoring and Grafana for historical analysis.**

**Real-Time Monitoring with Ray Dashboard:**

.. code-block:: python

    # Monitor pipeline performance in real-time
    import ray
    
    # Enable verbose progress reporting
    from ray.data.context import DataContext
    ctx = DataContext.get_current()
    ctx.enable_progress_bars = True
    ctx.verbose_stats_logs = True
    
    # Run workload with detailed monitoring
    result = ray.data.read_parquet("s3://data/") \
        .map_batches(transform_function) \
        .write_parquet("s3://output/")
    
    # View detailed statistics
    print(result.stats())

**Historical Analysis with Grafana:**
- **Performance trends**: Track performance over time to identify degradation
- **Resource utilization**: Monitor CPU/GPU efficiency across different workloads
- **Cost optimization**: Identify opportunities to reduce resource usage
- **Capacity planning**: Understand resource requirements for scaling

**Monitoring Different Workload Types:**

.. code-block:: python

    # ETL workloads: Monitor throughput and data quality
    etl_result = ray.data.read_csv("s3://raw-data/") \
        .filter(lambda row: row["valid"]) \
        .map_batches(clean_data) \
        .write_snowflake(table="clean_data", connection_parameters=config)
    
    # AI/ML workloads: Monitor GPU utilization and memory usage
    ml_result = ray.data.read_images("s3://images/") \
        .map_batches(ai_inference, num_gpus=1, batch_size=32) \
        .write_parquet("s3://predictions/")

Advanced Monitoring with Anyscale
---------------------------------

**Anyscale provides advanced Ray Data Dashboard capabilities beyond open-source monitoring.**

**Anyscale Ray Data Dashboard Features:**

**1. Pipeline Visualization**
- **Tree view**: Visual representation of complex data pipelines
- **DAG view**: Directed acyclic graph visualization of operation flow
- **Operation drilldown**: Click on operators to see detailed metrics

**2. Advanced Debugging**
- **Dataset-aware logs**: Logs automatically associated with specific datasets
- **Error attribution**: Clear identification of error sources and affected data
- **Bottleneck identification**: Automatic identification of performance bottlenecks

**3. Persistent Monitoring**
- **Historical data**: Access monitoring data even after job completion
- **Session management**: View past sessions and compare performance
- **Trend analysis**: Long-term performance and resource usage trends

**Accessing Anyscale Monitoring:**
- **Workspaces**: Available in Anyscale workspaces for development
- **Jobs**: Integrated with Anyscale jobs for production monitoring
- **Services**: Available for Ray Data used in serving applications

Troubleshooting with Monitoring Tools
------------------------------------

**Use monitoring tools to quickly identify and resolve performance issues.**

**Common Issues and Monitoring Solutions:**

**Issue 1: Slow Performance**
- **Ray Dashboard**: Check CPU/GPU utilization graphs
- **Grafana**: Review historical performance trends
- **Solution**: Increase resources or optimize batch sizes

**Issue 2: Memory Issues**
- **Ray Dashboard**: Monitor object store memory usage
- **Look for**: Memory usage spikes or spilling indicators
- **Solution**: Reduce batch sizes or increase cluster memory

**Issue 3: Pipeline Stalls**
- **Ray Dashboard**: Check operator queue lengths and backpressure
- **Anyscale Dashboard**: Use tree view to identify bottleneck operators
- **Solution**: Balance resource allocation across pipeline stages

**Issue 4: Resource Waste**
- **Grafana**: Monitor resource utilization over time
- **Look for**: Low CPU/GPU utilization or idle resources
- **Solution**: Increase concurrency or optimize resource allocation

**Monitoring Configuration Checklist**

**Basic Monitoring Setup:**
- [ ] **Ray Dashboard enabled**: Accessible at http://localhost:8265
- [ ] **Progress bars enabled**: Real-time progress visibility
- [ ] **Verbose logging**: Detailed statistics and execution information
- [ ] **Resource monitoring**: CPU, GPU, memory utilization tracking

**Advanced Monitoring Setup:**
- [ ] **Grafana configured**: Historical metrics collection and visualization
- [ ] **Custom metrics**: Application-specific metrics for business KPIs
- [ ] **Alerting configured**: Notifications for performance degradation
- [ ] **Log aggregation**: Centralized logging for troubleshooting

**Production Monitoring:**
- [ ] **Automated monitoring**: Continuous monitoring without manual intervention
- [ ] **Performance baselines**: Established baselines for performance comparison
- [ ] **Capacity planning**: Resource usage trends for scaling decisions
- [ ] **Cost tracking**: Resource usage and cost optimization monitoring

Monitoring Integration Patterns
------------------------------

**Integrate Ray Data monitoring with existing observability infrastructure.**

**Basic Integration Example:**

.. code-block:: python

    # Ray Data with built-in monitoring
    import ray
    
    # Configure for monitoring
    ray.init(include_dashboard=True)
    
    # Your Ray Data pipeline automatically appears in dashboard
    result = ray.data.read_parquet("s3://data/") \
        .map_batches(process_data) \
        .write_parquet("s3://output/")

**Enterprise Integration:**
- **Grafana**: Use Ray Data metrics in existing Grafana dashboards
- **Prometheus**: Ray automatically exposes metrics for Prometheus scraping
- **Alerting**: Set up alerts based on Ray Data performance metrics
- **Log aggregation**: Integrate Ray Data logs with enterprise logging systems

**Anyscale Platform Integration:**
- **Advanced dashboards**: Tree and DAG views for complex pipeline visualization
- **Persistent monitoring**: Historical data access and trend analysis
- **Automated debugging**: Built-in issue detection and resolution guidance

Next Steps
----------

**Master Ray Data Monitoring:**

**For Basic Monitoring:**
→ Use Ray Dashboard for real-time pipeline monitoring and troubleshooting

**For Advanced Analytics:**
→ Set up Grafana dashboards for historical performance analysis

**For Production Deployment:**
→ Integrate with enterprise monitoring infrastructure

**For Advanced Capabilities:**
→ Consider Anyscale platform for advanced Ray Data Dashboard features

**For Performance Optimization:**
→ Apply monitoring insights to :ref:`Performance Optimization <performance-optimization>`
