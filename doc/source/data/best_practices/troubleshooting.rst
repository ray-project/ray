.. _troubleshooting:

Troubleshooting Guide: Debugging Ray Data Issues
================================================

**Keywords:** Ray Data troubleshooting, debugging, performance issues, memory errors, out of memory, data quality, error handling, optimization, performance tuning, cluster issues

**Navigation:** :ref:`Ray Data <data>` → :ref:`Best Practices <best_practices>` → Troubleshooting Guide

This comprehensive troubleshooting guide helps you diagnose and resolve common issues with Ray Data deployments. Learn to identify performance bottlenecks, memory issues, data quality problems, and integration challenges with step-by-step debugging procedures.

**Quick Issue Resolution Matrix**

:::list-table
   :header-rows: 1

- - **Issue Type**
  - **Common Symptoms**
  - **Quick Diagnosis**
  - **Immediate Action**
- - Performance Issues
  - Slow processing, high latency
  - Check block sizes and resource allocation
  - Optimize configuration settings
- - Memory Problems
  - Out of memory errors, crashes
  - Monitor memory usage and block sizes
  - Reduce block size or enable streaming
- - Data Quality Issues
  - Incorrect results, validation failures
  - Inspect data samples and schemas
  - Add validation and error handling
- - Integration Problems
  - Connection failures, authentication errors
  - Test connectivity and credentials
  - Verify network and security settings
- - Cluster Issues
  - Node failures, scaling problems
  - Check cluster health and resources
  - Review cluster configuration

:::

**What you'll learn:**

* Common performance issues and optimization strategies
* Memory management and out-of-memory error resolution
* Data quality debugging and validation failures
* Integration and connectivity troubleshooting

Common Performance Issues
-------------------------

**Slow Processing Performance**

**Symptom:** Ray Data pipelines are running slower than expected.

**Diagnosis Steps:**

.. code-block:: python

    import ray
    from ray.data.context import DataContext

    # Check current configuration
    ctx = DataContext.get_current()
    print(f"Target max block size: {ctx.target_max_block_size}")
    print(f"Target min block size: {ctx.target_min_block_size}")
    
    # Monitor resource utilization
    def diagnose_performance(dataset):
        """Diagnose performance issues in dataset processing."""
        
        # Check dataset statistics
        print(f"Dataset size: {dataset.size_bytes()} bytes")
        print(f"Number of blocks: {dataset.num_blocks()}")
        print(f"Schema: {dataset.schema()}")
        
        # Profile execution
        start_time = time.time()
        result = dataset.take(1)  # Sample operation
        end_time = time.time()
        
        print(f"Sample operation took: {end_time - start_time:.2f} seconds")
        
        return result

**Common Causes and Solutions:**

1. **Suboptimal Block Sizes**
   
   .. code-block:: python
   
       # Problem: Blocks too small (too many tasks)
       # Solution: Increase block size
       ctx = DataContext.get_current()
       ctx.target_max_block_size = 256 * 1024 * 1024  # 256MB
       
       # Problem: Blocks too large (memory pressure)
       # Solution: Decrease block size
       ctx.target_max_block_size = 64 * 1024 * 1024   # 64MB

2. **Inefficient Transformations**
   
   .. code-block:: python
   
       # Problem: Row-by-row processing
       def slow_transform(row):
           # Processes one row at a time
           return row["value"] * 2
       
       slow_dataset = dataset.map(slow_transform)
       
       # Solution: Batch processing
       def fast_transform(batch):
           # Processes entire batch at once
           batch["value"] = batch["value"] * 2
           return batch
       
       fast_dataset = dataset.map_batches(fast_transform)

3. **Resource Contention**
   
   .. code-block:: python
   
       # Check cluster resources
       print(ray.cluster_resources())
       
       # Adjust resource allocation
       dataset.map_batches(
           transform_func,
           num_cpus=2,  # Allocate 2 CPUs per task
           num_gpus=0.5  # Allocate 0.5 GPU per task
       )

Memory Issues
-------------

**Out of Memory (OOM) Errors**

**Symptom:** `Ray.exceptions.OutOfMemoryError` or tasks being killed due to memory pressure.

**Diagnosis:**

.. code-block:: python

    import psutil
    import ray

    def diagnose_memory_issues():
        """Diagnose memory-related issues."""
        
        # Check system memory
        memory = psutil.virtual_memory()
        print(f"Total memory: {memory.total / (1024**3):.1f} GB")
        print(f"Available memory: {memory.available / (1024**3):.1f} GB")
        print(f"Memory usage: {memory.percent}%")
        
        # Check Ray object store
        object_store_stats = ray.cluster_resources()
        print(f"Object store memory: {object_store_stats.get('object_store_memory', 0) / (1024**3):.1f} GB")
        
        # Check for large objects in object store
        ray.internal.internal_api.memory_summary(stats_only=True)

**Solutions:**

1. **Reduce Block Size**
   
   .. code-block:: python
   
       # Reduce memory pressure by using smaller blocks
       from ray.data.context import DataContext
       
       ctx = DataContext.get_current()
       ctx.target_max_block_size = 32 * 1024 * 1024  # 32MB blocks
       
       # For very large datasets, use even smaller blocks
       ctx.target_max_block_size = 16 * 1024 * 1024  # 16MB blocks

2. **Enable Streaming Execution**
   
   .. code-block:: python
   
       # Process data in streaming fashion
       def memory_efficient_pipeline(input_path, output_path):
           """Memory-efficient pipeline using streaming execution."""
           
           # Load data lazily
           dataset = ray.data.read_parquet(input_path)
           
           # Apply transformations without materializing
           processed = dataset.map_batches(transform_batch)
           
           # Write directly without materializing entire dataset
           processed.write_parquet(output_path)
           
           # Don't call .take_all() or similar operations that materialize

3. **Optimize Data Types**
   
   .. code-block:: python
   
       def optimize_memory_usage(batch):
           """Optimize data types to reduce memory usage."""
           
           # Use smaller numeric types where possible
           if 'id' in batch.columns:
               batch['id'] = batch['id'].astype('int32')  # Instead of int64
           
           if 'amount' in batch.columns:
               batch['amount'] = batch['amount'].astype('float32')  # Instead of float64
           
           # Use categorical for repeated strings
           if 'category' in batch.columns:
               batch['category'] = batch['category'].astype('category')
           
           return batch

**Data Processing Hangs or Stalls**

**Symptom:** Ray Data pipeline appears to hang or makes no progress.

**Diagnosis:**

.. code-block:: python

    def diagnose_hanging_pipeline():
        """Diagnose hanging or stalled pipelines."""
        
        # Check Ray cluster status
        print("Cluster nodes:", ray.nodes())
        
        # Check for failed tasks
        try:
            # Try a simple operation with timeout
            import signal
            
            def timeout_handler(signum, frame):
                raise TimeoutError("Operation timed out")
            
            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(30)  # 30 second timeout
            
            # Test basic operation
            test_data = ray.data.range(10)
            result = test_data.take(5)
            
            signal.alarm(0)  # Cancel timeout
            print("Basic operations working")
            
        except TimeoutError:
            print("Pipeline is hanging - check for resource deadlocks")

**Solutions:**

1. **Check Resource Deadlocks**
   
   .. code-block:: python
   
       # Avoid resource deadlocks by setting appropriate limits
       dataset.map_batches(
           transform_func,
           compute=ray.data.ActorPoolStrategy(
               size=2,  # Don't use all available resources
               min_size=1
           ),
           num_cpus=1  # Leave resources for other operations
       )

2. **Implement Timeouts**
   
   .. code-block:: python
   
       def robust_pipeline_with_timeout():
           """Implement pipeline with timeout and retry logic."""
           
           import time
           from concurrent.futures import TimeoutError
           
           try:
               # Set execution timeout
               start_time = time.time()
               timeout_seconds = 3600  # 1 hour timeout
               
               dataset = ray.data.read_parquet("s3://bucket/data/")
               
               def check_timeout():
                   if time.time() - start_time > timeout_seconds:
                       raise TimeoutError("Pipeline execution exceeded timeout")
               
               # Process with periodic timeout checks
               result = dataset.map_batches(
                   lambda batch: transform_with_timeout_check(batch, check_timeout)
               )
               
               return result
               
           except TimeoutError:
               print("Pipeline timed out - check for hanging operations")
               # Implement retry or fallback logic

Data Quality Issues
-------------------

**Schema Validation Failures**

**Symptom:** Data doesn't match expected schema, causing processing errors.

**Diagnosis:**

.. code-block:: python

    def diagnose_schema_issues(dataset):
        """Diagnose schema-related issues."""
        
        # Check actual schema
        actual_schema = dataset.schema()
        print(f"Actual schema: {actual_schema}")
        
        # Sample data to inspect
        sample = dataset.take(5)
        print(f"Sample data: {sample}")
        
        # Check for schema inconsistencies across blocks
        def check_block_schemas(batch):
            return {
                'columns': list(batch.columns),
                'dtypes': {col: str(dtype) for col, dtype in batch.dtypes.items()},
                'row_count': len(batch)
            }
        
        schema_info = dataset.map_batches(check_block_schemas).take_all()
        
        # Look for inconsistencies
        unique_schemas = set(str(info) for info in schema_info)
        if len(unique_schemas) > 1:
            print("Warning: Inconsistent schemas detected across blocks")
            for i, schema in enumerate(unique_schemas):
                print(f"Schema variant {i}: {schema}")

**Solutions:**

1. **Schema Enforcement**
   
   .. code-block:: python
   
       import pyarrow as pa
       
       # Define expected schema
       expected_schema = pa.schema([
           pa.field("id", pa.int64()),
           pa.field("name", pa.string()),
           pa.field("amount", pa.float64()),
           pa.field("date", pa.timestamp('s'))
       ])
       
       # Load with schema enforcement
       dataset = ray.data.read_parquet(
           "s3://bucket/data/",
           schema=expected_schema
       )

2. **Schema Normalization**
   
   .. code-block:: python
   
       def normalize_schema(batch):
           """Normalize schema across different data sources."""
           
           # Handle missing columns
           expected_columns = ['id', 'name', 'amount', 'date']
           for col in expected_columns:
               if col not in batch.columns:
                   batch[col] = None
           
           # Standardize data types
           if 'id' in batch.columns:
               batch['id'] = pd.to_numeric(batch['id'], errors='coerce')
           
           if 'date' in batch.columns:
               batch['date'] = pd.to_datetime(batch['date'], errors='coerce')
           
           # Reorder columns
           batch = batch[expected_columns]
           
           return batch
       
       normalized_dataset = dataset.map_batches(normalize_schema)

**Data Corruption Issues**

**Symptom:** Invalid or corrupted data causing processing failures.

**Diagnosis and Solutions:**

.. code-block:: python

    def handle_data_corruption():
        """Handle and recover from data corruption issues."""
        
        def robust_data_processing(batch):
            """Process data with corruption handling."""
            
            original_size = len(batch)
            
            # Remove rows with all null values
            batch = batch.dropna(how='all')
            
            # Handle specific corruption patterns
            if 'amount' in batch.columns:
                # Remove negative amounts if they shouldn't exist
                batch = batch[batch['amount'] >= 0]
                
                # Handle infinite values
                batch = batch[~batch['amount'].isin([float('inf'), float('-inf')])]
            
            # Log data quality issues
            cleaned_size = len(batch)
            if cleaned_size < original_size:
                print(f"Removed {original_size - cleaned_size} corrupted records")
            
            return batch
        
        # Apply robust processing
        cleaned_dataset = dataset.map_batches(robust_data_processing)
        
        return cleaned_dataset

Integration Issues
------------------

**Database Connection Problems**

**Symptom:** Unable to connect to databases or frequent connection timeouts.

**Diagnosis:**

.. code-block:: python

    def diagnose_database_connection():
        """Diagnose database connection issues."""
        
        import sqlalchemy
        
        try:
            # Test direct connection
            engine = sqlalchemy.create_engine(
                "postgresql://user:pass@host:5432/db",
                pool_pre_ping=True,
                pool_recycle=3600
            )
            
            with engine.connect() as conn:
                result = conn.execute(sqlalchemy.text("SELECT 1"))
                print("Database connection successful")
                
        except Exception as e:
            print(f"Database connection failed: {e}")
            
            # Check common issues
            print("Troubleshooting steps:")
            print("1. Verify connection string")
            print("2. Check network connectivity")
            print("3. Verify credentials")
            print("4. Check firewall settings")

**Solutions:**

1. **Connection Pooling and Retry Logic**
   
   .. code-block:: python
   
       def robust_database_read(query, connection_string):
           """Read from database with retry logic."""
           
           import time
           from sqlalchemy import create_engine
           
           max_retries = 3
           retry_delay = 5
           
           for attempt in range(max_retries):
               try:
                   # Create engine with connection pooling
                   engine = create_engine(
                       connection_string,
                       pool_size=5,
                       max_overflow=10,
                       pool_pre_ping=True,
                       pool_recycle=3600
                   )
                   
                   # Read data
                   dataset = ray.data.read_sql(connection_string, query)
                   return dataset
                   
               except Exception as e:
                   print(f"Database read attempt {attempt + 1} failed: {e}")
                   if attempt < max_retries - 1:
                       time.sleep(retry_delay)
                   else:
                       raise

2. **Batch Size Optimization**
   
   .. code-block:: python
   
       # For large tables, use smaller batch sizes to avoid timeouts
       dataset = ray.data.read_sql(
           connection_string,
           query,
           parallelism=10,  # Create 10 parallel connections
       )

**Cloud Storage Access Issues**

**Symptom:** Unable to access S3, GCS, or Azure storage.

**Solutions:**

.. code-block:: python

    def diagnose_cloud_storage():
        """Diagnose cloud storage access issues."""
        
        import boto3
        from botocore.exceptions import ClientError
        
        try:
            # Test S3 access
            s3_client = boto3.client('s3')
            response = s3_client.list_objects_v2(
                Bucket='your-bucket',
                MaxKeys=1
            )
            print("S3 access successful")
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            
            if error_code == 'NoCredentialsError':
                print("AWS credentials not configured")
                print("Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
            elif error_code == 'AccessDenied':
                print("Access denied - check IAM permissions")
            else:
                print(f"S3 error: {e}")

Performance Monitoring
----------------------

**Live Performance Monitoring**

.. code-block:: python

    import time
    import psutil
    import ray

    class PerformanceMonitor:
        """Monitor Ray Data pipeline performance."""
        
        def __init__(self):
            self.metrics = []
            
        def monitor_pipeline(self, dataset, operation_name):
            """Monitor pipeline execution with detailed metrics."""
            
            def monitored_operation(batch):
                start_time = time.time()
                start_memory = psutil.virtual_memory().used
                
                # Your actual processing logic here
                result = self.process_batch(batch)
                
                end_time = time.time()
                end_memory = psutil.virtual_memory().used
                
                # Collect metrics
                metrics = {
                    'operation': operation_name,
                    'timestamp': start_time,
                    'duration': end_time - start_time,
                    'memory_delta': end_memory - start_memory,
                    'input_rows': len(batch),
                    'output_rows': len(result),
                    'throughput_rows_per_sec': len(batch) / (end_time - start_time)
                }
                
                self.metrics.append(metrics)
                
                # Alert on performance degradation
                if metrics['duration'] > 30:  # 30 second threshold
                    print(f"Performance alert: {operation_name} took {metrics['duration']:.2f}s")
                
                return result
            
            return dataset.map_batches(monitored_operation)
        
        def process_batch(self, batch):
            """Placeholder for actual batch processing logic."""
            return batch
        
        def get_performance_summary(self):
            """Generate performance summary report."""
            
            if not self.metrics:
                return "No performance data collected"
            
            import pandas as pd
            df = pd.DataFrame(self.metrics)
            
            summary = {
                'total_operations': len(df),
                'avg_duration': df['duration'].mean(),
                'max_duration': df['duration'].max(),
                'avg_throughput': df['throughput_rows_per_sec'].mean(),
                'total_memory_used': df['memory_delta'].sum()
            }
            
            return summary

Best Practices for Troubleshooting
-----------------------------------

**1. Enable Comprehensive Logging**

.. code-block:: python

    import logging
    
    # Configure Ray Data logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("ray.data")
    logger.setLevel(logging.DEBUG)

**2. Use Progressive Debugging**

.. code-block:: python

    # Start with small data samples
    sample_dataset = dataset.limit(100)
    
    # Test transformations on samples first
    result = sample_dataset.map_batches(your_transform).take_all()
    
    # Scale up gradually
    medium_dataset = dataset.limit(10000)
    # ... then full dataset

**3. Monitor Resource Usage**

.. code-block:: python

    # Monitor during execution
    def resource_aware_processing():
        while processing:
            memory_usage = psutil.virtual_memory().percent
            cpu_usage = psutil.cpu_percent()
            
            if memory_usage > 90:
                print("Warning: High memory usage")
            if cpu_usage > 95:
                print("Warning: High CPU usage")

**4. Implement Circuit Breakers**

.. code-block:: python

    class CircuitBreaker:
        def __init__(self, failure_threshold=5, timeout=60):
            self.failure_count = 0
            self.failure_threshold = failure_threshold
            self.timeout = timeout
            self.last_failure_time = None
            
        def call(self, func, *args, **kwargs):
            if self.failure_count >= self.failure_threshold:
                if time.time() - self.last_failure_time < self.timeout:
                    raise Exception("Circuit breaker is open")
                else:
                    self.failure_count = 0  # Reset after timeout
            
            try:
                result = func(*args, **kwargs)
                self.failure_count = 0  # Reset on success
                return result
            except Exception as e:
                self.failure_count += 1
                self.last_failure_time = time.time()
                raise

Next Steps
----------

* **Monitoring & Observability**: Set up proactive monitoring → :ref:`monitoring-observability`
* **Production Deployment**: Deploy with proper error handling → :ref:`production-deployment`
* **Data Quality**: Implement comprehensive quality checks → :ref:`data-quality-governance`