.. _installation-setup:

Installation & Setup: Getting Started with Ray Data
===================================================

**Keywords:** Ray Data installation, setup, pip install, Python data processing, distributed computing setup, cluster configuration, cloud deployment, local installation, requirements

**Navigation:** :ref:`Ray Data <data>` → Installation & Setup

**Learning Path:** Start here if you're new to Ray Data. After installation, continue with :ref:`Quickstart Guide <data_quickstart>` for hands-on learning.

Ray Data is a unified data processing engine that handles any type of data workload - from traditional ETL and business intelligence to cutting-edge AI and machine learning. As part of the :ref:`Ray ecosystem <ray-overview>`, Ray Data integrates seamlessly with :ref:`Ray Train <train-docs>`, :ref:`Ray Tune <tune-docs>`, and :ref:`Ray Serve <serve-docs>` for complete AI and data workflows.

This guide covers installation options, configuration, and setup for different deployment scenarios.

**What you'll learn:**

* Installation options for different environments and use cases
* Configuration for optimal performance and resource utilization
* Setup patterns for development, testing, and production environments
* Troubleshooting common installation and setup issues

**Quick Installation Checklist**

Before you begin, ensure you have:

- [ ] **Python 3.8+** installed on your system
- [ ] **pip package manager** available and updated
- [ ] **Internet connection** for downloading packages
- [ ] **Sufficient disk space** (minimum 2GB for full installation)
- [ ] **Administrative privileges** if installing system-wide

**Installation Decision Matrix**

Choose your installation type based on your use case:

:::list-table
   :header-rows: 1

- - **Use Case**
  - **Installation Command**
  - **Size**
  - **Best For**
- - Basic Data Processing
  - ``pip install 'ray[data]'``
  - ~500MB
  - ETL, BI, basic analytics
- - Machine Learning Workloads
  - ``pip install 'ray[data,train,tune,serve]'``
  - ~800MB
  - AI/ML, training, inference
- - Complete Ray Ecosystem
  - ``pip install 'ray[data,default]'``
  - ~1.2GB
  - All features, cloud integration
- - Development & Testing
  - ``pip install 'ray[data,test]'``
  - ~600MB
  - Development, testing, debugging

:::

Installation Options
--------------------

**Basic Installation**

Ray Data is included with Ray and can be installed with a single command:

.. code-block:: bash

    pip install -U 'ray[data]'

This installs Ray Data with core dependencies optimized for most data processing workloads.

**Installation with ML Dependencies**

For machine learning and AI workloads, install with additional ML libraries:

.. code-block:: bash

    pip install -U 'ray[data,train,tune,serve]'

This includes Ray Train for distributed training, Ray Tune for hyperparameter tuning, and Ray Serve for model serving.

**Installation with All Dependencies**

For comprehensive data processing with all supported formats:

.. code-block:: bash

    pip install -U 'ray[data,default]'

This includes additional dependencies for specialized data formats and cloud integrations.

**Cloud-Specific Installations**

For cloud deployments, install with cloud-specific dependencies:

.. code-block:: bash

    # AWS
    pip install -U 'ray[data]' boto3 s3fs

    # GCP  
    pip install -U 'ray[data]' google-cloud-storage gcsfs

    # Azure
    pip install -U 'ray[data]' azure-storage-blob adlfs

**Development Environment Setup**

For development and testing:

.. code-block:: bash

    pip install -U 'ray[data]' pytest jupyter pandas pyarrow

Environment Configuration
-------------------------

**Ray Data Context Configuration**

Configure Ray Data for optimal performance:

.. code-block:: python

    import ray
    from ray.data.context import DataContext

    # Get current context
    ctx = DataContext.get_current()

    # Configure for different workload types
    
    # For large analytical workloads
    ctx.target_max_block_size = 256 * 1024 * 1024  # 256MB blocks
    ctx.target_min_block_size = 128 * 1024 * 1024  # 128MB minimum
    
    # For memory-constrained environments
    ctx.target_max_block_size = 64 * 1024 * 1024   # 64MB blocks
    ctx.target_min_block_size = 32 * 1024 * 1024   # 32MB minimum
    
    # For GPU workloads
    ctx.target_max_block_size = 128 * 1024 * 1024  # 128MB blocks
    ctx.enable_tensor_extension_casting = True     # Enable tensor support

**Resource Configuration**

Configure cluster resources for different workload types:

.. code-block:: python

    # Initialize Ray cluster with specific resources
    ray.init(
        num_cpus=16,           # CPU cores available
        num_gpus=4,            # GPU devices available  
        object_store_memory=8 * 1024**3,  # 8GB object store
        dashboard_host='0.0.0.0'  # Enable dashboard access
    )

    # Configure execution options
    from ray.data import ExecutionOptions, ExecutionResources

    execution_options = ExecutionOptions(
        resource_limits=ExecutionResources(
            cpu=12,  # Limit CPU usage
            gpu=2,   # Limit GPU usage
            object_store_memory=6 * 1024**3  # Limit object store usage
        ),
        preserve_order=True,  # Maintain data ordering
        actor_locality_enabled=True  # Enable data locality optimization
    )

    # Apply execution options
    ctx.execution_options = execution_options

**Performance Tuning Configuration**

Optimize Ray Data for specific performance characteristics:

.. code-block:: python

    # For high-throughput workloads
    ctx.enable_progress_bars = False  # Reduce overhead
    ctx.enable_auto_log_stats = False  # Reduce logging overhead
    ctx.actor_prefetcher_enabled = True  # Enable prefetching
    
    # For memory-intensive workloads
    ctx.eager_free = True  # Enable eager memory cleanup
    ctx.use_push_based_shuffle = True  # Optimize shuffle operations
    
    # For fault-tolerant workloads
    ctx.max_errored_blocks = 10  # Allow up to 10 block failures
    ctx.actor_task_retry_on_errors = True  # Enable task retries

Development Setup
-----------------

**Local Development Environment**

Set up Ray Data for local development and testing:

.. code-block:: python

    import ray

    # Initialize Ray for local development
    ray.init(
        num_cpus=4,  # Use 4 CPU cores
        num_gpus=0,  # No GPU for local testing
        object_store_memory=2 * 1024**3,  # 2GB object store
        ignore_reinit_error=True  # Allow re-initialization
    )

    # Configure for development
    from ray.data.context import DataContext
    
    ctx = DataContext.get_current()
    ctx.enable_progress_bars = True  # Show progress in development
    ctx.enable_auto_log_stats = True  # Enable detailed logging
    ctx.target_max_block_size = 64 * 1024 * 1024  # Smaller blocks for testing

**Testing Configuration**

Configure Ray Data for automated testing:

.. code-block:: python

    import pytest
    import ray

    @pytest.fixture(scope="session")
    def ray_cluster():
        """Initialize Ray cluster for testing."""
        ray.init(
            num_cpus=2,
            object_store_memory=1 * 1024**3,
            ignore_reinit_error=True
        )
        yield
        ray.shutdown()

    @pytest.fixture
    def small_test_data():
        """Create small test dataset."""
        return ray.data.range(100)

Production Setup
----------------

**Production Cluster Configuration**

Configure Ray Data for production deployments:

.. code-block:: python

    # Production cluster initialization
    ray.init(
        address="ray://production-cluster:10001",  # Connect to existing cluster
        runtime_env={
            "pip": ["ray[data]==2.9.0", "pandas==2.0.3", "pyarrow==13.0.0"],
            "env_vars": {
                "RAY_DATA_STRICT_MODE": "1",
                "RAY_DATA_ENABLE_PROGRESS_BARS": "0"
            }
        }
    )

    # Production-optimized configuration
    ctx = DataContext.get_current()
    ctx.target_max_block_size = 256 * 1024 * 1024  # Large blocks for throughput
    ctx.enable_progress_bars = False  # Reduce overhead
    ctx.enable_auto_log_stats = True  # Enable monitoring
    ctx.max_errored_blocks = 5  # Limited error tolerance
    ctx.actor_task_retry_on_errors = ["ConnectionError", "TimeoutError"]

**Cloud Production Setup**

Configure for cloud production deployments:

.. code-block:: python

    # AWS production setup
    import boto3
    
    # Configure AWS credentials
    session = boto3.Session()
    credentials = session.get_credentials()
    
    ray.init(
        address="ray://aws-cluster:10001",
        runtime_env={
            "env_vars": {
                "AWS_ACCESS_KEY_ID": credentials.access_key,
                "AWS_SECRET_ACCESS_KEY": credentials.secret_key,
                "AWS_SESSION_TOKEN": credentials.token,
                "AWS_DEFAULT_REGION": "us-west-2"
            }
        }
    )

**Monitoring and Observability Setup**

Enable comprehensive monitoring for production:

.. code-block:: python

    # Enable detailed monitoring
    ctx.enable_per_node_metrics = True
    ctx.memory_usage_poll_interval_s = 30  # Monitor memory every 30 seconds
    ctx.verbose_stats_logs = True  # Detailed statistics
    
    # Configure custom logging
    import logging
    
    # Set up Ray Data logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('/var/log/ray-data.log'),
            logging.StreamHandler()
        ]
    )
    
    # Enable Ray Data specific logging
    ray_data_logger = logging.getLogger("ray.data")
    ray_data_logger.setLevel(logging.DEBUG)

Troubleshooting Installation
----------------------------

**Common Installation Issues**

**Dependency Conflicts**

.. code-block:: bash

    # Check for dependency conflicts
    pip check
    
    # Resolve conflicts by creating clean environment
    python -m venv ray-data-env
    source ray-data-env/bin/activate  # On Windows: ray-data-env\Scripts\activate
    pip install -U 'ray[data]'

**Memory Configuration Issues**

.. code-block:: python

    # Check Ray cluster resources
    import ray
    
    ray.init()
    print("Cluster resources:", ray.cluster_resources())
    print("Available resources:", ray.available_resources())
    
    # Adjust object store memory if needed
    ray.shutdown()
    ray.init(object_store_memory=4 * 1024**3)  # 4GB object store

**Cloud Access Issues**

.. code-block:: python

    # Test cloud storage access
    import ray
    
    try:
        # Test S3 access
        test_data = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")
        print("S3 access successful")
        print(f"Loaded {test_data.count()} rows")
    except Exception as e:
        print(f"S3 access failed: {e}")
        print("Check AWS credentials and permissions")

Best Practices for Setup
------------------------

**1. Environment Isolation**

* Use virtual environments for dependency management
* Pin specific versions for production deployments
* Test installations in staging environments before production

**2. Resource Planning**

* Allocate appropriate object store memory (typically 30% of total RAM)
* Configure block sizes based on data characteristics and available memory
* Plan CPU and GPU allocation based on workload requirements

**3. Monitoring Setup**

* Enable comprehensive logging and metrics collection
* Set up alerting for resource exhaustion and performance issues
* Configure dashboard access for operational visibility

**4. Security Configuration**

* Use secure credential management for cloud access
* Configure network security for cluster communication
* Enable audit logging for compliance requirements

**5. Performance Optimization**

* Configure block sizes based on workload characteristics
* Enable appropriate optimizations (eager_free, push_based_shuffle)
* Monitor and tune configuration based on actual workload performance

Next Steps
----------

After installation and setup:

* **Quickstart Guide**: Learn basic Ray Data operations → :ref:`data_quickstart`
* **Key Concepts**: Understand Ray Data architecture → :ref:`data_key_concepts`
* **Loading Data**: Learn comprehensive data loading patterns → :ref:`loading_data`
* **Performance Optimization**: Optimize for your workloads → :ref:`performance-optimization`