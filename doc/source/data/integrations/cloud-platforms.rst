.. _cloud-platforms:

Cloud Platform Integration
==========================

Ray Data provides native integration with major cloud platforms, enabling you to leverage cloud-native services for optimal performance, cost efficiency, and scalability. This guide shows you how to integrate Ray Data with AWS, Google Cloud Platform, and Microsoft Azure.

**What you'll learn:**
* Integrate Ray Data with AWS services (S3, Glue, EMR, Lambda)
* Connect to Google Cloud Platform services (GCS, BigQuery, Cloud Storage)
* Use Microsoft Azure services (Blob Storage, Data Lake, Synapse)
* Optimize for cloud-native performance and cost efficiency
* Implement multi-cloud and hybrid cloud strategies

**Why cloud platform integration matters:**
* **Native performance**: Leverage cloud-optimized storage and compute services
* **Cost optimization**: Use cloud-native pricing models and resource management
* **Scalability**: Automatic scaling with cloud infrastructure
* **Security**: Inherit cloud platform security and compliance features
* **Ecosystem integration**: Connect with cloud-native data and ML services

What is cloud platform integration?
-----------------------------------

Cloud platform integration enables Ray Data to work seamlessly with cloud-native services, providing:

* **Storage optimization**: Use cloud-optimized storage formats and access patterns
* **Compute integration**: Leverage cloud compute services for processing
* **Service connectivity**: Connect with cloud-native data and ML services
* **Cost management**: Optimize costs using cloud pricing models
* **Security integration**: Inherit cloud platform security and compliance

Why integrate with cloud platforms?
-----------------------------------

**Business Benefits:**
* **Cost efficiency**: Pay only for resources used with cloud-native pricing
* **Scalability**: Automatic scaling with cloud infrastructure
* **Performance**: Cloud-optimized storage and compute services
* **Compliance**: Inherit cloud platform security and compliance features
* **Innovation**: Access to latest cloud-native data and ML services

**Technical Benefits:**
* **Native APIs**: Direct integration with cloud service APIs
* **Optimized formats**: Cloud-optimized data formats and compression
* **Resource management**: Automatic resource allocation and scaling
* **Fault tolerance**: Cloud-native redundancy and failover
* **Monitoring**: Integrated cloud monitoring and observability

How to integrate with cloud platforms
-------------------------------------

Follow this approach for successful cloud platform integration:

1. **Choose cloud platform**: Select AWS, GCP, or Azure based on requirements
2. **Configure authentication**: Set up cloud credentials and access controls
3. **Select services**: Choose appropriate storage, compute, and data services
4. **Optimize performance**: Use cloud-native optimization techniques
5. **Monitor costs**: Track resource usage and optimize for cost efficiency
6. **Implement security**: Ensure proper access controls and data protection

.. _aws-integration:

AWS Integration
---------------

**What is AWS integration?**

Amazon Web Services provides comprehensive cloud infrastructure for data processing. Ray Data integrates natively with AWS services, enabling you to build scalable, cost-effective data processing solutions.

**Key AWS Services:**
* **Amazon S3**: Scalable object storage for data lakes and data warehouses
* **AWS Glue**: Serverless data integration and ETL service
* **Amazon EMR**: Managed Hadoop and Spark clusters
* **AWS Lambda**: Serverless compute for event-driven processing
* **Amazon Athena**: Serverless query service for S3 data

**Reading from S3**

S3 is the foundation for data storage in AWS. Ray Data provides native S3 integration with optimized performance.

.. code-block:: python

    import ray

    # Read from S3 bucket
    s3_data = ray.data.read_parquet("s3://my-bucket/data/")

    # Read with specific prefix
    recent_data = ray.data.read_csv("s3://my-bucket/logs/2024/*.csv")

    # Read with authentication
    s3_data = ray.data.read_parquet(
        "s3://my-bucket/data/",
        filesystem=ray.data.datasource.S3FileSystem(
            region="us-west-2",
            aws_access_key_id="your-key",
            aws_secret_access_key="your-secret"
        )
    )

**Writing to S3**

Write processed results to S3 for storage, sharing, and further processing.

.. code-block:: python

    # Write processed data to S3
    processed_data.write_parquet("s3://my-bucket/processed/")

    # Write with partitioning
    partitioned_data.write_parquet(
        "s3://my-bucket/partitioned/",
        partition_cols=["year", "month", "day"]
    )

    # Write with compression
    compressed_data.write_parquet(
        "s3://my-bucket/compressed/",
        compression="snappy"
    )

**AWS Glue Integration**

Use AWS Glue for data cataloging and ETL workflows with Ray Data.

.. code-block:: python

    import boto3
    import ray

    def read_from_glue_catalog(database, table):
        """Read data from AWS Glue catalog"""
        
        # Get table metadata from Glue
        glue_client = boto3.client('glue')
        response = glue_client.get_table(
            DatabaseName=database,
            Name=table
        )
        
        # Extract S3 location
        s3_location = response['Table']['StorageDescriptor']['Location']
        
        # Read data with Ray Data
        data = ray.data.read_parquet(s3_location)
        return data

    # Read from Glue catalog
    customer_data = read_from_glue_catalog("analytics", "customers")

**Amazon EMR Integration**

Deploy Ray Data on Amazon EMR for managed Hadoop clusters.

.. code-block:: python

    # EMR cluster configuration
    emr_config = {
        "Applications": [{"Name": "Spark"}],
        "Instances": {
            "InstanceGroups": [
                {
                    "Name": "Primary",
                    "Market": "ON_DEMAND",
                    "InstanceRole": "PRIMARY",
                    "InstanceType": "m5.xlarge",
                    "InstanceCount": 1
                },
                {
                    "Name": "Core",
                    "Market": "ON_DEMAND", 
                    "InstanceRole": "CORE",
                    "InstanceType": "m5.xlarge",
                    "InstanceCount": 2
                }
            ]
        }
    }

    # Deploy Ray on EMR
    # (This requires EMR cluster setup and Ray installation)

**AWS Lambda Integration**

Use AWS Lambda for event-driven data processing with Ray Data.

.. code-block:: python

    import json
    import ray
    import boto3

    def lambda_handler(event, context):
        """Lambda function for data processing"""
        
        # Initialize Ray
        ray.init()
        
        # Process S3 event
        for record in event['Records']:
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            
            # Read data from S3
            data = ray.data.read_parquet(f"s3://{bucket}/{key}")
            
            # Process data
            processed = data.map_batches(process_batch)
            
            # Write results
            processed.write_parquet(f"s3://{bucket}/processed/{key}")
        
        return {"statusCode": 200, "body": "Processing complete"}

    def process_batch(batch):
        """Process data batch"""
        # Your processing logic here
        return batch

.. _gcp-integration:

Google Cloud Platform Integration
--------------------------------

**What is GCP integration?**

Google Cloud Platform provides serverless and managed services for data processing. Ray Data integrates with GCP services for scalable, cost-effective data solutions.

**Key GCP Services:**
* **Google Cloud Storage**: Scalable object storage with global edge locations
* **BigQuery**: Serverless data warehouse for analytics
* **Cloud Dataflow**: Stream and batch data processing service
* **Cloud Functions**: Serverless compute for event-driven processing
* **Vertex AI**: Managed ML platform and services

**Reading from Google Cloud Storage**

GCS provides high-performance object storage with global availability.

.. code-block:: python

    import ray

    # Read from GCS bucket
    gcs_data = ray.data.read_parquet("gs://my-bucket/data/")

    # Read with wildcards
    log_data = ray.data.read_csv("gs://my-bucket/logs/*.csv")

    # Read with authentication
    gcs_data = ray.data.read_parquet(
        "gs://my-bucket/data/",
        filesystem=ray.data.datasource.GCSFileSystem(
            project="your-project",
            token="your-token"
        )
    )

**Writing to Google Cloud Storage**

Write processed results to GCS for storage and sharing.

.. code-block:: python

    # Write to GCS
    processed_data.write_parquet("gs://my-bucket/processed/")

    # Write with partitioning
    partitioned_data.write_parquet(
        "gs://my-bucket/partitioned/",
        partition_cols=["region", "date"]
    )

**BigQuery Integration**

BigQuery provides serverless data warehouse capabilities with native Ray Data integration.

.. code-block:: python

    import ray

    # Read from BigQuery
    bq_data = ray.data.read_bigquery(
        dataset="project.dataset.table"
    )

    # Read with custom query
    query_data = ray.data.read_bigquery(
        query="""
        SELECT customer_id, SUM(amount) as total_spent
        FROM `project.sales.transactions`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        GROUP BY customer_id
        """
    )

    # Write to BigQuery
    processed_data.write_bigquery(
        dataset="project.analytics.processed_data"
    )

**Cloud Functions Integration**

Use Cloud Functions for event-driven data processing.

.. code-block:: python

    import ray
    from google.cloud import storage

    def process_data_cloud_function(event, context):
        """Cloud Function for data processing"""
        
        # Initialize Ray
        ray.init()
        
        # Get GCS event details
        bucket_name = event['bucket']
        file_name = event['name']
        
        # Read data from GCS
        data = ray.data.read_parquet(f"gs://{bucket_name}/{file_name}")
        
        # Process data
        processed = data.map_batches(transform_batch)
        
        # Write results
        processed.write_parquet(f"gs://{bucket_name}/processed/{file_name}")
        
        return "Processing complete"

    def transform_batch(batch):
        """Transform data batch"""
        # Your transformation logic here
        return batch

.. _azure-integration:

Microsoft Azure Integration
---------------------------

**What is Azure integration?**

Microsoft Azure provides enterprise-grade cloud services for data processing and analytics. Ray Data integrates with Azure services for comprehensive data solutions.

**Key Azure Services:**
* **Azure Blob Storage**: Scalable object storage for data lakes
* **Azure Data Lake Storage**: Optimized storage for big data analytics
* **Azure Synapse Analytics**: Enterprise data warehouse and analytics
* **Azure Functions**: Serverless compute for event-driven processing
* **Azure Machine Learning**: Managed ML platform and services

**Reading from Azure Blob Storage**

Azure Blob Storage provides scalable object storage with enterprise features.

.. code-block:: python

    import ray

    # Read from Azure Blob Storage
    blob_data = ray.data.read_parquet("abfs://container@account.blob.core.windows.net/data/")

    # Read with authentication
    blob_data = ray.data.read_parquet(
        "abfs://container@account.blob.core.windows.net/data/",
        filesystem=ray.data.datasource.AzureBlobFileSystem(
            account_name="your-account",
            account_key="your-key"
        )
    )

**Writing to Azure Blob Storage**

Write processed results to Azure Blob Storage.

.. code-block:: python

    # Write to Azure Blob Storage
    processed_data.write_parquet("abfs://container@account.blob.core.windows.net/processed/")

    # Write with partitioning
    partitioned_data.write_parquet(
        "abfs://container@account.blob.core.windows.net/partitioned/",
        partition_cols=["year", "month"]
    )

**Azure Data Lake Storage Integration**

Azure Data Lake Storage provides optimized storage for big data analytics.

.. code-block:: python

    import ray

    # Read from Data Lake Storage
    lake_data = ray.data.read_parquet("abfs://filesystem@account.dfs.core.windows.net/data/")

    # Read with hierarchical namespace
    hierarchical_data = ray.data.read_csv(
        "abfs://filesystem@account.dfs.core.windows.net/logs/year=2024/month=*/day=*/data.csv"
    )

**Azure Synapse Integration**

Azure Synapse provides enterprise data warehouse capabilities.

.. code-block:: python

    import ray
    import pyodbc

    def create_synapse_connection():
        """Create connection to Azure Synapse"""
        return pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=your-server.database.windows.net;"
            "DATABASE=your-database;"
            "UID=your-username;"
            "PWD=your-password"
        )

    # Read from Synapse
    synapse_data = ray.data.read_sql(
        sql="SELECT * FROM sales_data WHERE region = 'US'",
        connection_factory=create_synapse_connection
    )

    # Write to Synapse
    processed_data.write_sql(
        table="processed_sales",
        connection_factory=create_synapse_connection
    )

Multi-Cloud Integration
-----------------------

**Cross-Platform Data Processing**

Process data across multiple cloud platforms for comprehensive analytics and cost optimization.

.. code-block:: python

    import ray
    from ray.data.aggregate import Sum, Count, Mean

    def multi_cloud_analytics():
        """Analytics pipeline using multiple cloud platforms"""
        
        # Extract from AWS S3
        aws_data = ray.data.read_parquet("s3://aws-bucket/sales-data/")
        
        # Extract from GCS
        gcs_data = ray.data.read_parquet("gs://gcp-bucket/customer-data/")
        
        # Extract from Azure Blob Storage
        azure_data = ray.data.read_parquet("abfs://container@account.blob.core.windows.net/product-data/")
        
        # Combine data from multiple clouds
        combined_data = aws_data \
            .join(gcs_data, on="customer_id", how="inner") \
            .join(azure_data, on="product_id", how="inner")
        
        # Perform cross-cloud analytics
        cloud_analysis = combined_data.groupby(["region", "cloud_source"]).aggregate(
            Sum("amount"),
            Count("transaction_id"),
            Mean("price")
        )
        
        # Save results to primary cloud
        cloud_analysis.write_parquet("s3://aws-bucket/cross-cloud-analysis/")
        
        return cloud_analysis

    # Execute multi-cloud pipeline
    results = multi_cloud_analytics()

**Cloud Cost Optimization**

Optimize costs by using the most cost-effective cloud services for different workloads.

.. code-block:: python

    def optimize_cloud_costs():
        """Optimize costs across cloud platforms"""
        
        # Use AWS for compute-intensive workloads
        compute_data = ray.data.read_parquet("s3://aws-bucket/compute-data/")
        processed_compute = compute_data.map_batches(compute_intensive_transform)
        processed_compute.write_parquet("s3://aws-bucket/processed-compute/")
        
        # Use GCS for storage-intensive workloads
        storage_data = ray.data.read_parquet("gs://gcp-bucket/storage-data/")
        processed_storage = storage_data.map_batches(storage_intensive_transform)
        processed_storage.write_parquet("gs://gcp-bucket/processed-storage/")
        
        # Use Azure for enterprise workloads
        enterprise_data = ray.data.read_parquet("abfs://container@account.blob.core.windows.net/enterprise-data/")
        processed_enterprise = enterprise_data.map_batches(enterprise_transform)
        processed_enterprise.write_parquet("abfs://container@account.blob.core.windows.net/processed-enterprise/")

Performance Optimization
------------------------

**Cloud-Native Optimization**

Optimize performance using cloud-native features and services.

.. code-block:: python

    def optimize_cloud_performance():
        """Optimize performance for cloud platforms"""
        
        # Use cloud-optimized file formats
        # Parquet with Snappy compression for analytics
        analytics_data = ray.data.read_parquet("s3://bucket/analytics/")
        analytics_data.write_parquet(
            "s3://bucket/optimized-analytics/",
            compression="snappy"
        )
        
        # Use cloud-native partitioning
        # Partition by date for time-series data
        time_series_data = ray.data.read_csv("gs://bucket/time-series/")
        time_series_data.write_parquet(
            "gs://bucket/partitioned-time-series/",
            partition_cols=["year", "month", "day"]
        )
        
        # Use cloud-native authentication
        # IAM roles for AWS, service accounts for GCP
        aws_data = ray.data.read_parquet(
            "s3://bucket/data/",
            filesystem=ray.data.datasource.S3FileSystem(
                use_ssl=True,
                verify_ssl=True
            )
        )

**Resource Management**

Optimize cloud resource usage for cost efficiency.

.. code-block:: python

    def optimize_cloud_resources():
        """Optimize cloud resource usage"""
        
        # Configure Ray for cloud environments
        ray.init(
            _system_config={
                "object_spilling_config": json.dumps({
                    "type": "filesystem",
                    "params": {
                        "directory_path": "/tmp/spill"
                    }
                })
            }
        )
        
        # Use spot instances for cost optimization
        # (Configure in cloud-specific deployment)
        
        # Implement auto-scaling
        # (Configure in cloud-specific deployment)

Security and Compliance
-----------------------

**Cloud Security Integration**

Implement enterprise-grade security using cloud platform features.

.. code-block:: python

    def implement_cloud_security():
        """Implement cloud security best practices"""
        
        # Use IAM roles for AWS
        aws_data = ray.data.read_parquet(
            "s3://secure-bucket/data/",
            filesystem=ray.data.datasource.S3FileSystem(
                region="us-west-2"
                # IAM role automatically used
            )
        )
        
        # Use service accounts for GCP
        gcs_data = ray.data.read_parquet(
            "gs://secure-bucket/data/"
            # Service account automatically used
        )
        
        # Use managed identity for Azure
        azure_data = ray.data.read_parquet(
            "abfs://secure-container@account.blob.core.windows.net/data/"
            # Managed identity automatically used
        )

**Data Encryption**

Ensure data encryption at rest and in transit.

.. code-block:: python

    def implement_data_encryption():
        """Implement data encryption"""
        
        # S3 server-side encryption
        encrypted_data.write_parquet(
            "s3://encrypted-bucket/data/",
            filesystem=ray.data.datasource.S3FileSystem(
                s3_additional_kwargs={
                    "ServerSideEncryption": "AES256"
                }
            )
        )
        
        # GCS customer-managed encryption
        encrypted_data.write_parquet(
            "gs://encrypted-bucket/data/"
            # Customer-managed encryption keys configured in GCS
        )

Best Practices
--------------

**Cloud-Native Best Practices**

Follow these best practices for cloud platform integration:

* **Use managed services**: Leverage cloud-native managed services when possible
* **Implement auto-scaling**: Use cloud auto-scaling for cost optimization
* **Optimize storage**: Use appropriate storage classes and formats
* **Monitor costs**: Track resource usage and optimize for cost efficiency
* **Security first**: Implement proper authentication and authorization
* **Backup and recovery**: Use cloud-native backup and disaster recovery

**Performance Optimization**

Optimize performance for cloud environments:

* **Use cloud-optimized formats**: Parquet, ORC, and cloud-native compression
* **Implement partitioning**: Partition data for efficient querying
* **Leverage cloud features**: Use cloud-native performance features
* **Monitor performance**: Track performance metrics and optimize accordingly

**Cost Management**

Manage costs effectively in cloud environments:

* **Use spot instances**: Leverage spot instances for cost optimization
* **Implement auto-scaling**: Scale resources based on demand
* **Monitor usage**: Track resource usage and costs
* **Optimize storage**: Use appropriate storage classes and lifecycle policies

Next Steps
----------

* Learn about :ref:`Data Warehouse Integration <data-warehouses>` for comprehensive data solutions
* Explore :ref:`BI Tools Integration <bi-tools>` for visualization and analytics
* See :ref:`ETL Tools Integration <etl-tools>` for workflow orchestration
* Review :ref:`Best Practices <best-practices>` for operational excellence
* Check out :ref:`Business Intelligence <business-intelligence>` for advanced analytics workflows
