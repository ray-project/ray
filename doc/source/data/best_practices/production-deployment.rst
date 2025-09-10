.. _production-deployment:

Production Deployment
=====================

Production deployment involves deploying Ray Data systems that meet enterprise requirements for reliability, performance, and operational excellence. This guide covers deployment strategies, configuration, and operational best practices.

What is production deployment?
------------------------------

Production deployment encompasses:

* **Infrastructure setup**: Configuring clusters, networking, and storage
* **Security configuration**: Authentication, authorization, and data protection
* **Monitoring implementation**: Observability, alerting, and logging systems
* **Operational procedures**: Deployment, scaling, backup, and recovery processes
* **Performance optimization**: Tuning for production workload requirements

Why focus on production deployment?
-----------------------------------

* **Reliability**: Ensure systems meet uptime and availability requirements
* **Performance**: Optimize for production workload characteristics
* **Security**: Protect data and systems according to enterprise standards
* **Scalability**: Handle growth in data volume and user demand
* **Maintainability**: Enable efficient operations and troubleshooting

How to deploy Ray Data in production
------------------------------------

Follow this systematic approach:

1. **Plan infrastructure**: Size clusters and configure networking
2. **Configure security**: Implement authentication and data protection
3. **Set up monitoring**: Deploy observability and alerting systems
4. **Deploy applications**: Install and configure Ray Data workloads
5. **Validate deployment**: Test functionality and performance
6. **Establish operations**: Implement ongoing operational procedures

Infrastructure Planning
-----------------------

**Cluster Sizing**

Plan cluster resources based on your workload characteristics and performance requirements:

**CPU-Intensive Workloads (ETL, Data Cleaning)**
- **Instance types**: CPU-optimized instances (c5.xlarge, c5.2xlarge)
- **Memory ratio**: 2-4 GB memory per CPU core
- **Scaling approach**: Horizontal scaling with more nodes
- **Cost optimization**: Use spot instances for non-critical workloads

**Memory-Intensive Workloads (Large Analytics, Aggregations)**
- **Instance types**: Memory-optimized instances (r5.xlarge, r5.2xlarge) 
- **Memory ratio**: 8-16 GB memory per CPU core
- **Scaling approach**: Fewer nodes with more memory per node
- **Performance focus**: Larger instance types for memory efficiency

**GPU Workloads (AI/ML, Computer Vision, NLP)**
- **Instance types**: GPU-optimized instances (p3.2xlarge, g4dn.xlarge)
- **GPU memory**: Match GPU memory to model and batch size requirements
- **CPU ratio**: 4-8 CPU cores per GPU for data preprocessing
- **Cost consideration**: GPU instances are expensive - right-size carefully

**Mixed Workloads (Multimodal Processing)**
- **Instance types**: Balanced instances (m5.xlarge, m5.2xlarge)
- **Resource allocation**: Mix of CPU and GPU nodes in same cluster
- **Flexibility**: Ability to handle both CPU and GPU operations
- **Scaling strategy**: Dynamic scaling based on workload mix

**Cluster Sizing Example:**

.. code-block:: python

    # Simple cluster resource planning
    ray.init(
        num_cpus=16,    # 16 CPU cores for processing
        num_gpus=2,     # 2 GPUs for AI workloads
        object_store_memory=32*1024*1024*1024  # 32GB object store
    )

**Network and Storage Configuration**

Configure networking and storage for optimal Ray Data performance:

**Network Requirements:**
- **Bandwidth**: 10 Gbps minimum between nodes for large data transfers
- **Latency**: Low latency networking for distributed coordination
- **Security groups**: Open ports 8000-9000 for Ray cluster communication
- **Load balancing**: Distribute traffic across cluster nodes appropriately

**Storage Configuration:**
- **High-performance storage**: NVMe SSDs for local temp storage and object spilling
- **Network storage**: High-bandwidth connection to data sources (S3, data warehouses)
- **Capacity planning**: 2-3x your largest dataset size for intermediate processing
- **Backup strategy**: Regular backups of processed data and pipeline configurations
            "complex": 2.0,     # ML inference and complex analytics
            "gpu_intensive": 3.0  # Image/video processing, training
        }
        
        multiplier = complexity_multiplier.get(processing_complexity, 1.5)
        
        return {
            "memory_gb": int(base_memory_gb * multiplier),
            "cpus": int(base_cpus * multiplier),
            "nodes": max(2, int((base_cpus * multiplier) // 16)),  # 16 CPUs per node
            "storage_gb": daily_data_gb * 7  # 7 days of data retention
        }

    # Example calculation
    requirements = calculate_cluster_requirements(
        daily_data_gb=100,
        processing_complexity="moderate"
    )
    print(f"Cluster requirements: {requirements}")

**Network Configuration**

Configure networking for optimal performance:

.. code-block:: yaml

    # Example cluster configuration (cluster.yaml)
    cluster_name: ray-data-production

    provider:
        type: aws
        region: us-west-2
        availability_zone: us-west-2a

    auth:
        ssh_user: ubuntu
        ssh_private_key: ~/.ssh/ray-cluster.pem

    head_node:
        InstanceType: m5.4xlarge
        ImageId: ami-0abcdef1234567890
        
    worker_nodes:
        InstanceType: m5.2xlarge
        ImageId: ami-0abcdef1234567890
        MinWorkers: 2
        MaxWorkers: 10
        InitialWorkers: 4

**Storage Configuration**

Configure storage for data persistence and performance:

.. code-block:: python

    import ray

    # Configure Ray Data for production storage
    ctx = ray.data.DataContext.get_current()
    
    # Set appropriate block sizes for your data
    ctx.target_max_block_size = 128 * 1024 * 1024  # 128MB blocks
    
    # Configure object store memory
    ctx.execution_options.resource_limits.object_store_memory = 8_000_000_000  # 8GB

Security Configuration
----------------------

**Authentication Setup**

Configure secure authentication for production environments:

.. code-block:: python

    import ray

    # Initialize Ray with authentication
    ray.init(
        address="ray://head-node:10001",
        runtime_env={
            "env_vars": {
                "AWS_ACCESS_KEY_ID": "your-access-key",
                "AWS_SECRET_ACCESS_KEY": "your-secret-key"
            }
        }
    )

**Data Encryption**

Ensure data protection in transit and at rest:

.. code-block:: python

    # Configure encryption for cloud storage
    import pyarrow.fs as fs

    # S3 with encryption
    s3_fs = fs.S3FileSystem(
        access_key="your-key",
        secret_key="your-secret",
        session_token="your-token",
        region="us-west-2"
    )

    # Read with encrypted filesystem
    ds = ray.data.read_parquet(
        "s3://encrypted-bucket/data/",
        filesystem=s3_fs
    )

**Access Control**

Implement role-based access control:

.. code-block:: python

    def create_secure_connection_factory(user_role):
        """Create database connections based on user role"""
        
        connection_configs = {
            "analyst": {
                "user": "readonly_user",
                "password": "readonly_password",
                "database": "analytics_db"
            },
            "engineer": {
                "user": "readwrite_user", 
                "password": "readwrite_password",
                "database": "production_db"
            }
        }
        
        config = connection_configs.get(user_role)
        if not config:
            raise ValueError(f"Invalid user role: {user_role}")
        
        def connection_factory():
            import psycopg2
            return psycopg2.connect(**config)
        
        return connection_factory

Monitoring and Alerting
-----------------------

**Production Monitoring Setup**

Implement comprehensive monitoring for production systems:

.. code-block:: python

    import ray
    import time
    import logging

    class ProductionMonitor:
        def __init__(self):
            self.metrics = {}
            self.alerts = []
        
        def monitor_pipeline_execution(self, pipeline_name, ds):
            """Monitor pipeline execution with detailed metrics"""
            
            start_time = time.time()
            
            try:
                # Execute pipeline with monitoring
                result = ds.map_batches(
                    self.monitored_transform,
                    fn_kwargs={"pipeline_name": pipeline_name}
                )
                
                execution_time = time.time() - start_time
                record_count = result.count()
                
                # Log success metrics
                self.metrics[pipeline_name] = {
                    "status": "success",
                    "execution_time": execution_time,
                    "record_count": record_count,
                    "records_per_second": record_count / execution_time,
                    "timestamp": time.time()
                }
                
                # Check performance thresholds
                if execution_time > 3600:  # 1 hour threshold
                    self.create_alert(
                        f"Pipeline {pipeline_name} exceeded time threshold: {execution_time:.0f}s"
                    )
                
                return result
                
            except Exception as e:
                # Log failure
                self.metrics[pipeline_name] = {
                    "status": "failed",
                    "error": str(e),
                    "timestamp": time.time()
                }
                
                self.create_alert(f"Pipeline {pipeline_name} failed: {str(e)}")
                raise
        
        def monitored_transform(self, batch, pipeline_name):
            """Transform with monitoring"""
            
            batch_start = time.time()
            
            # Apply your transformation logic here
            result = your_transform_logic(batch)
            
            batch_time = time.time() - batch_start
            
            # Log batch processing metrics
            logging.info(f"Pipeline {pipeline_name}: processed {len(batch)} records in {batch_time:.2f}s")
            
            return result
        
        def create_alert(self, message):
            """Create alert for monitoring system"""
            alert = {
                "timestamp": time.time(),
                "message": message,
                "severity": "warning"
            }
            
            self.alerts.append(alert)
            
            # Send to monitoring system (implement your alerting logic)
            print(f"ALERT: {message}")
        
        def get_metrics_summary(self):
            """Get summary of pipeline metrics"""
            return {
                "total_pipelines": len(self.metrics),
                "successful_pipelines": sum(1 for m in self.metrics.values() if m["status"] == "success"),
                "failed_pipelines": sum(1 for m in self.metrics.values() if m["status"] == "failed"),
                "total_alerts": len(self.alerts)
            }

    def your_transform_logic(batch):
        """Placeholder for your transformation logic"""
        return batch

    # Use production monitor
    monitor = ProductionMonitor()
    ds = ray.data.read_parquet("s3://production-data/")
    result = monitor.monitor_pipeline_execution("customer_pipeline", ds)

**Alerting Configuration**

Set up automated alerting for production issues:

.. code-block:: python

    class AlertManager:
        def __init__(self, alert_thresholds):
            self.thresholds = alert_thresholds
            
        def check_pipeline_health(self, metrics):
            """Check pipeline health against thresholds"""
            
            alerts = []
            
            # Check execution time
            if metrics.get("execution_time", 0) > self.thresholds["max_execution_time"]:
                alerts.append({
                    "type": "performance",
                    "message": f"Pipeline execution time exceeded threshold: {metrics['execution_time']:.0f}s"
                })
            
            # Check error rate
            error_rate = metrics.get("error_rate", 0)
            if error_rate > self.thresholds["max_error_rate"]:
                alerts.append({
                    "type": "quality",
                    "message": f"Error rate exceeded threshold: {error_rate:.1%}"
                })
            
            # Check data volume
            record_count = metrics.get("record_count", 0)
            if record_count < self.thresholds["min_records"]:
                alerts.append({
                    "type": "data",
                    "message": f"Record count below threshold: {record_count}"
                })
            
            return alerts

    # Configure alerting
    alert_manager = AlertManager({
        "max_execution_time": 3600,  # 1 hour
        "max_error_rate": 0.05,      # 5%
        "min_records": 1000          # Minimum expected records
    })

Deployment Strategies
--------------------

**Blue-Green Deployment**

Implement zero-downtime deployments:

.. code-block:: python

    class BlueGreenDeployment:
        def __init__(self):
            self.active_cluster = "blue"
            self.standby_cluster = "green"
        
        def deploy_new_version(self, pipeline_code):
            """Deploy new version using blue-green strategy"""
            
            target_cluster = self.standby_cluster
            
            try:
                # Deploy to standby cluster
                self.deploy_to_cluster(target_cluster, pipeline_code)
                
                # Validate deployment
                if self.validate_deployment(target_cluster):
                    # Switch traffic to new version
                    self.switch_traffic(target_cluster)
                    
                    # Update cluster roles
                    self.active_cluster, self.standby_cluster = self.standby_cluster, self.active_cluster
                    
                    print(f"Deployment successful. Active cluster: {self.active_cluster}")
                else:
                    raise Exception("Deployment validation failed")
                    
            except Exception as e:
                print(f"Deployment failed: {e}")
                # Rollback if necessary
                self.rollback_deployment()
        
        def deploy_to_cluster(self, cluster_name, pipeline_code):
            """Deploy pipeline to specified cluster"""
            # Implement cluster deployment logic
            pass
        
        def validate_deployment(self, cluster_name):
            """Validate deployment health"""
            # Run health checks
            return True
        
        def switch_traffic(self, new_cluster):
            """Switch traffic to new cluster"""
            # Update load balancer or service discovery
            pass
        
        def rollback_deployment(self):
            """Rollback to previous version"""
            # Implement rollback logic
            pass

**Canary Deployment**

Gradually roll out changes to minimize risk:

.. code-block:: python

    def canary_deployment(new_pipeline, traffic_percentage=10):
        """Deploy new pipeline to subset of traffic"""
        
        import random
        
        def route_traffic(batch):
            """Route traffic between old and new pipelines"""
            
            # Determine routing for this batch
            if random.randint(1, 100) <= traffic_percentage:
                # Route to new pipeline
                return new_pipeline.process(batch)
            else:
                # Route to existing pipeline
                return existing_pipeline.process(batch)
        
        return route_traffic

    # Implement canary deployment
    canary_processor = canary_deployment(new_pipeline_version, traffic_percentage=5)

Backup and Recovery
-------------------

**Data Backup Strategies**

Implement comprehensive backup for production data:

.. code-block:: python

    def backup_production_data():
        """Backup critical production datasets"""
        
        from datetime import datetime
        
        backup_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Backup processed datasets
        critical_datasets = [
            "customer_analytics",
            "sales_summary", 
            "ml_training_data"
        ]
        
        for dataset_name in critical_datasets:
            # Read from production location
            ds = ray.data.read_parquet(f"s3://production/{dataset_name}/")
            
            # Write to backup location with timestamp
            backup_path = f"s3://backups/{dataset_name}/{backup_timestamp}/"
            ds.write_parquet(backup_path)
            
            print(f"Backed up {dataset_name} to {backup_path}")

**Disaster Recovery Planning**

Implement disaster recovery procedures:

.. code-block:: python

    class DisasterRecoveryManager:
        def __init__(self, primary_region, backup_region):
            self.primary_region = primary_region
            self.backup_region = backup_region
        
        def create_recovery_checkpoint(self, pipeline_state):
            """Create checkpoint for disaster recovery"""
            
            checkpoint_data = {
                "pipeline_state": pipeline_state,
                "timestamp": time.time(),
                "region": self.primary_region
            }
            
            # Save checkpoint to backup region
            checkpoint_ds = ray.data.from_items([checkpoint_data])
            checkpoint_ds.write_json(f"s3://backup-{self.backup_region}/checkpoints/")
        
        def recover_from_disaster(self):
            """Recover pipeline from backup region"""
            
            # Read latest checkpoint
            checkpoints = ray.data.read_json(f"s3://backup-{self.backup_region}/checkpoints/")
            latest_checkpoint = checkpoints.sort("timestamp", descending=True).take(1)[0]
            
            # Restore pipeline state
            return latest_checkpoint["pipeline_state"]

Operational Procedures
---------------------

**Health Check Implementation**

Implement automated health checks:

.. code-block:: python

    def production_health_check():
        """Comprehensive health check for production Ray Data system"""
        
        health_status = {
            "cluster": "unknown",
            "storage": "unknown", 
            "processing": "unknown",
            "overall": "unknown"
        }
        
        try:
            # Check cluster health
            cluster_resources = ray.cluster_resources()
            if cluster_resources.get("CPU", 0) > 0:
                health_status["cluster"] = "healthy"
            else:
                health_status["cluster"] = "unhealthy"
            
            # Check storage connectivity
            test_ds = ray.data.range(10)
            test_ds.write_parquet("s3://health-check/test.parquet")
            health_status["storage"] = "healthy"
            
            # Check processing capability
            result = test_ds.map_batches(lambda batch: batch)
            if result.count() == 10:
                health_status["processing"] = "healthy"
            else:
                health_status["processing"] = "unhealthy"
            
            # Overall health assessment
            if all(status == "healthy" for status in health_status.values() if status != "unknown"):
                health_status["overall"] = "healthy"
            else:
                health_status["overall"] = "unhealthy"
                
        except Exception as e:
            health_status["overall"] = "unhealthy"
            health_status["error"] = str(e)
        
        return health_status

    # Run health check
    health = production_health_check()
    print(f"System health: {health}")

**Scaling Procedures**

Implement automated scaling based on workload:

.. code-block:: python

    def auto_scale_cluster(current_load, target_utilization=0.7):
        """Auto-scale cluster based on current load"""
        
        current_resources = ray.available_resources()
        cpu_utilization = 1 - (current_resources["CPU"] / ray.cluster_resources()["CPU"])
        
        if cpu_utilization > target_utilization:
            # Scale up
            desired_nodes = int(ray.nodes().__len__() * 1.5)
            print(f"Scaling up to {desired_nodes} nodes")
            # Implement scaling logic
            
        elif cpu_utilization < (target_utilization * 0.5):
            # Scale down
            desired_nodes = max(2, int(ray.nodes().__len__() * 0.8))
            print(f"Scaling down to {desired_nodes} nodes")
            # Implement scaling logic

Performance Tuning
------------------

**Production Optimization**

Optimize Ray Data for production workloads:

.. code-block:: python

    def configure_production_performance():
        """Configure Ray Data for optimal production performance"""
        
        ctx = ray.data.DataContext.get_current()
        
        # Optimize for throughput
        ctx.execution_options.preserve_order = False  # Allow reordering for performance
        ctx.execution_options.actor_locality_enabled = True  # Improve data locality
        
        # Configure resource limits
        ctx.execution_options.resource_limits.cpu = 0.9  # Use 90% of available CPUs
        ctx.execution_options.resource_limits.object_store_memory = 6_000_000_000  # 6GB
        
        # Optimize block size for your workload
        ctx.target_max_block_size = 256 * 1024 * 1024  # 256MB for large datasets
        ctx.target_min_block_size = 64 * 1024 * 1024   # 64MB minimum

    # Apply production configuration
    configure_production_performance()

**Workload-Specific Tuning**

Tune based on specific workload characteristics:

.. code-block:: python

    def tune_for_workload_type(workload_type):
        """Tune Ray Data configuration for specific workload types"""
        
        ctx = ray.data.DataContext.get_current()
        
        if workload_type == "etl":
            # ETL workloads: optimize for throughput
            ctx.target_max_block_size = 512 * 1024 * 1024  # Large blocks
            ctx.execution_options.resource_limits.cpu = 1.0
            
        elif workload_type == "ml_training":
            # ML training: optimize for GPU utilization
            ctx.target_max_block_size = 128 * 1024 * 1024  # Moderate blocks
            ctx.execution_options.locality_with_output = True
            
        elif workload_type == "analytics":
            # Analytics: optimize for memory efficiency
            ctx.target_max_block_size = 64 * 1024 * 1024   # Smaller blocks
            ctx.execution_options.preserve_order = True
        
        elif workload_type == "streaming":
            # Streaming: optimize for latency
            ctx.target_max_block_size = 32 * 1024 * 1024   # Small blocks
            ctx.execution_options.actor_locality_enabled = False

Operational Excellence
---------------------

**Deployment Checklist**

Use this checklist for production deployments:

**Pre-deployment:**

* [ ] Infrastructure capacity planned and provisioned
* [ ] Security configuration tested and validated
* [ ] Monitoring and alerting systems deployed
* [ ] Backup and recovery procedures tested
* [ ] Performance benchmarks established

**During deployment:**

* [ ] Deploy to staging environment first
* [ ] Run comprehensive integration tests
* [ ] Validate performance meets requirements
* [ ] Verify monitoring and alerting functionality
* [ ] Execute deployment runbook procedures

**Post-deployment:**

* [ ] Monitor system health and performance
* [ ] Validate data quality and accuracy
* [ ] Confirm backup procedures are working
* [ ] Document any issues and resolutions
* [ ] Update operational procedures based on learnings

**Maintenance Procedures**

Establish regular maintenance routines:

.. code-block:: python

    def daily_maintenance_tasks():
        """Daily maintenance tasks for Ray Data production systems"""
        
        # Check system health
        health = production_health_check()
        if health["overall"] != "healthy":
            print(f"Health check failed: {health}")
        
        # Clean up old temporary files
        cleanup_temp_files()
        
        # Validate backup integrity
        validate_backups()
        
        # Check resource utilization
        check_resource_utilization()
        
        # Review error logs
        review_error_logs()

    def cleanup_temp_files():
        """Clean up temporary files and objects"""
        # Implement cleanup logic
        pass

    def validate_backups():
        """Validate backup integrity"""
        # Test backup restoration
        pass

    def check_resource_utilization():
        """Monitor resource utilization trends"""
        resources = ray.cluster_resources()
        utilization = 1 - (ray.available_resources()["CPU"] / resources["CPU"])
        print(f"CPU utilization: {utilization:.1%}")

    def review_error_logs():
        """Review and analyze error logs"""
        # Parse and analyze Ray Data logs
        pass

Next Steps
----------

* Learn about :ref:`Monitoring & Observability <monitoring-observability>` for comprehensive system monitoring
* Explore :ref:`Data Quality Governance <data-quality-governance>` for data management best practices
* See :ref:`Troubleshooting <troubleshooting>` for diagnosing and resolving issues
* Review :ref:`Performance Optimization <performance-optimization>` for advanced tuning techniques
