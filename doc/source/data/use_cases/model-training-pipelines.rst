.. _model-training-pipelines:

Model Training Data Pipelines: ML Data Preparation at Scale
===========================================================

**Keywords:** machine learning training data, model training pipeline, data preprocessing, feature engineering, training data preparation, ML pipeline, PyTorch data loading, distributed training, GPU training data

**Navigation:** :ref:`Ray Data <data>` → :ref:`Use Cases <use_cases>` → Model Training Data Pipelines

This use case demonstrates how to build robust data pipelines that prepare training data for machine learning models, including feature engineering, data augmentation, and distributed training data delivery.

**What you'll build:**

* Scalable training data preparation pipeline
* Feature engineering and data augmentation workflows
* Distributed data loading for model training
* Training data quality validation and monitoring

Training Data Pipeline Architecture
-----------------------------------

**Ray Data's Training Data Advantages**

:::list-table
   :header-rows: 1

- - **Training Phase**
  - **Traditional Approach**
  - **Ray Data Approach**
  - **Key Benefits**
- - Data Loading
  - Framework-specific loaders
  - Universal data loading API
  - Framework flexibility, better performance
- - Preprocessing
  - Single-node processing
  - Distributed preprocessing
  - Scale with data size, GPU utilization
- - Feature Engineering
  - Manual feature creation
  - Automated distributed features
  - Complex features at scale
- - Data Augmentation
  - Limited by single-node memory
  - Distributed augmentation
  - Larger augmentation datasets
- - Training Delivery
  - Framework-coupled delivery
  - Streaming data delivery
  - Continuous training data flow

:::

Use Case 1: Deep Learning Training Data Pipeline
-------------------------------------------------

**Business Scenario:** Prepare large-scale training data for deep learning models, including image preprocessing, data augmentation, and distributed training data delivery.

.. code-block:: python

    import ray
    import torch
    import torchvision.transforms as transforms
    from PIL import Image
    import numpy as np

    def deep_learning_training_pipeline():
        """Prepare training data for deep learning models."""
        
        # Load raw training images
        training_images = ray.data.read_images("s3://training-data/raw-images/")
        labels_data = ray.data.read_csv("s3://training-data/labels.csv")
        
        def preprocess_training_images(batch):
            """Preprocess images for deep learning training."""
            processed = []
            
            # Define augmentation transforms
            train_transforms = transforms.Compose([
                transforms.RandomResizedCrop(224),
                transforms.RandomHorizontalFlip(p=0.5),
                transforms.RandomRotation(degrees=15),
                transforms.ColorJitter(brightness=0.2, contrast=0.2, saturation=0.2, hue=0.1),
                transforms.ToTensor(),
                transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])
            ])
            
            for item in batch.to_pylist():
                image = Image.fromarray(item["image"])
                path = item["path"]
                
                # Extract image ID from path
                image_id = path.split("/")[-1].split(".")[0]
                
                # Apply data augmentation (create multiple variants)
                augmented_images = []
                for i in range(3):  # Create 3 augmented versions
                    augmented_tensor = train_transforms(image)
                    augmented_images.append({
                        "image_id": f"{image_id}_aug_{i}",
                        "original_image_id": image_id,
                        "processed_image": augmented_tensor.numpy(),
                        "augmentation_id": i,
                        "file_path": path
                    })
                
                processed.extend(augmented_images)
            
            return ray.data.from_pylist(processed)
        
        def add_training_labels(batch):
            """Add labels and training metadata."""
            # Simulate label lookup (would join with actual labels)
            labeled_data = []
            
            for item in batch.to_pylist():
                original_image_id = item["original_image_id"]
                
                # Simulate label assignment (replace with actual label lookup)
                class_labels = ["cat", "dog", "bird", "fish", "rabbit"]
                label_id = hash(original_image_id) % len(class_labels)
                class_name = class_labels[label_id]
                
                # Create one-hot encoding
                one_hot_label = [0] * len(class_labels)
                one_hot_label[label_id] = 1
                
                labeled_data.append({
                    **item,
                    "class_id": label_id,
                    "class_name": class_name,
                    "one_hot_label": one_hot_label,
                    "is_training_ready": True
                })
            
            return ray.data.from_pylist(labeled_data)
        
        def validate_training_data(batch):
            """Validate training data quality."""
            # Check for data quality issues
            valid_data = []
            
            for item in batch.to_pylist():
                image_tensor = item["processed_image"]
                
                # Quality checks
                has_valid_shape = len(image_tensor.shape) == 3 and image_tensor.shape[0] == 3
                has_valid_range = np.all(image_tensor >= -3) and np.all(image_tensor <= 3)  # Normalized range
                has_valid_label = item["class_id"] >= 0 and item["class_id"] < 5
                
                is_valid = has_valid_shape and has_valid_range and has_valid_label
                
                if is_valid:
                    valid_data.append({
                        **item,
                        "data_quality_score": 1.0,
                        "validation_passed": True
                    })
                else:
                    # Log quality issues
                    quality_issues = []
                    if not has_valid_shape:
                        quality_issues.append("invalid_shape")
                    if not has_valid_range:
                        quality_issues.append("invalid_pixel_range")
                    if not has_valid_label:
                        quality_issues.append("invalid_label")
                    
                    valid_data.append({
                        **item,
                        "data_quality_score": 0.0,
                        "validation_passed": False,
                        "quality_issues": quality_issues
                    })
            
            return ray.data.from_pylist(valid_data)
        
        # Preprocess images with GPU acceleration
        preprocessed_images = training_images.map_batches(
            preprocess_training_images,
            compute=ray.data.ActorPoolStrategy(size=4),
            num_gpus=1,  # GPU for image preprocessing
            batch_size=32
        )
        
        # Add labels to training data
        labeled_data = preprocessed_images.map_batches(add_training_labels)
        
        # Validate training data quality
        validated_data = labeled_data.map_batches(validate_training_data)
        
        # Filter for high-quality training data
        training_ready = validated_data.filter(
            lambda row: row["validation_passed"] and row["data_quality_score"] > 0.9
        )
        
        # Create train/validation split
        train_data = training_ready.random_sample(0.8, seed=42)
        val_data = training_ready.filter(
            lambda row: hash(row["original_image_id"]) % 5 == 0  # 20% for validation
        )
        
        # Save training datasets
        train_data.write_parquet("s3://training-datasets/train/")
        val_data.write_parquet("s3://training-datasets/validation/")
        
        # Create training summary
        training_summary = training_ready.groupby("class_name").aggregate(
            ray.data.aggregate.Count("image_id"),
            ray.data.aggregate.Mean("data_quality_score")
        )
        
        training_summary.write_csv("s3://reports/training-data-summary.csv")
        
        return train_data, val_data, training_summary

Use Case 3: Feature Store Pipeline
-----------------------------------

**Business Scenario:** Build a feature store that creates, validates, and serves features for multiple machine learning models with consistent data lineage.

.. code-block:: python

    import ray
    import pandas as pd
    from ray.data.aggregate import Sum, Count, Mean, Min, Max, Std

    def feature_store_pipeline():
        """Build comprehensive feature store for ML models."""
        
        # Load raw data sources
        transactions = ray.data.read_parquet("s3://raw-data/transactions/")
        customers = ray.data.read_parquet("s3://raw-data/customers/")
        products = ray.data.read_parquet("s3://raw-data/products/")
        
        def create_customer_behavioral_features(batch):
            """Create behavioral features for customer analysis."""
            features = []
            
            for customer_group in batch.groupby("customer_id"):
                customer_id, group_data = customer_group
                
                # Transaction pattern features
                transaction_count = len(group_data)
                total_spent = group_data["amount"].sum()
                avg_transaction = group_data["amount"].mean()
                transaction_std = group_data["amount"].std()
                
                # Temporal features
                group_data["transaction_date"] = pd.to_datetime(group_data["timestamp"])
                days_active = (group_data["transaction_date"].max() - 
                             group_data["transaction_date"].min()).days
                
                # Purchase frequency
                purchase_frequency = transaction_count / max(days_active, 1)
                
                # Category diversity
                unique_categories = group_data["product_category"].nunique()
                category_diversity = unique_categories / max(transaction_count, 1)
                
                # Spending trends (simplified)
                recent_transactions = group_data.tail(5)
                recent_avg = recent_transactions["amount"].mean() if len(recent_transactions) > 0 else 0
                spending_trend = "increasing" if recent_avg > avg_transaction else "decreasing"
                
                features.append({
                    "customer_id": customer_id,
                    "transaction_count": transaction_count,
                    "total_spent": total_spent,
                    "avg_transaction_value": avg_transaction,
                    "transaction_std": transaction_std if not pd.isna(transaction_std) else 0,
                    "days_active": days_active,
                    "purchase_frequency": purchase_frequency,
                    "category_diversity": category_diversity,
                    "unique_categories": unique_categories,
                    "spending_trend": spending_trend,
                    "recent_avg_transaction": recent_avg,
                    "feature_creation_date": pd.Timestamp.now(),
                    "feature_version": "v1.0"
                })
            
            return ray.data.from_pylist(features)
        
        def create_product_performance_features(batch):
            """Create product performance features."""
            product_features = []
            
            for product_group in batch.groupby("product_id"):
                product_id, group_data = product_group
                
                # Sales performance metrics
                total_sales = len(group_data)
                total_revenue = group_data["amount"].sum()
                avg_sale_price = group_data["amount"].mean()
                price_variance = group_data["amount"].var()
                
                # Customer metrics
                unique_customers = group_data["customer_id"].nunique()
                repeat_purchase_rate = (total_sales - unique_customers) / max(unique_customers, 1)
                
                # Temporal patterns
                group_data["sale_date"] = pd.to_datetime(group_data["timestamp"])
                sales_period = (group_data["sale_date"].max() - group_data["sale_date"].min()).days
                sales_velocity = total_sales / max(sales_period, 1)
                
                product_features.append({
                    "product_id": product_id,
                    "total_sales": total_sales,
                    "total_revenue": total_revenue,
                    "avg_sale_price": avg_sale_price,
                    "price_variance": price_variance if not pd.isna(price_variance) else 0,
                    "unique_customers": unique_customers,
                    "repeat_purchase_rate": repeat_purchase_rate,
                    "sales_velocity": sales_velocity,
                    "performance_tier": "high" if sales_velocity > 1.0 else "medium" if sales_velocity > 0.1 else "low"
                })
            
            return ray.data.from_pylist(product_features)
        
        # Create behavioral features with distributed processing
        customer_features = transactions.map_batches(
            create_customer_behavioral_features,
            compute=ray.data.ActorPoolStrategy(size=6)
        )
        
        # Create product features
        product_features = transactions.map_batches(
            create_product_performance_features,
            compute=ray.data.ActorPoolStrategy(size=4)
        )
        
        # Join with demographic data
        enriched_customer_features = customer_features.join(
            customers.select_columns(["customer_id", "age", "gender", "location", "signup_date"]),
            on="customer_id",
            how="inner"
        )
        
        # Join product features with product metadata
        enriched_product_features = product_features.join(
            products.select_columns(["product_id", "category", "brand", "price", "launch_date"]),
            on="product_id",
            how="inner"
        )
        
        # Create feature quality scores
        def validate_feature_quality(batch):
            """Validate and score feature quality."""
            # Check for missing values
            missing_ratio = batch.isnull().sum(axis=1) / len(batch.columns)
            
            # Check for outliers (simplified)
            numeric_columns = batch.select_dtypes(include=[np.number]).columns
            outlier_scores = []
            
            for col in numeric_columns:
                if col in batch.columns:
                    q1 = batch[col].quantile(0.25)
                    q3 = batch[col].quantile(0.75)
                    iqr = q3 - q1
                    outlier_mask = (batch[col] < (q1 - 1.5 * iqr)) | (batch[col] > (q3 + 1.5 * iqr))
                    outlier_scores.append(outlier_mask.sum() / len(batch))
            
            avg_outlier_ratio = np.mean(outlier_scores) if outlier_scores else 0
            
            # Calculate overall feature quality
            batch["missing_value_ratio"] = missing_ratio
            batch["outlier_ratio"] = avg_outlier_ratio
            batch["feature_quality_score"] = 1.0 - (missing_ratio + avg_outlier_ratio) / 2
            batch["is_training_ready"] = (
                (missing_ratio < 0.1) & 
                (avg_outlier_ratio < 0.05) &
                (batch["feature_quality_score"] > 0.8)
            )
            
            return batch
        
        # Validate feature quality
        validated_customer_features = enriched_customer_features.map_batches(
            validate_feature_quality
        )
        
        validated_product_features = enriched_product_features.map_batches(
            validate_feature_quality
        )
        
        # Filter for training-ready features
        training_ready_customers = validated_customer_features.filter(
            lambda row: row["is_training_ready"]
        )
        
        training_ready_products = validated_product_features.filter(
            lambda row: row["is_training_ready"]
        )
        
        # Save to feature store with versioning
        training_ready_customers.write_parquet("s3://feature-store/customer-features/v1/")
        training_ready_products.write_parquet("s3://feature-store/product-features/v1/")
        
        # Create feature store metadata
        feature_metadata = {
            "customer_features": {
                "version": "v1.0",
                "feature_count": len(training_ready_customers.schema().names),
                "record_count": training_ready_customers.count(),
                "quality_threshold": 0.8,
                "creation_date": pd.Timestamp.now()
            },
            "product_features": {
                "version": "v1.0", 
                "feature_count": len(training_ready_products.schema().names),
                "record_count": training_ready_products.count(),
                "quality_threshold": 0.8,
                "creation_date": pd.Timestamp.now()
            }
        }
        
        # Save metadata
        metadata_df = pd.DataFrame(feature_metadata).T
        ray.data.from_pandas(metadata_df).write_csv("s3://feature-store/metadata/")
        
        return training_ready_customers, training_ready_products

Use Case 2: Distributed Training Data Delivery
-----------------------------------------------

**Business Scenario:** Efficiently deliver training data to distributed training workers with optimal performance and resource utilization.

.. code-block:: python

    import ray
    from ray import train

    def distributed_training_data_delivery():
        """Deliver training data efficiently to distributed training workers."""
        
        # Load prepared training data
        training_dataset = ray.data.read_parquet("s3://feature-store/training-ready/")
        
        def create_training_batches(batch):
            """Create optimized training batches."""
            # Prepare features and labels
            feature_columns = [col for col in batch.columns if col.startswith("feature_")]
            label_column = "target"
            
            # Create feature matrix
            X = batch[feature_columns].values
            y = batch[label_column].values
            
            # Add batch metadata for training
            batch_info = {
                "features": X.tolist(),
                "labels": y.tolist(),
                "batch_size": len(batch),
                "feature_count": len(feature_columns),
                "batch_id": hash(str(batch.iloc[0]["customer_id"])) % 10000
            }
            
            return ray.data.from_items([batch_info])
        
        def prepare_for_streaming_training(dataset):
            """Prepare dataset for streaming to training workers."""
            
            # Create training-optimized batches
            training_batches = dataset.map_batches(
                create_training_batches,
                batch_size=1000,  # Large batches for training efficiency
                compute=ray.data.ActorPoolStrategy(size=4)
            )
            
            # Shuffle for training
            shuffled_batches = training_batches.random_shuffle(seed=42)
            
            return shuffled_batches
        
        # Prepare streaming training data
        streaming_data = prepare_for_streaming_training(training_dataset)
        
        # Example: Integrate with Ray Train for distributed training
        def train_model_with_ray_data():
            """Example integration with Ray Train."""
            
            def train_func():
                # Get training data iterator
                train_dataset = train.get_dataset_shard("train")
                
                # Iterate through training data
                for batch in train_dataset.iter_torch_batches(batch_size=64):
                    # Training logic would go here
                    features = batch["features"]
                    labels = batch["labels"]
                    
                    # Simulate training step
                    print(f"Training batch with {len(features)} samples")
            
            # Configure distributed training
            trainer = train.Trainer(
                backend="torch",
                num_workers=4,
                use_gpu=True,
                datasets={"train": streaming_data}
            )
            
            return trainer.fit(train_func)
        
        # Save optimized training data
        streaming_data.write_parquet("s3://training-datasets/streaming-ready/")
        
        return streaming_data

**Training Data Pipeline Checklist**

**Data Preparation:**
- [ ] **Data quality validation**: Implement comprehensive quality checks
- [ ] **Feature engineering**: Create relevant features for model training
- [ ] **Data augmentation**: Apply appropriate augmentation techniques
- [ ] **Label validation**: Ensure label quality and consistency
- [ ] **Train/validation split**: Create proper data splits with no leakage

**Performance Optimization:**
- [ ] **Batch sizing**: Optimize batch sizes for training framework
- [ ] **GPU utilization**: Use GPU for preprocessing when beneficial
- [ ] **Memory management**: Monitor memory usage with large datasets
- [ ] **Streaming delivery**: Use streaming for continuous data flow
- [ ] **Caching strategy**: Cache frequently accessed data appropriately

**Integration with Training:**
- [ ] **Framework compatibility**: Ensure compatibility with PyTorch/TensorFlow
- [ ] **Data format**: Use appropriate tensor formats for training
- [ ] **Distributed training**: Support multi-GPU and multi-node training
- [ ] **Reproducibility**: Ensure consistent data ordering and randomization
- [ ] **Monitoring**: Track data delivery performance and bottlenecks

Next Steps
----------

Enhance your model training workflows:

* **Advanced Feature Engineering**: Complex feature creation → :ref:`Feature Engineering <feature-engineering>`
* **GPU-Accelerated Pipelines**: Optimize for GPU usage → :ref:`AI-Powered Pipelines <ai-powered-pipelines>`
* **Computer Vision Training**: Image-specific training data → :ref:`Computer Vision Pipelines <computer-vision-pipelines>`
* **Production ML Pipelines**: Deploy training pipelines → :ref:`Best Practices <best_practices>`
