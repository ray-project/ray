.. _feature-engineering:

Feature Engineering: Advanced ML Feature Creation at Scale
==========================================================

**Keywords:** feature engineering, machine learning features, data transformation, statistical features, time series features, categorical encoding, feature store, ML preprocessing, distributed feature creation

**Navigation:** :ref:`Ray Data <data>` → :ref:`Use Cases <use_cases>` → Feature Engineering

This use case demonstrates advanced feature engineering techniques using Ray Data's distributed processing capabilities. Learn to create complex features for machine learning models including statistical transformations, time-series features, and categorical encodings at scale.

**What you'll build:**

* Advanced statistical feature transformations
* Time-series and temporal feature engineering
* Categorical feature encoding and embedding preparation
* Feature validation and quality scoring pipeline

Feature Engineering Architecture
--------------------------------

**Ray Data's Feature Engineering Advantages**

:::list-table
   :header-rows: 1

- - **Feature Type**
  - **Complexity**
  - **Ray Data Benefit**
  - **Scaling Capability**
- - Statistical Features
  - Mathematical computations
  - GPU acceleration for complex math
  - Linear scaling with data size
- - Time Series Features
  - Window operations, lags
  - Distributed window computations
  - Handle massive time series
- - Categorical Features
  - High-cardinality encoding
  - Distributed encoding strategies
  - Process millions of categories
- - Interaction Features
  - Cross-feature combinations
  - Parallel combination generation
  - Exponential feature space exploration
- - Aggregated Features
  - Group-level computations
  - Distributed groupby operations
  - Scale to billions of groups

:::

Use Case 1: Advanced Statistical Feature Engineering
-----------------------------------------------------

**Business Scenario:** Create sophisticated statistical features for fraud detection models, including outlier scores, distribution features, and anomaly indicators.

.. code-block:: python

    import ray
    import numpy as np
    import pandas as pd
    from scipy import stats

    def advanced_statistical_features():
        """Create advanced statistical features for fraud detection."""
        
        # Load transaction data
        transactions = ray.data.read_parquet("s3://transactions/raw/")
        
        def create_distribution_features(batch):
            """Create features based on statistical distributions."""
            # Sort by timestamp for time-based features
            batch = batch.sort_values("timestamp")
            
            # Amount distribution features
            amounts = batch["amount"].values
            
            # Central tendency features
            batch["amount_mean"] = np.mean(amounts)
            batch["amount_median"] = np.median(amounts)
            batch["amount_mode"] = stats.mode(amounts, keepdims=True)[0][0]
            
            # Dispersion features
            batch["amount_std"] = np.std(amounts)
            batch["amount_var"] = np.var(amounts)
            batch["amount_range"] = np.max(amounts) - np.min(amounts)
            batch["amount_iqr"] = np.percentile(amounts, 75) - np.percentile(amounts, 25)
            
            # Shape features
            batch["amount_skewness"] = stats.skew(amounts)
            batch["amount_kurtosis"] = stats.kurtosis(amounts)
            
            # Percentile features
            for p in [5, 25, 75, 95]:
                batch[f"amount_p{p}"] = np.percentile(amounts, p)
            
            # Outlier detection features
            q1, q3 = np.percentile(amounts, [25, 75])
            iqr = q3 - q1
            outlier_bounds = [q1 - 1.5 * iqr, q3 + 1.5 * iqr]
            
            batch["outlier_count"] = np.sum((amounts < outlier_bounds[0]) | 
                                           (amounts > outlier_bounds[1]))
            batch["outlier_ratio"] = batch["outlier_count"] / len(amounts)
            
            # Z-score features
            z_scores = np.abs((amounts - np.mean(amounts)) / np.std(amounts))
            batch["max_z_score"] = np.max(z_scores)
            batch["mean_z_score"] = np.mean(z_scores)
            batch["extreme_z_count"] = np.sum(z_scores > 3)
            
            return batch
        
        def create_temporal_features(batch):
            """Create time-based and temporal features."""
            # Convert timestamp to datetime
            batch["datetime"] = pd.to_datetime(batch["timestamp"])
            
            # Extract time components
            batch["hour"] = batch["datetime"].dt.hour
            batch["day_of_week"] = batch["datetime"].dt.dayofweek
            batch["month"] = batch["datetime"].dt.month
            batch["quarter"] = batch["datetime"].dt.quarter
            batch["year"] = batch["datetime"].dt.year
            
            # Business time features
            batch["is_weekend"] = batch["day_of_week"].isin([5, 6])
            batch["is_business_hours"] = batch["hour"].between(9, 17)
            batch["is_late_night"] = batch["hour"].between(22, 6)
            
            # Seasonal features
            batch["is_holiday_season"] = batch["month"].isin([11, 12])
            batch["is_summer"] = batch["month"].isin([6, 7, 8])
            
            # Time since features (requires sorting by customer and time)
            batch = batch.sort_values(["customer_id", "datetime"])
            batch["time_since_last"] = batch.groupby("customer_id")["datetime"].diff().dt.total_seconds()
            batch["time_since_last"] = batch["time_since_last"].fillna(0)
            
            # Transaction velocity features
            batch["transactions_per_hour"] = batch.groupby(["customer_id", "hour"]).cumcount() + 1
            batch["transactions_per_day"] = batch.groupby(["customer_id", batch["datetime"].dt.date]).cumcount() + 1
            
            return batch
        
        def create_behavioral_features(batch):
            """Create behavioral pattern features."""
            behavioral_features = []
            
            # Group by customer for behavioral analysis
            for customer_id, customer_data in batch.groupby("customer_id"):
                customer_data = customer_data.sort_values("datetime")
                
                # Transaction patterns
                transaction_count = len(customer_data)
                unique_merchants = customer_data["merchant_id"].nunique()
                unique_categories = customer_data["category"].nunique()
                
                # Spending patterns
                amounts = customer_data["amount"].values
                spending_velocity = np.gradient(amounts)  # Rate of spending change
                spending_acceleration = np.gradient(spending_velocity)  # Acceleration
                
                # Location patterns
                unique_locations = customer_data["location"].nunique() if "location" in customer_data.columns else 1
                location_entropy = stats.entropy(customer_data["location"].value_counts()) if "location" in customer_data.columns else 0
                
                # Time patterns
                hour_distribution = customer_data["hour"].value_counts(normalize=True)
                hour_entropy = stats.entropy(hour_distribution)
                
                # Risk indicators
                late_night_ratio = (customer_data["hour"] >= 22).sum() / transaction_count
                weekend_ratio = customer_data["is_weekend"].sum() / transaction_count
                
                behavioral_features.append({
                    "customer_id": customer_id,
                    "transaction_count": transaction_count,
                    "merchant_diversity": unique_merchants,
                    "category_diversity": unique_categories,
                    "location_diversity": unique_locations,
                    "location_entropy": location_entropy,
                    "hour_entropy": hour_entropy,
                    "late_night_ratio": late_night_ratio,
                    "weekend_ratio": weekend_ratio,
                    "avg_spending_velocity": np.mean(spending_velocity),
                    "max_spending_acceleration": np.max(np.abs(spending_acceleration)),
                    "behavioral_risk_score": (late_night_ratio + weekend_ratio + location_entropy) / 3
                })
            
            return ray.data.from_pylist(behavioral_features)
        
        # Create statistical features
        statistical_features = transactions.map_batches(
            create_distribution_features,
            compute=ray.data.ActorPoolStrategy(size=4)
        )
        
        # Create temporal features
        temporal_features = statistical_features.map_batches(
            create_temporal_features,
            compute=ray.data.ActorPoolStrategy(size=4)
        )
        
        # Create behavioral features
        behavioral_features = temporal_features.map_batches(
            create_behavioral_features,
            compute=ray.data.ActorPoolStrategy(size=6)
        )
        
        # Save comprehensive feature set
        behavioral_features.write_parquet("s3://features/comprehensive-features/")
        
        return behavioral_features

Use Case 3: High-Cardinality Categorical Encoding
--------------------------------------------------

**Business Scenario:** Encode high-cardinality categorical features for machine learning models using distributed encoding strategies.

.. code-block:: python

    import ray
    import pandas as pd
    from sklearn.preprocessing import LabelEncoder

    def high_cardinality_encoding_pipeline():
        """Handle high-cardinality categorical features at scale."""
        
        # Load data with high-cardinality categories
        user_behavior = ray.data.read_parquet("s3://user-data/behavior/")
        
        def frequency_based_encoding(batch):
            """Create frequency-based encodings for categorical features."""
            # High-cardinality categorical columns
            categorical_columns = ["user_agent", "referrer_url", "product_sku", "campaign_id"]
            
            for col in categorical_columns:
                if col in batch.columns:
                    # Frequency encoding
                    value_counts = batch[col].value_counts()
                    batch[f"{col}_frequency"] = batch[col].map(value_counts)
                    
                    # Rank encoding
                    batch[f"{col}_rank"] = batch[col].map(
                        value_counts.rank(ascending=False).to_dict()
                    )
                    
                    # Rare category indicator
                    rare_threshold = 5
                    batch[f"{col}_is_rare"] = (batch[f"{col}_frequency"] < rare_threshold).astype(int)
                    
                    # Category stability (simplified)
                    batch[f"{col}_stability_score"] = np.minimum(
                        batch[f"{col}_frequency"] / batch[f"{col}_frequency"].max(), 1.0
                    )
            
            return batch
        
        def target_based_encoding(batch):
            """Create target-based encodings (mean encoding)."""
            target_column = "conversion_rate"  # Example target
            categorical_columns = ["product_category", "user_segment", "traffic_source"]
            
            for col in categorical_columns:
                if col in batch.columns and target_column in batch.columns:
                    # Mean target encoding
                    target_means = batch.groupby(col)[target_column].mean()
                    batch[f"{col}_target_mean"] = batch[col].map(target_means)
                    
                    # Count encoding for regularization
                    category_counts = batch[col].value_counts()
                    batch[f"{col}_count"] = batch[col].map(category_counts)
                    
                    # Smoothed target encoding (Bayesian approach)
                    global_mean = batch[target_column].mean()
                    smoothing_factor = 10
                    
                    smoothed_means = (
                        target_means * batch[f"{col}_count"] + 
                        global_mean * smoothing_factor
                    ) / (batch[f"{col}_count"] + smoothing_factor)
                    
                    batch[f"{col}_target_smooth"] = batch[col].map(smoothed_means)
            
            return batch
        
        def create_embedding_features(batch):
            """Prepare categorical features for embedding layers."""
            embedding_columns = ["user_id", "product_id", "category_id"]
            
            for col in embedding_columns:
                if col in batch.columns:
                    # Create embedding indices
                    unique_values = batch[col].unique()
                    value_to_index = {val: idx for idx, val in enumerate(unique_values)}
                    
                    batch[f"{col}_embedding_index"] = batch[col].map(value_to_index)
                    batch[f"{col}_vocab_size"] = len(unique_values)
                    
                    # Embedding dimension recommendation
                    recommended_dim = min(50, int(len(unique_values) ** 0.25) * 4)
                    batch[f"{col}_recommended_embedding_dim"] = recommended_dim
            
            return batch
        
        # Apply frequency-based encoding
        frequency_encoded = user_behavior.map_batches(
            frequency_based_encoding,
            compute=ray.data.ActorPoolStrategy(size=6)
        )
        
        # Apply target-based encoding
        target_encoded = frequency_encoded.map_batches(
            target_based_encoding,
            compute=ray.data.ActorPoolStrategy(size=4)
        )
        
        # Prepare embedding features
        embedding_ready = target_encoded.map_batches(
            create_embedding_features,
            compute=ray.data.ActorPoolStrategy(size=4)
        )
        
        # Create feature quality summary
        feature_summary = embedding_ready.select_columns([
            col for col in embedding_ready.schema().names 
            if any(suffix in col for suffix in ["_frequency", "_rank", "_target_mean", "_embedding_index"])
        ])
        
        # Save engineered features
        embedding_ready.write_parquet("s3://features/categorical-encoded/")
        feature_summary.write_parquet("s3://features/summary/")
        
        return embedding_ready, feature_summary

**Feature Engineering Best Practices Checklist**

**Statistical Features:**
- [ ] **Distribution analysis**: Understand data distributions before transformation
- [ ] **Outlier handling**: Identify and handle outliers appropriately
- [ ] **Normalization**: Apply appropriate scaling for numerical features
- [ ] **Missing value strategy**: Handle missing values consistently
- [ ] **Feature correlation**: Check for highly correlated redundant features

**Categorical Features:**
- [ ] **Cardinality assessment**: Understand category counts and distributions
- [ ] **Encoding strategy**: Choose appropriate encoding for cardinality level
- [ ] **Rare category handling**: Manage infrequent categories effectively
- [ ] **Target leakage**: Avoid target information leaking into features
- [ ] **Validation encoding**: Apply same encoding to train/test/validation sets

**Temporal Features:**
- [ ] **Time zone handling**: Manage time zones consistently
- [ ] **Seasonality capture**: Include seasonal and cyclical patterns
- [ ] **Lag features**: Create appropriate historical features
- [ ] **Window operations**: Use appropriate window sizes for patterns
- [ ] **Future leakage**: Ensure no future information in features

**Ray Ecosystem Integration**

Feature engineering with Ray Data integrates seamlessly with the complete Ray platform:

* **Ray Train**: Use engineered features for distributed model training → :ref:`Ray Train <train-docs>`
* **Ray Tune**: Optimize feature engineering hyperparameters → :ref:`Ray Tune <tune-docs>`
* **Ray Serve**: Serve models trained on Ray Data features → :ref:`Ray Serve <serve-docs>`

Next Steps
----------

Apply feature engineering to specific domains:

* **Time Series Features**: Advanced temporal analysis → :ref:`Advanced Analytics <advanced-analytics>`
* **Computer Vision Features**: Visual feature extraction → :ref:`Computer Vision Pipelines <working-with-images>`
* **NLP Features**: Text-based feature creation → :ref:`NLP Data Processing <nlp-data-processing>`
* **Model Training**: Use features for training → :ref:`Model Training Pipelines <model-training-pipelines>`
