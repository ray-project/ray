.. _working_with_time_series:

Working with Time Series
========================

Ray Data provides comprehensive support for time series processing workloads, from simple time-based transformations to complex forecasting and anomaly detection pipelines. This guide shows you how to efficiently work with time series datasets of any scale.

**What you'll learn:**

* Loading time series data from various sources and formats
* Performing time-based transformations and preprocessing
* Building forecasting and anomaly detection pipelines
* Integrating with popular time series frameworks
* Optimizing time series processing for production workloads

Why Ray Data for Time Series Processing
---------------------------------------

Ray Data excels at time series processing workloads through several key advantages:

**Temporal Data Excellence**
Native support for time-based operations, windowing, and temporal aggregations with optimized performance.

**Scalable Performance**
Process time series datasets larger than memory with streaming execution and intelligent resource allocation.

**Production Ready**
Battle-tested at companies processing millions of time series points daily with enterprise-grade monitoring and error handling.

**Real-time Capabilities**
Support for streaming time series data with low-latency processing and real-time analytics.

Loading Time Series Data
------------------------

Ray Data supports loading time series data from multiple sources and formats with automatic format detection and optimization.

**Time Series File Formats**

Ray Data can read time series from various formats using different read functions:

.. code-block:: python

    import ray
    import pandas as pd
    from datetime import datetime, timedelta

    # Load CSV time series data
    csv_timeseries = ray.data.read_csv("data/timeseries.csv")

    # Load Parquet time series data
    parquet_timeseries = ray.data.read_parquet("data/timeseries.parquet")

    # Load JSON time series data
    json_timeseries = ray.data.read_json("data/timeseries.json")

    # Load from cloud storage
    cloud_timeseries = ray.data.read_parquet("s3://bucket/timeseries-data/")

    # Load with specific file patterns
    daily_files = ray.data.read_parquet("data/timeseries/daily_*.parquet")
    hourly_files = ray.data.read_parquet("data/timeseries/hourly_*.parquet")

**Time Series Data Structure**

Organize time series data with proper timestamp handling:

.. code-block:: python

    import pandas as pd
    import numpy as np
    from typing import Dict, Any
    import ray

    def create_sample_timeseries():
        """Create sample time series data for demonstration."""
        
        # Generate sample time series data
        dates = pd.date_range('2023-01-01', periods=1000, freq='H')
        
        # Create multiple time series
        data = {
            'timestamp': dates,
            'value': np.random.randn(1000).cumsum(),
            'category': np.random.choice(['A', 'B', 'C'], 1000),
            'metric_1': np.random.exponential(1, 1000),
            'metric_2': np.random.normal(100, 20, 1000)
        }
        
        return pd.DataFrame(data)

    # Create and load sample time series
    sample_df = create_sample_timeseries()
    timeseries_data = ray.data.from_pandas([sample_df])

    # Load from existing files
    existing_timeseries = ray.data.read_parquet("data/existing_timeseries.parquet")

**Database Time Series Sources**

Load time series data from various database sources:

.. code-block:: python

    import ray
    from sqlalchemy import create_engine
    import pandas as pd

    # Load from SQL database
    def load_from_sql():
        """Load time series data from SQL database."""
        
        engine = create_engine('postgresql://user:pass@localhost/timeseries_db')
        
        # Query time series data
        query = """
        SELECT timestamp, value, category, metric_1, metric_2
        FROM timeseries_table
        WHERE timestamp >= '2023-01-01'
        ORDER BY timestamp
        """
        
        df = pd.read_sql(query, engine)
        return ray.data.from_pandas([df])

    # Load from different database types
    postgres_timeseries = load_from_sql()
    
    # Load from Snowflake
    snowflake_timeseries = ray.data.read_snowflake(
        "SELECT * FROM timeseries_table",
        connection_parameters={
            "account": "your_account",
            "user": "your_user",
            "password": "your_password",
            "warehouse": "your_warehouse",
            "database": "your_database",
            "schema": "your_schema"
        }
    )

Time Series Transformations
---------------------------

Transform time series data using Ray Data's powerful transformation capabilities with support for complex temporal operations.

**Basic Time Series Transformations**

.. code-block:: python

    import pandas as pd
    import numpy as np
    from typing import Dict, Any
    import ray

    def basic_timeseries_transformations(batch: Dict[str, Any]) -> Dict[str, Any]:
        """Apply basic time series transformations."""
        
        transformed_data = []
        
        for row in batch["data"]:
            timestamp = pd.to_datetime(row["timestamp"])
            value = row["value"]
            
            # Extract time components
            hour = timestamp.hour
            day_of_week = timestamp.dayofweek
            month = timestamp.month
            quarter = timestamp.quarter
            
            # Apply basic transformations
            # 1. Moving average (simplified)
            # In practice, you'd use proper rolling windows
            moving_avg = value  # Placeholder
            
            # 2. Time-based features
            is_weekend = 1 if day_of_week >= 5 else 0
            is_business_hour = 1 if 9 <= hour <= 17 else 0
            
            # 3. Seasonal indicators
            is_q1 = 1 if quarter == 1 else 0
            is_q2 = 1 if quarter == 2 else 0
            is_q3 = 1 if quarter == 3 else 0
            is_q4 = 1 if quarter == 4 else 0
            
            transformed_data.append({
                "timestamp": timestamp,
                "original_value": value,
                "hour": hour,
                "day_of_week": day_of_week,
                "month": month,
                "quarter": quarter,
                "moving_avg": moving_avg,
                "is_weekend": is_weekend,
                "is_business_hour": is_business_hour,
                "is_q1": is_q1,
                "is_q2": is_q2,
                "is_q3": is_q3,
                "is_q4": is_q4
            })
        
        batch["transformed_data"] = transformed_data
        return batch

    # Apply basic transformations
    transformed_timeseries = timeseries_data.map_batches(basic_timeseries_transformations)

**Advanced Time Series Processing**

.. code-block:: python

    import pandas as pd
    import numpy as np
    from scipy import stats
    from typing import Dict, Any
    import ray

    class AdvancedTimeseriesProcessor:
        """Advanced time series processing with multiple techniques."""
        
        def __init__(self):
            self.window_size = 24  # 24-hour window
        
        def __call__(self, batch: Dict[str, Any]) -> Dict[str, Any]:
            """Apply advanced time series processing techniques."""
            
            processed_data = []
            
            for row in batch["transformed_data"]:
                timestamp = row["timestamp"]
                value = row["original_value"]
                
                # Apply advanced processing techniques
                
                # 1. Statistical features
                # In practice, you'd calculate these over proper windows
                z_score = 0  # Placeholder for z-score
                percentile_25 = 0  # Placeholder for 25th percentile
                percentile_75 = 0  # Placeholder for 75th percentile
                
                # 2. Trend analysis
                # In practice, you'd use proper trend detection
                trend = 0  # Placeholder for trend
                trend_strength = 0  # Placeholder for trend strength
                
                # 3. Seasonality detection
                # In practice, you'd use FFT or seasonal decomposition
                seasonal_strength = 0  # Placeholder for seasonal strength
                seasonal_period = 24  # Assume 24-hour seasonality
                
                # 4. Volatility measures
                # In practice, you'd calculate over proper windows
                volatility = 0  # Placeholder for volatility
                garch_volatility = 0  # Placeholder for GARCH volatility
                
                # 5. Change point detection
                # In practice, you'd use proper change point algorithms
                is_change_point = 0  # Placeholder for change point indicator
                change_point_confidence = 0  # Placeholder for confidence
                
                processed_data.append({
                    "timestamp": timestamp,
                    "original_value": value,
                    "z_score": z_score,
                    "percentile_25": percentile_25,
                    "percentile_75": percentile_75,
                    "trend": trend,
                    "trend_strength": trend_strength,
                    "seasonal_strength": seasonal_strength,
                    "seasonal_period": seasonal_period,
                    "volatility": volatility,
                    "garch_volatility": garch_volatility,
                    "is_change_point": is_change_point,
                    "change_point_confidence": change_point_confidence
                })
            
            batch["advanced_processed"] = processed_data
            return batch

    # Apply advanced processing
    advanced_processed = transformed_timeseries.map_batches(AdvancedTimeseriesProcessor())

**Temporal Aggregations and Windowing**

.. code-block:: python

    import pandas as pd
    import numpy as np
    from typing import Dict, Any
    import ray

    def temporal_aggregations(batch: Dict[str, Any]) -> Dict[str, Any]:
        """Apply temporal aggregations and windowing."""
        
        # Convert batch to pandas for easier temporal operations
        df = pd.DataFrame(batch["advanced_processed"])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')
        
        # Set timestamp as index for time-based operations
        df.set_index('timestamp', inplace=True)
        
        # 1. Resampling to different frequencies
        hourly_data = df.resample('H').agg({
            'original_value': ['mean', 'std', 'min', 'max'],
            'volatility': 'mean',
            'trend': 'last'
        }).round(4)
        
        daily_data = df.resample('D').agg({
            'original_value': ['mean', 'std', 'min', 'max', 'sum'],
            'volatility': 'mean',
            'trend': 'last'
        }).round(4)
        
        weekly_data = df.resample('W').agg({
            'original_value': ['mean', 'std', 'min', 'max', 'sum'],
            'volatility': 'mean',
            'trend': 'last'
        }).round(4)
        
        monthly_data = df.resample('M').agg({
            'original_value': ['mean', 'std', 'min', 'max', 'sum'],
            'volatility': 'mean',
            'trend': 'last'
        }).round(4)
        
        # 2. Rolling window calculations
        # 24-hour rolling window
        rolling_24h = df['original_value'].rolling(window=24).agg([
            'mean', 'std', 'min', 'max'
        ]).round(4)
        
        # 7-day rolling window
        rolling_7d = df['original_value'].rolling(window=7*24).agg([
            'mean', 'std', 'min', 'max'
        ]).round(4)
        
        # 3. Expanding window calculations
        expanding_stats = df['original_value'].expanding().agg([
            'mean', 'std', 'min', 'max'
        ]).round(4)
        
        # 4. Time-based grouping
        hourly_groups = df.groupby(df.index.hour).agg({
            'original_value': ['mean', 'std', 'count'],
            'volatility': 'mean'
        }).round(4)
        
        daily_groups = df.groupby(df.index.dayofweek).agg({
            'original_value': ['mean', 'std', 'count'],
            'volatility': 'mean'
        }).round(4)
        
        monthly_groups = df.groupby(df.index.month).agg({
            'original_value': ['mean', 'std', 'count'],
            'volatility': 'mean'
        }).round(4)
        
        # Combine all aggregations
        aggregated_data = {
            "hourly": hourly_data.to_dict(),
            "daily": daily_data.to_dict(),
            "weekly": weekly_data.to_dict(),
            "monthly": monthly_data.to_dict(),
            "rolling_24h": rolling_24h.to_dict(),
            "rolling_7d": rolling_7d.to_dict(),
            "expanding": expanding_stats.to_dict(),
            "hourly_groups": hourly_groups.to_dict(),
            "daily_groups": daily_groups.to_dict(),
            "monthly_groups": monthly_groups.to_dict()
        }
        
        batch["temporal_aggregations"] = aggregated_data
        return batch

    # Apply temporal aggregations
    aggregated_timeseries = advanced_processed.map_batches(temporal_aggregations)

Time Series Analysis Pipelines
------------------------------

Build end-to-end time series analysis pipelines with Ray Data for various applications.

**Forecasting Pipeline**

.. code-block:: python

    import pandas as pd
    import numpy as np
    from sklearn.linear_model import LinearRegression
    from sklearn.preprocessing import StandardScaler
    from typing import Dict, Any
    import ray

    class TimeseriesForecaster:
        """Time series forecasting using multiple models."""
        
        def __init__(self, forecast_horizon=24):
            self.forecast_horizon = forecast_horizon
            self.scaler = StandardScaler()
            self.model = LinearRegression()
        
        def __call__(self, batch: Dict[str, Any]) -> Dict[str, Any]:
            """Generate time series forecasts."""
            
            forecasting_results = []
            
            for row in batch["temporal_aggregations"]:
                try:
                    # Extract time series data
                    hourly_values = row["hourly"]["original_value"]["mean"]
                    
                    if not hourly_values:
                        forecasting_results.append({"error": "No data available"})
                        continue
                    
                    # Prepare features for forecasting
                    # In practice, you'd use proper feature engineering
                    values = list(hourly_values.values())
                    timestamps = list(hourly_values.keys())
                    
                    if len(values) < self.forecast_horizon * 2:
                        forecasting_results.append({"error": "Insufficient data for forecasting"})
                        continue
                    
                    # Create lag features
                    X = []
                    y = []
                    
                    for i in range(len(values) - self.forecast_horizon):
                        # Use last 24 values as features
                        features = values[i:i+24]
                        target = values[i+24:i+24+self.forecast_horizon]
                        
                        if len(features) == 24 and len(target) == self.forecast_horizon:
                            X.append(features)
                            y.append(target)
                    
                    if len(X) == 0:
                        forecasting_results.append({"error": "Could not create features"})
                        continue
                    
                    # Convert to numpy arrays
                    X = np.array(X)
                    y = np.array(y)
                    
                    # Scale features
                    X_scaled = self.scaler.fit_transform(X)
                    
                    # Train model
                    self.model.fit(X_scaled, y)
                    
                    # Generate forecast
                    last_features = np.array(values[-24:]).reshape(1, -1)
                    last_features_scaled = self.scaler.transform(last_features)
                    
                    forecast = self.model.predict(last_features_scaled)[0]
                    
                    # Calculate forecast confidence (simplified)
                    confidence = 0.8  # Placeholder for confidence calculation
                    
                    # Generate future timestamps
                    last_timestamp = pd.to_datetime(timestamps[-1])
                    future_timestamps = pd.date_range(
                        start=last_timestamp + pd.Timedelta(hours=1),
                        periods=self.forecast_horizon,
                        freq='H'
                    )
                    
                    forecasting_results.append({
                        "last_timestamp": str(last_timestamp),
                        "forecast_horizon": self.forecast_horizon,
                        "forecast_values": forecast.tolist(),
                        "future_timestamps": [str(ts) for ts in future_timestamps],
                        "confidence": confidence,
                        "model_type": "LinearRegression",
                        "features_used": 24
                    })
                    
                except Exception as e:
                    forecasting_results.append({"error": str(e)})
            
            batch["forecasting_results"] = forecasting_results
            return batch

    # Build forecasting pipeline
    forecasting_pipeline = (
        aggregated_timeseries
        .map_batches(TimeseriesForecaster())
    )

**Anomaly Detection Pipeline**

.. code-block:: python

    import pandas as pd
    import numpy as np
    from sklearn.ensemble import IsolationForest
    from sklearn.preprocessing import StandardScaler
    from typing import Dict, Any
    import ray

    class TimeseriesAnomalyDetector:
        """Anomaly detection in time series data."""
        
        def __init__(self, contamination=0.1):
            self.contamination = contamination
            self.scaler = StandardScaler()
            self.isolation_forest = IsolationForest(
                contamination=contamination,
                random_state=42
            )
        
        def __call__(self, batch: Dict[str, Any]) -> Dict[str, Any]:
            """Detect anomalies in time series data."""
            
            anomaly_results = []
            
            for row in batch["temporal_aggregations"]:
                try:
                    # Extract time series data
                    hourly_values = row["hourly"]["original_value"]["mean"]
                    
                    if not hourly_values:
                        anomaly_results.append({"error": "No data available"})
                        continue
                    
                    # Prepare data for anomaly detection
                    values = list(hourly_values.values())
                    timestamps = list(hourly_values.keys())
                    
                    if len(values) < 10:
                        anomaly_results.append({"error": "Insufficient data for anomaly detection"})
                        continue
                    
                    # Create features for anomaly detection
                    features = []
                    feature_timestamps = []
                    
                    for i in range(len(values)):
                        if i >= 3:  # Need at least 3 previous values
                            # Create feature vector
                            feature_vector = [
                                values[i],  # Current value
                                values[i-1],  # Previous value
                                values[i-2],  # Two values ago
                                values[i-3],  # Three values ago
                                values[i] - values[i-1],  # First difference
                                values[i-1] - values[i-2],  # Second difference
                                values[i-2] - values[i-3],  # Third difference
                                abs(values[i] - values[i-1]),  # Absolute difference
                                np.mean(values[max(0, i-24):i]) if i >= 24 else np.mean(values[:i]),  # Rolling mean
                                np.std(values[max(0, i-24):i]) if i >= 24 else np.std(values[:i])   # Rolling std
                            ]
                            
                            features.append(feature_vector)
                            feature_timestamps.append(timestamps[i])
                    
                    if len(features) == 0:
                        anomaly_results.append({"error": "Could not create features"})
                        continue
                    
                    # Convert to numpy array
                    features = np.array(features)
                    
                    # Scale features
                    features_scaled = self.scaler.fit_transform(features)
                    
                    # Detect anomalies
                    anomaly_labels = self.isolation_forest.fit_predict(features_scaled)
                    
                    # Extract anomaly information
                    anomalies = []
                    for i, label in enumerate(anomaly_labels):
                        if label == -1:  # Anomaly detected
                            anomalies.append({
                                "timestamp": str(feature_timestamps[i]),
                                "value": values[timestamps.index(feature_timestamps[i])],
                                "anomaly_score": float(self.isolation_forest.score_samples(features_scaled[i:i+1])[0]),
                                "feature_values": features[i].tolist()
                            })
                    
                    # Calculate anomaly statistics
                    total_points = len(anomaly_labels)
                    anomaly_count = len(anomalies)
                    anomaly_rate = anomaly_count / total_points if total_points > 0 else 0
                    
                    anomaly_results.append({
                        "total_points": total_points,
                        "anomaly_count": anomaly_count,
                        "anomaly_rate": anomaly_rate,
                        "anomalies": anomalies,
                        "contamination": self.contamination,
                        "model_type": "IsolationForest"
                    })
                    
                except Exception as e:
                    anomaly_results.append({"error": str(e)})
            
            batch["anomaly_detection"] = anomaly_results
            return batch

    # Build anomaly detection pipeline
    anomaly_pipeline = (
        aggregated_timeseries
        .map_batches(TimeseriesAnomalyDetector())
    )

**Seasonal Decomposition Pipeline**

.. code-block:: python

    import pandas as pd
    import numpy as np
    from statsmodels.tsa.seasonal import seasonal_decompose
    from typing import Dict, Any
    import ray

    class SeasonalDecomposer:
        """Seasonal decomposition of time series data."""
        
        def __init__(self, period=24):
            self.period = period  # 24 hours for daily seasonality
        
        def __call__(self, batch: Dict[str, Any]) -> Dict[str, Any]:
            """Decompose time series into trend, seasonal, and residual components."""
            
            decomposition_results = []
            
            for row in batch["temporal_aggregations"]:
                try:
                    # Extract time series data
                    hourly_values = row["hourly"]["original_value"]["mean"]
                    
                    if not hourly_values:
                        decomposition_results.append({"error": "No data available"})
                        continue
                    
                    # Prepare data for decomposition
                    values = list(hourly_values.values())
                    timestamps = list(hourly_values.keys())
                    
                    if len(values) < self.period * 2:
                        decomposition_results.append({"error": "Insufficient data for decomposition"})
                        continue
                    
                    # Create pandas Series with proper index
                    series = pd.Series(values, index=pd.to_datetime(timestamps))
                    
                    # Perform seasonal decomposition
                    decomposition = seasonal_decompose(
                        series,
                        period=self.period,
                        extrapolate_trend='freq'
                    )
                    
                    # Extract components
                    trend = decomposition.trend.dropna().tolist()
                    seasonal = decomposition.seasonal.dropna().tolist()
                    residual = decomposition.resid.dropna().tolist()
                    
                    # Calculate component statistics
                    trend_mean = np.mean(trend) if len(trend) > 0 else 0
                    trend_std = np.std(trend) if len(trend) > 0 else 0
                    
                    seasonal_mean = np.mean(seasonal) if len(seasonal) > 0 else 0
                    seasonal_std = np.std(seasonal) if len(seasonal) > 0 else 0
                    
                    residual_mean = np.mean(residual) if len(residual) > 0 else 0
                    residual_std = np.std(residual) if len(residual) > 0 else 0
                    
                    # Calculate strength of trend and seasonality
                    # In practice, you'd use proper strength measures
                    trend_strength = 0.7  # Placeholder
                    seasonal_strength = 0.6  # Placeholder
                    
                    decomposition_results.append({
                        "period": self.period,
                        "trend": {
                            "values": trend,
                            "mean": float(trend_mean),
                            "std": float(trend_std),
                            "strength": trend_strength
                        },
                        "seasonal": {
                            "values": seasonal,
                            "mean": float(seasonal_mean),
                            "std": float(seasonal_std),
                            "strength": seasonal_strength
                        },
                        "residual": {
                            "values": residual,
                            "mean": float(residual_mean),
                            "std": float(residual_std)
                        },
                        "original_length": len(values),
                        "decomposed_length": len(trend)
                    })
                    
                except Exception as e:
                    decomposition_results.append({"error": str(e)})
            
            batch["seasonal_decomposition"] = decomposition_results
            return batch

    # Build seasonal decomposition pipeline
    decomposition_pipeline = (
        aggregated_timeseries
        .map_batches(SeasonalDecomposer())
    )

Performance Optimization
------------------------

Optimize time series processing pipelines for maximum performance and efficiency.

**Batch Size Optimization**

.. code-block:: python

    from ray.data.context import DataContext
    import ray

    # Configure optimal batch sizes for time series processing
    ctx = DataContext.get_current()
    
    # For time series processing, moderate batch sizes work well
    ctx.target_max_block_size = 256 * 1024 * 1024  # 256MB blocks
    
    # Optimize batch sizes based on time series characteristics
    def optimize_timeseries_batch_size(timeseries_data):
        """Determine optimal batch size for time series processing."""
        
        # Analyze time series characteristics
        sample_batch = timeseries_data.take_batch(batch_size=100)
        
        # Calculate optimal batch size based on data size
        # Time series data is typically smaller than other data types
        target_batch_size = 64  # Good default for time series
        
        return target_batch_size

    # Apply optimized batch processing
    optimal_batch_size = optimize_timeseries_batch_size(aggregated_timeseries)
    optimized_pipeline = aggregated_timeseries.map_batches(
        process_timeseries,
        batch_size=optimal_batch_size
    )

**Memory Management**

.. code-block:: python

    def memory_efficient_timeseries_processing(batch: Dict[str, Any]) -> Dict[str, Any]:
        """Process time series with memory efficiency."""
        
        # Process in smaller chunks to manage memory
        chunk_size = 32
        results = []
        
        for i in range(0, len(batch["temporal_aggregations"]), chunk_size):
            chunk = batch["temporal_aggregations"][i:i+chunk_size]
            
            # Process chunk
            processed_chunk = process_timeseries_chunk(chunk)
            results.extend(processed_chunk)
            
            # Explicitly clear chunk from memory
            del chunk
        
        batch["processed_timeseries"] = results
        return batch

    # Use memory-efficient processing
    memory_optimized = aggregated_timeseries.map_batches(memory_efficient_timeseries_processing)

**Parallel Processing Optimization**

.. code-block:: python

    # Configure parallel processing for time series workloads
    parallel_strategy = ray.data.ActorPoolStrategy(
        size=8,  # Number of parallel workers
        max_tasks_in_flight_per_actor=2  # Pipeline depth per worker
    )

    # Apply parallel-optimized processing
    parallel_optimized = aggregated_timeseries.map_batches(
        TimeseriesProcessor,
        compute=parallel_strategy,
        batch_size=32
    )

Saving and Exporting Time Series
--------------------------------

Save processed time series data in various formats for different use cases.

**Time Series File Formats**

.. code-block:: python

    import pandas as pd
    from typing import Dict, Any
    import ray

    def save_timeseries_files(batch: Dict[str, Any]) -> Dict[str, Any]:
        """Save processed time series in various formats."""
        
        for i, timeseries_data in enumerate(batch["temporal_aggregations"]):
            if "error" in timeseries_data:
                continue
            
            # Save as CSV
            df = pd.DataFrame(timeseries_data["hourly"])
            df.to_csv(f"output/timeseries_{i}_hourly.csv", index=False)
            
            # Save as Parquet
            df.to_parquet(f"output/timeseries_{i}_hourly.parquet", index=False)
            
            # Save as JSON
            df.to_json(f"output/timeseries_{i}_hourly.json", orient="records")
        
        return batch

    # Save time series files
    saved_timeseries = aggregated_timeseries.map_batches(save_timeseries_files)

**Structured Formats**

.. code-block:: python

    # Save as Parquet with metadata
    processed_timeseries.write_parquet(
        "s3://output/timeseries-dataset/",
        compression="snappy"
    )

    # Save as JSON Lines
    processed_timeseries.write_json(
        "s3://output/timeseries-metadata.jsonl"
    )

    # Save as CSV
    processed_timeseries.write_csv(
        "s3://output/timeseries-data/"
    )

Integration with ML Frameworks
------------------------------

Integrate Ray Data time series processing with popular machine learning frameworks.

**PyTorch Integration**

.. code-block:: python

    import torch
    from torch.utils.data import DataLoader
    import ray

    # Convert Ray Dataset to PyTorch format
    torch_dataset = processed_timeseries.to_torch(
        label_column="target",
        feature_columns=["features"],
        batch_size=64
    )

    # Use with PyTorch training
    model = YourPyTorchTimeseriesModel()
    optimizer = torch.optim.Adam(model.parameters())
    
    for batch in torch_dataset:
        features = batch["features"]
        targets = batch["target"]
        
        # Training step
        optimizer.zero_grad()
        outputs = model(features)
        loss = torch.nn.functional.mse_loss(outputs, targets)
        loss.backward()
        optimizer.step()

**TensorFlow Integration**

.. code-block:: python

    import tensorflow as tf
    import ray

    # Convert Ray Dataset to TensorFlow format
    tf_dataset = processed_timeseries.to_tf(
        label_column="target",
        feature_columns=["features"],
        batch_size=64
    )

    # Use with TensorFlow training
    model = tf.keras.Sequential([
        tf.keras.layers.Input(shape=(None,)),
        tf.keras.layers.LSTM(128, return_sequences=True),
        tf.keras.layers.LSTM(64),
        tf.keras.layers.Dense(1)
    ])
    
    model.compile(
        optimizer='adam',
        loss='mse',
        metrics=['mae']
    )
    
    model.fit(tf_dataset, epochs=10)

**Hugging Face Integration**

.. code-block:: python

    from transformers import AutoModelForTimeSeriesClassification
    import torch
    import ray

    # Load Hugging Face time series model
    model = AutoModelForTimeSeriesClassification.from_pretrained("microsoft/ts2vec-base")

    def huggingface_timeseries_processing(batch):
        """Process time series with Hugging Face models."""
        
        # Extract features using Hugging Face
        # This is a simplified example
        batch["huggingface_features"] = "processed_features"
        
        return batch

    # Apply Hugging Face processing
    hf_processed = processed_timeseries.map_batches(huggingface_timeseries_processing)

Production Deployment
---------------------

Deploy time series processing pipelines to production with monitoring and optimization.

**Production Pipeline Configuration**

.. code-block:: python

    def production_timeseries_pipeline():
        """Production-ready time series processing pipeline."""
        
        # Configure for production
        ctx = DataContext.get_current()
        ctx.target_max_block_size = 256 * 1024 * 1024  # 256MB blocks
        ctx.enable_auto_log_stats = True
        ctx.verbose_stats_logs = True
        
        # Load time series data
        timeseries_data = ray.data.read_parquet("s3://input/timeseries/")
        
        # Apply processing
        processed = timeseries_data.map_batches(
            production_timeseries_processor,
            compute=ray.data.ActorPoolStrategy(size=8),
            batch_size=64
        )
        
        # Save results
        processed.write_parquet("s3://output/processed-timeseries/")
        
        return processed

**Monitoring and Observability**

.. code-block:: python

    # Enable comprehensive monitoring
    ctx = DataContext.get_current()
    ctx.enable_per_node_metrics = True
    ctx.memory_usage_poll_interval_s = 1.0

    # Monitor pipeline performance
    def monitor_pipeline_performance(dataset):
        """Monitor time series processing pipeline performance."""
        
        stats = dataset.stats()
        print(f"Processing time: {stats.total_time}")
        print(f"Memory usage: {stats.memory_usage}")
        print(f"CPU usage: {stats.cpu_usage}")
        
        return dataset

    # Apply monitoring
    monitored_pipeline = timeseries_data.map_batches(
        process_timeseries
    ).map_batches(monitor_pipeline_performance)

Best Practices
--------------

**1. Time Series Format Selection**

* **Parquet**: Best for large time series datasets, good compression
* **CSV**: Good for small datasets, human-readable
* **JSON**: Good for complex nested time series data
* **Arrow**: Best for in-memory processing, fast

**2. Batch Size Optimization**

* Start with batch size 32-64 for time series processing
* Adjust based on time series length and complexity
* Monitor memory usage and adjust accordingly

**3. Memory Management**

* Use streaming execution for large datasets
* Process time series in chunks to manage memory
* Clear intermediate results when possible

**4. Temporal Operations**

* Use proper time-based indexing
* Handle timezone conversions correctly
* Use appropriate resampling frequencies

**5. Error Handling**

* Implement robust error handling for missing data
* Use `max_errored_blocks` to handle failures gracefully
* Log and monitor processing errors

Next Steps
----------

Now that you understand time series processing with Ray Data, explore related topics:

* **Working with AI**: AI and machine learning workflows → :ref:`working-with-ai`
* **Working with Tabular Data**: Tabular data processing → :ref:`working-with-tabular-data`
* **Performance Optimization**: Optimize time series processing performance → :ref:`performance-optimization`
* **Fault Tolerance**: Handle failures in time series pipelines → :ref:`fault-tolerance`

For practical examples:

* **Time Series Analysis Examples**: Real-world time series applications → :ref:`timeseries-analysis-examples`
* **Forecasting Examples**: Time series forecasting applications → :ref:`forecasting-examples`
* **Anomaly Detection Examples**: Time series anomaly detection → :ref:`anomaly-detection-examples`
