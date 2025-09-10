.. _bi-tools:

Business Intelligence Data Preparation
=====================================

Ray Data excels at preparing and optimizing data for consumption by business intelligence platforms. This guide covers data preparation patterns, export optimization, and best practices for BI workflows.

**What you'll learn:**

* Data preparation and optimization for BI tool consumption
* Export formats and structures optimized for BI platforms
* Batch processing patterns for dashboard data preparation
* Performance optimization for analytical workloads

Data Preparation for BI Platforms
---------------------------------

**Optimizing Data for BI Tool Consumption**

While Ray Data doesn't provide native BI tool connectors, it excels at preparing data in formats optimized for BI platform consumption:

.. code-block:: python

    import ray
    import pandas as pd

    def prepare_bi_optimized_data():
        """Prepare data optimized for BI platform consumption."""
        
        # Load source data
        sales_data = ray.data.read_snowflake(
            "snowflake://account/database/schema",
            """
            SELECT s.sale_date, s.customer_id, c.customer_name, 
                   s.product_id, p.product_name, p.product_category,
                   s.quantity, s.unit_price, s.total_amount, s.region
            FROM sales s
            JOIN customers c ON s.customer_id = c.customer_id
            JOIN products p ON s.product_id = p.product_id
            WHERE s.sale_date >= CURRENT_DATE - 365
            """
        )
        
        def optimize_for_bi_consumption(batch):
            """Optimize data structure for BI platform consumption using Ray Data native operations."""
            import numpy as np
            from datetime import datetime
            
            # Create time dimensions using Ray Data native operations
            dates = batch['sale_date']
            years = np.array([int(date.split('-')[0]) for date in dates])
            months = np.array([int(date.split('-')[1]) for date in dates])
            quarters = np.array([(month - 1) // 3 + 1 for month in months])
            
            batch['year'] = years.astype('int32')
            batch['month'] = months.astype('int32')
            batch['quarter'] = quarters.astype('int32')
            
            # Create month names using Ray Data native operations
            month_names = ["", "January", "February", "March", "April", "May", "June",
                          "July", "August", "September", "October", "November", "December"]
            batch['month_name'] = np.array([month_names[month] for month in months])
            
            # Optimize numeric columns using numpy
            batch['quantity'] = batch['quantity'].astype('int32')
            batch['unit_price'] = batch['unit_price'].astype('float32')
            batch['total_amount'] = batch['total_amount'].astype('float32')
            
            # Create calculated fields using Ray Data native operations
            batch['revenue_per_unit'] = batch['total_amount'] / batch['quantity']
            batch['is_high_value'] = (batch['total_amount'] > 1000).astype(bool)
            
            # Create hierarchies for drill-down using string operations
            batch['time_hierarchy'] = np.array([
                f"{year} > Q{quarter} > {month_name}"
                for year, quarter, month_name in zip(years, quarters, batch['month_name'])
            ])
            
            return batch
        
        bi_optimized = sales_data.map_batches(optimize_for_bi_consumption)
        
        # Export in multiple formats for different BI tools
        bi_optimized.write_csv("s3://bi-data/sales_detail.csv")
        bi_optimized.write_parquet("s3://bi-data/sales_detail.parquet")
        
        return bi_optimized

Multi-Format BI Data Export
---------------------------

**Preparing Data for Multiple BI Platforms**

.. code-block:: python

    def prepare_analytical_data():
        """Prepare data optimized for analytical tool consumption."""
        
        business_data = ray.data.read_parquet("s3://processed/business_metrics/")
        
        def optimize_for_analytics(batch):
            """Optimize data for analytical tool consumption."""
            
            # Create time-based dimensions for analysis
            batch['year_month'] = batch['date'].dt.to_period('M').astype(str)
            batch['year_quarter'] = batch['date'].dt.to_period('Q').astype(str)
            
            # Create hierarchical columns for drill-down analysis
            batch['geography_hierarchy'] = (
                batch['country'] + ' > ' + 
                batch['region'] + ' > ' + 
                batch['city']
            )
            
            # Pre-calculate common business metrics
            batch['revenue_usd'] = batch['revenue'] * batch['exchange_rate']
            batch['profit_margin_percent'] = (batch['profit'] / batch['revenue']) * 100
            
            # Create analytical flag columns
            batch['is_weekend'] = batch['date'].dt.dayofweek.isin([5, 6])
            batch['is_high_value_customer'] = batch['customer_ltv'] > 5000
            
            return batch
        
        analytics_optimized = business_data.map_batches(optimize_for_analytics)
        
        # Export in multiple formats for different analytical tools
        analytics_optimized.write_parquet("s3://analytics-data/business_metrics.parquet")
        analytics_optimized.write_csv("s3://analytics-data/business_metrics.csv")
        
        return analytics_optimized

Data Warehouse Export Optimization
----------------------------------

**Preparing Data for Data Warehouse Consumption**

.. code-block:: python

    def prepare_looker_data():
        """Prepare data optimized for Looker's SQL-based approach."""
        
        source_data = ray.data.read_bigquery(
            "project.dataset.sales_data",
            project_id="your-gcp-project"
        )
        
        def optimize_for_looker(batch):
            """Optimize for Looker's data modeling."""
            
            # Ensure consistent date formatting
            batch['sale_date'] = pd.to_datetime(batch['sale_date'])
            
            # Create dimension keys for joins
            batch['date_key'] = batch['sale_date'].dt.strftime('%Y%m%d')
            batch['customer_key'] = 'CUST_' + batch['customer_id'].astype(str)
            batch['product_key'] = 'PROD_' + batch['product_id'].astype(str)
            
            # Ensure proper data types
            batch['customer_id'] = batch['customer_id'].astype('int64')
            batch['total_amount'] = batch['total_amount'].astype('float64')
            
            # Clean string fields
            batch['customer_name'] = batch['customer_name'].str.strip()
            batch['region'] = batch['region'].str.upper().str.strip()
            
            return batch
        
        looker_optimized = source_data.map_batches(optimize_for_looker)
        
        # Write to BigQuery for Looker
        looker_optimized.write_bigquery(
            "project.looker_data.sales_fact",
            project_id="your-gcp-project",
            mode="overwrite"
        )
        
        return looker_optimized

Frequent Dashboard Updates
-------------------------

**Batch Dashboard Data Refresh**

.. code-block:: python

    def setup_frequent_updates():
        """Set up frequent dashboard data updates through batch processing."""
        
        def process_streaming_updates():
            """Process streaming data for dashboard updates."""
            
            # Read incremental data
            new_data_paths = ["s3://streaming-data/batch_001/", "s3://streaming-data/batch_002/"]
            
            for batch_path in new_data_paths:
                new_data = ray.data.read_json(batch_path)
                
                # Calculate incremental metrics
                def calculate_incremental_metrics(batch):
                    current_hour = datetime.now().replace(minute=0, second=0, microsecond=0)
                    
                    hourly_metrics = {
                        'hour': current_hour.isoformat(),
                        'total_transactions': len(batch),
                        'total_revenue': batch['amount'].sum(),
                        'avg_transaction_value': batch['amount'].mean(),
                        'unique_customers': batch['customer_id'].nunique()
                    }
                    
                    return pd.DataFrame([hourly_metrics])
                
                incremental_metrics = new_data.map_batches(calculate_incremental_metrics)
                
                # Update dashboard data
                incremental_metrics.write_sql(
                    "postgresql://dashboard-db:5432/dashboard",
                    "hourly_metrics",
                    mode="append"
                )
                
                print(f"Updated dashboard with {new_data.count()} new records")

**Dashboard Refresh Automation**

.. code-block:: python

    class DashboardRefreshManager:
        """Automate dashboard refreshes after data updates."""
        
        def __init__(self):
            self.refresh_configs = {}
            self.last_refresh_times = {}
        
        def register_dashboard(self, dashboard_id: str, refresh_config: Dict):
            """Register dashboard for automated refresh."""
            
            self.refresh_configs[dashboard_id] = {
                'bi_tool': refresh_config['bi_tool'],
                'refresh_method': refresh_config['refresh_method'],
                'min_refresh_interval': refresh_config.get('min_refresh_interval', 300)
            }
        
        def trigger_refresh(self, dashboard_id: str):
            """Trigger dashboard refresh."""
            
            config = self.refresh_configs[dashboard_id]
            bi_tool = config['bi_tool']
            
            if bi_tool == 'tableau':
                return self._refresh_tableau(dashboard_id, config)
            elif bi_tool == 'powerbi':
                return self._refresh_powerbi(dashboard_id, config)
            elif bi_tool == 'looker':
                return self._refresh_looker(dashboard_id, config)
            
            return False
        
        def _refresh_tableau(self, dashboard_id: str, config: Dict):
            """Refresh Tableau dashboard."""
            # Implementation depends on Tableau Server API
            print(f"Refreshing Tableau dashboard: {dashboard_id}")
            return True

Best Practices
--------------

**1. Optimize for BI Performance**

* Use appropriate data types to minimize memory usage
* Pre-calculate common measures and KPIs
* Create summary tables for complex calculations
* Implement proper indexing and partitioning

**2. Design for User Experience**

* Create intuitive hierarchies for drill-down
* Provide clear column names and descriptions
* Implement proper data formatting
* Ensure consistent data quality

**3. Automate Refresh Processes**

* Set up automated refresh after data updates
* Implement intelligent scheduling
* Monitor refresh success rates
* Provide fallback mechanisms

**4. Monitor Performance**

* Track dashboard load times
* Monitor query performance
* Optimize based on usage patterns
* Implement caching strategies

Next Steps
----------

* **Data Warehouses**: Connect with warehouse platforms → :ref:`data-warehouses`
* **ETL Tools**: Integrate with orchestration tools → :ref:`etl-tools`
* **Performance Optimization**: Optimize BI workloads → :ref:`performance-optimization`