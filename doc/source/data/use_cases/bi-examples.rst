.. _bi-examples:

Business Intelligence Examples
==============================

Business Intelligence examples demonstrate how to create analytics, reports, and insights using Ray Data. These examples show complete implementations that serve data engineers, data scientists, and business stakeholders.

What are BI examples?
---------------------

BI examples are complete analytical workflows that demonstrate:

* **Executive dashboards**: High-level metrics for decision makers
* **Operational reporting**: Detailed analytics for business operations
* **Customer analytics**: Customer behavior and segmentation analysis
* **Financial reporting**: Revenue, profit, and cost analysis
* **Performance monitoring**: KPI tracking and trend analysis

Why Ray Data transforms business intelligence
--------------------------------------------

* **Break BI tool limits**: Analyze datasets that crash traditional BI platforms
* **Add AI to BI workflows**: Apply machine learning models to business metrics
* **Speed up dashboards**: Pre-calculate complex metrics for instant dashboard loading
* **Unify all data sources**: Combine customer databases with social media sentiment in single analyses

How these examples serve different roles
----------------------------------------

**For Executives:**
* Clear business value and ROI demonstration
* High-level metrics and KPI calculations
* Strategic insights from data analysis

**For Data Engineers:**
* Production-ready pipeline implementations
* Performance optimization techniques
* Integration with existing data infrastructure

**For Data Scientists:**
* Advanced analytical techniques and statistical methods
* Feature engineering for predictive models
* Experimental analysis frameworks

**For AI Engineers:**
* Integration of traditional BI with AI/ML capabilities
* Multimodal data analysis combining structured and unstructured data
* Scalable inference and prediction pipelines

Executive Sales Dashboard
-------------------------

This example creates a comprehensive sales dashboard that provides executive-level insights into business performance. The dashboard combines revenue metrics, customer analytics, and operational KPIs.

**Business Value for Executives:**

Executive dashboards enable data-driven decision making by providing:

* **Revenue visibility**: Frequent revenue tracking and forecasting
* **Customer insights**: Customer acquisition, retention, and lifetime value metrics
* **Operational efficiency**: Performance metrics across regions and product lines
* **Competitive intelligence**: Market position and growth trend analysis

**Technical Implementation for Data Engineers:**

The implementation uses Ray Data's distributed processing to handle large-scale sales data efficiently, ensuring dashboard updates complete within acceptable time windows.

.. code-block:: python

    import ray
    from ray.data.aggregate import Sum, Count, Mean, Max, Min
    from datetime import datetime, timedelta

    class ExecutiveDashboardPipeline:
        def __init__(self):
            self.current_date = datetime.now()
            self.metrics = {}
        
        def extract_sales_data(self):
            """Extract sales data from multiple sources"""
            
            # Load sales transactions
            sales = ray.data.read_parquet("s3://data/sales-transactions/")
            
            # Load customer data
            customers = ray.data.read_csv("s3://data/customers.csv")
            
            # Load product catalog
            products = ray.data.read_json("s3://data/products.json")
            
            return sales, customers, products

**Revenue Analytics**

Revenue analytics provide the foundation for executive decision making, showing not just current performance but trends and forecasts.

.. code-block:: python

        def calculate_revenue_metrics(self, sales, customers, products):
            """Calculate comprehensive revenue metrics"""
            
            # Join all data sources for comprehensive analysis
            enriched_sales = sales.join(customers, on="customer_id") \
                                  .join(products, on="product_id")
            
            # Calculate daily revenue trends
            daily_revenue = enriched_sales.groupby("order_date").aggregate(
                Sum("revenue"),
                Count("transaction_id"),
                Mean("order_value")
            )
            
            # Calculate regional performance
            regional_performance = enriched_sales.groupby("region").aggregate(
                Sum("revenue"),
                Count("customer_id"),
                Mean("revenue")
            )
            
            return daily_revenue, regional_performance

**Customer Lifetime Value Analysis**

CLV analysis helps executives understand customer value distribution and retention strategies.

.. code-block:: python

        def analyze_customer_value(self, enriched_sales):
            """Analyze customer lifetime value and segmentation"""
            
            # Calculate customer metrics
            customer_metrics = enriched_sales.groupby("customer_id").aggregate(
                Sum("revenue"),
                Count("transaction_id"),
                Max("order_date"),
                Mean("order_value")
            )
            
            # Add customer segmentation
            def assign_customer_tier(row):
                total_revenue = row["sum(revenue)"]
                if total_revenue >= 10000:
                    row["tier"] = "VIP"
                    row["tier_priority"] = 1
                elif total_revenue >= 5000:
                    row["tier"] = "Premium" 
                    row["tier_priority"] = 2
                elif total_revenue >= 1000:
                    row["tier"] = "Standard"
                    row["tier_priority"] = 3
                else:
                    row["tier"] = "Basic"
                    row["tier_priority"] = 4
                return row
            
            segmented_customers = customer_metrics.map(assign_customer_tier)
            
            return segmented_customers

**Executive Summary Generation**

The executive summary distills complex data into actionable insights for leadership decision making.

.. code-block:: python

        def generate_executive_summary(self, daily_revenue, customer_segments):
            """Generate executive summary metrics"""
            
            def calculate_summary_metrics(batch):
                import numpy as np
                
                # Calculate key executive metrics
                total_revenue = np.sum(batch["sum(revenue)"])
                total_transactions = np.sum(batch["count(transaction_id)"])
                avg_order_value = np.mean(batch["mean(order_value)"])
                
                return {
                    "total_revenue": round(total_revenue, 2),
                    "total_transactions": int(total_transactions),
                    "average_order_value": round(avg_order_value, 2),
                    "revenue_per_transaction": round(total_revenue / total_transactions, 2) if total_transactions > 0 else 0
                }
            
            executive_metrics = daily_revenue.map_batches(calculate_summary_metrics)
            
            return executive_metrics

Customer Behavior Analysis
--------------------------

This example demonstrates advanced customer analytics that help businesses understand customer behavior patterns and optimize marketing strategies.

**Business Value for Marketing Teams:**

Customer behavior analysis enables:

* **Segmentation strategies**: Identify high-value customer segments for targeted marketing
* **Retention insights**: Understand factors that drive customer loyalty
* **Campaign optimization**: Optimize marketing spend based on customer value
* **Personalization**: Enable personalized customer experiences based on behavior patterns

**Advanced Analytics for Data Scientists:**

The analysis combines traditional BI metrics with statistical analysis and predictive modeling techniques.

.. code-block:: python

    class CustomerBehaviorAnalysis:
        def __init__(self):
            self.analysis_date = datetime.now()
        
        def perform_rfm_analysis(self, customer_transactions):
            """Perform Recency, Frequency, Monetary (RFM) analysis"""
            
            # Calculate RFM components using Ray Data native operations
            rfm_metrics = customer_transactions.groupby("customer_id").aggregate(
                Max("transaction_date"),  # Recency component
                Count("transaction_id"),  # Frequency component  
                Sum("amount")            # Monetary component
            )
            
            return rfm_metrics

**Statistical Analysis**

Statistical analysis provides deeper insights into customer behavior patterns and helps identify opportunities for business improvement.

.. code-block:: python

        def calculate_statistical_insights(self, customer_data):
            """Calculate statistical insights for customer behavior"""
            
            # Calculate distribution statistics
            revenue_stats = customer_data.aggregate(
                Mean("sum(amount)"),
                ray.data.aggregate.Std("sum(amount)"),
                Min("sum(amount)"),
                Max("sum(amount)")
            )
            
            # Calculate percentiles for segmentation
            def calculate_percentiles(batch):
                import numpy as np
                
                amounts = np.array(batch["sum(amount)"])
                return {
                    "percentile_25": np.percentile(amounts, 25),
                    "percentile_50": np.percentile(amounts, 50),
                    "percentile_75": np.percentile(amounts, 75),
                    "percentile_90": np.percentile(amounts, 90)
                }
            
            percentile_stats = customer_data.map_batches(calculate_percentiles)
            
            return revenue_stats, percentile_stats

**Predictive Customer Scoring**

Predictive scoring helps identify customers likely to churn or increase spending, enabling proactive business actions.

.. code-block:: python

        def create_predictive_scores(self, rfm_metrics):
            """Create predictive customer scores"""
            
            def calculate_customer_scores(row):
                recency_days = (self.analysis_date - 
                               datetime.fromisoformat(row["max(transaction_date)"])).days
                frequency = row["count(transaction_id)"]
                monetary = row["sum(amount)"]
                
                # Simple scoring algorithm (can be enhanced with ML models)
                recency_score = max(0, 100 - (recency_days / 10))  # Decay over time
                frequency_score = min(100, frequency * 10)         # Cap at 100
                monetary_score = min(100, monetary / 100)          # Scale to 0-100
                
                # Weighted composite score
                row["customer_score"] = (
                    recency_score * 0.3 + 
                    frequency_score * 0.4 + 
                    monetary_score * 0.3
                )
                
                # Risk assessment
                if recency_days > 180:
                    row["churn_risk"] = "High"
                elif recency_days > 90:
                    row["churn_risk"] = "Medium"
                else:
                    row["churn_risk"] = "Low"
                
                return row
            
            scored_customers = rfm_metrics.map(calculate_customer_scores)
            
            return scored_customers

Operational Performance Dashboard
--------------------------------

This example creates operational dashboards that help businesses monitor day-to-day performance and identify operational improvements.

**Business Value for Operations Teams:**

Operational dashboards provide:

* **Frequent monitoring**: Track key operational metrics with regular updates
* **Performance benchmarking**: Compare current performance against historical trends
* **Anomaly detection**: Identify unusual patterns that require attention
* **Resource optimization**: Understand resource utilization and optimization opportunities

**Implementation for AI Engineers:**

The implementation demonstrates how to combine traditional operational metrics with AI-powered insights for enhanced decision making.

.. code-block:: python

    class OperationalDashboard:
        def __init__(self):
            self.monitoring_window = timedelta(hours=24)
        
        def create_frequent_metrics(self, transaction_stream):
            """Create frequently updated operational metrics"""
            
            # Filter recent transactions for frequent analysis
            recent_transactions = transaction_stream.filter(
                lambda row: (datetime.now() - 
                           datetime.fromisoformat(row["timestamp"])) < self.monitoring_window
            )
            
            # Calculate operational KPIs
            operational_kpis = recent_transactions.map_batches(
                self.calculate_operational_metrics
            )
            
            return operational_kpis

**Frequent KPI Calculation**

Frequently updated KPIs enable timely visibility into business performance and operational health.

.. code-block:: python

        def calculate_operational_metrics(self, batch):
            """Calculate operational KPIs from transaction data"""
            import numpy as np
            
            # Calculate basic operational metrics
            transaction_count = len(batch["transaction_id"])
            total_revenue = np.sum(batch["amount"])
            avg_transaction_value = np.mean(batch["amount"]) if transaction_count > 0 else 0
            
            # Calculate performance indicators
            high_value_transactions = np.sum(batch["amount"] >= 1000)
            error_rate = np.sum([status != "success" for status in batch["status"]]) / transaction_count
            
            return {
                "timestamp": datetime.now().isoformat(),
                "total_transactions": transaction_count,
                "total_revenue": round(total_revenue, 2),
                "average_transaction_value": round(avg_transaction_value, 2),
                "high_value_transaction_count": int(high_value_transactions),
                "error_rate_percentage": round(error_rate * 100, 2)
            }

**Anomaly Detection for Operations**

Anomaly detection helps operations teams identify issues before they impact business performance.

.. code-block:: python

        def detect_operational_anomalies(self, metrics_data):
            """Detect anomalies in operational metrics"""
            
            def analyze_patterns(batch):
                import numpy as np
                
                # Calculate statistical thresholds
                revenue_values = np.array(batch["total_revenue"])
                revenue_mean = np.mean(revenue_values)
                revenue_std = np.std(revenue_values)
                
                # Identify anomalies (values beyond 2 standard deviations)
                anomaly_threshold = revenue_mean + (2 * revenue_std)
                
                anomalies = []
                for i, revenue in enumerate(revenue_values):
                    if revenue > anomaly_threshold:
                        anomalies.append({
                            "timestamp": batch["timestamp"][i],
                            "metric": "revenue",
                            "value": revenue,
                            "threshold": anomaly_threshold,
                            "severity": "high" if revenue > anomaly_threshold * 1.5 else "medium"
                        })
                
                return {"anomalies": anomalies}
            
            anomaly_results = metrics_data.map_batches(analyze_patterns)
            
            return anomaly_results

Financial Performance Analysis
------------------------------

This example demonstrates financial analysis capabilities that help CFOs and financial analysts understand business performance and profitability.

**Executive Value Proposition:**

Financial analysis provides executives with:

* **Profitability insights**: Understand which products, regions, and customers drive profitability
* **Cost optimization**: Identify areas where costs can be reduced without impacting revenue
* **Investment planning**: Data-driven insights for resource allocation decisions
* **Risk assessment**: Financial risk indicators and trend analysis

**Advanced Analytics for Finance Teams:**

The financial analysis combines traditional accounting metrics with predictive analytics to provide forward-looking insights.

.. code-block:: python

    class FinancialPerformanceAnalysis:
        def __init__(self):
            self.fiscal_year_start = datetime(2024, 1, 1)
        
        def calculate_profitability_metrics(self, sales_data, cost_data):
            """Calculate comprehensive profitability metrics"""
            
            # Join sales and cost data for profit calculation
            profit_data = sales_data.join(cost_data, on="product_id")
            
            # Calculate profit margins using Ray Data native operations
            profit_analysis = profit_data.map_batches(self.compute_profit_margins)
            
            return profit_analysis

**Profit Margin Analysis**

Profit margin analysis helps identify the most profitable products, customers, and market segments.

.. code-block:: python

        def compute_profit_margins(self, batch):
            """Compute profit margins and financial KPIs"""
            import numpy as np
            
            # Calculate basic profit metrics
            revenue = np.array(batch["revenue"])
            cost = np.array(batch["cost"])
            profit = revenue - cost
            
            # Calculate margin percentages
            profit_margin = np.divide(profit, revenue, 
                                    out=np.zeros_like(profit), 
                                    where=revenue!=0) * 100
            
            # Add financial categorization
            batch["profit"] = profit
            batch["profit_margin_percent"] = profit_margin
            
            # Categorize profitability
            batch["profitability_tier"] = np.array([
                "High" if margin >= 30 else
                "Medium" if margin >= 15 else
                "Low" if margin >= 5 else
                "Negative" for margin in profit_margin
            ])
            
            return batch

**Financial Forecasting**

Financial forecasting helps executives plan for future business performance and resource requirements.

.. code-block:: python

        def create_financial_forecast(self, historical_data):
            """Create financial forecasts based on historical trends"""
            
            # Calculate monthly trends for forecasting
            monthly_trends = historical_data.map(self.extract_monthly_data) \
                                          .groupby(["year", "month"]).aggregate(
                                              Sum("revenue"),
                                              Sum("profit"),
                                              Count("transaction_id")
                                          )
            
            # Apply simple trend analysis for forecasting
            forecast_data = monthly_trends.map_batches(self.calculate_trend_forecast)
            
            return forecast_data
        
        def extract_monthly_data(self, row):
            """Extract monthly dimensions for trend analysis"""
            from datetime import datetime
            
            order_date = datetime.fromisoformat(row["order_date"])
            row["year"] = order_date.year
            row["month"] = order_date.month
            row["quarter"] = f"Q{(order_date.month-1)//3 + 1}"
            
            return row
        
        def calculate_trend_forecast(self, batch):
            """Calculate simple trend-based forecast"""
            import numpy as np
            
            # Simple linear trend calculation
            revenues = np.array(batch["sum(revenue)"])
            months = np.arange(len(revenues))
            
            # Calculate trend slope
            if len(revenues) > 1:
                trend_slope = np.polyfit(months, revenues, 1)[0]
                
                # Project next 3 months
                next_month_forecast = revenues[-1] + trend_slope
                forecast_confidence = "High" if abs(trend_slope) > revenues[-1] * 0.1 else "Medium"
            else:
                next_month_forecast = revenues[0] if len(revenues) > 0 else 0
                forecast_confidence = "Low"
            
            return {
                "next_month_revenue_forecast": round(next_month_forecast, 2),
                "trend_direction": "Growing" if trend_slope > 0 else "Declining",
                "forecast_confidence": forecast_confidence
            }

Product Performance Analytics
----------------------------

Product performance analytics help product managers and executives understand which products drive business success and which need attention.

**Strategic Value for Product Teams:**

Product analytics enable:

* **Portfolio optimization**: Identify top-performing products and underperformers
* **Market insights**: Understand customer preferences and market trends
* **Pricing optimization**: Data-driven pricing strategies based on performance
* **Innovation guidance**: Insights for new product development and features

.. code-block:: python

    class ProductPerformanceAnalytics:
        def __init__(self):
            self.analysis_period = timedelta(days=90)
        
        def analyze_product_portfolio(self, sales_data, products):
            """Analyze complete product portfolio performance"""
            
            # Join sales with product information
            product_sales = sales_data.join(products, on="product_id")
            
            # Calculate product performance metrics
            product_metrics = product_sales.groupby(["product_id", "category"]).aggregate(
                Sum("revenue"),
                Sum("quantity"),
                Count("transaction_id"),
                Mean("unit_price")
            )
            
            # Add performance categorization
            categorized_products = product_metrics.map(self.categorize_product_performance)
            
            return categorized_products

**Product Performance Categorization**

Performance categorization helps prioritize product management efforts and resource allocation.

.. code-block:: python

        def categorize_product_performance(self, row):
            """Categorize products based on performance metrics"""
            
            revenue = row["sum(revenue)"]
            quantity = row["sum(quantity)"]
            transaction_count = row["count(transaction_id)"]
            
            # Calculate performance score
            revenue_score = min(100, revenue / 10000)  # Scale revenue to 0-100
            volume_score = min(100, quantity / 1000)   # Scale volume to 0-100
            popularity_score = min(100, transaction_count / 100)  # Scale popularity to 0-100
            
            # Weighted performance score
            performance_score = (revenue_score * 0.5 + volume_score * 0.3 + popularity_score * 0.2)
            
            # Categorize products
            if performance_score >= 80:
                row["performance_category"] = "Star Products"
                row["strategic_action"] = "Invest and Expand"
            elif performance_score >= 60:
                row["performance_category"] = "Solid Performers"
                row["strategic_action"] = "Maintain and Optimize"
            elif performance_score >= 40:
                row["performance_category"] = "Needs Attention"
                row["strategic_action"] = "Analyze and Improve"
            else:
                row["performance_category"] = "Underperformers"
                row["strategic_action"] = "Review or Discontinue"
            
            row["performance_score"] = round(performance_score, 1)
            
            return row

Competitive Intelligence Dashboard
---------------------------------

This example shows how to create competitive intelligence dashboards that help executives understand market position and competitive dynamics.

**Strategic Value for Leadership:**

Competitive intelligence provides:

* **Market position**: Understand relative performance in the market
* **Competitive benchmarking**: Compare performance against industry standards
* **Opportunity identification**: Identify market gaps and growth opportunities
* **Strategic planning**: Data-driven input for strategic business decisions

.. code-block:: python

    class CompetitiveIntelligenceAnalysis:
        def __init__(self):
            self.market_data_sources = ["internal_sales", "market_research", "public_data"]
        
        def analyze_market_position(self, internal_sales, market_data):
            """Analyze competitive market position"""
            
            # Calculate internal market share
            internal_metrics = internal_sales.groupby("product_category").aggregate(
                Sum("revenue"),
                Count("customer_id")
            )
            
            # Compare with market data
            market_position = internal_metrics.join(market_data, on="product_category")
            
            # Calculate competitive metrics
            competitive_analysis = market_position.map(self.calculate_market_share)
            
            return competitive_analysis

**Market Share Calculation**

Market share analysis provides quantitative insights into competitive position and growth opportunities.

.. code-block:: python

        def calculate_market_share(self, row):
            """Calculate market share and competitive metrics"""
            
            internal_revenue = row["sum(revenue)"]
            total_market_revenue = row["total_market_revenue"]
            
            # Calculate market share percentage
            market_share = (internal_revenue / total_market_revenue * 100) if total_market_revenue > 0 else 0
            
            # Determine competitive position
            if market_share >= 25:
                row["market_position"] = "Market Leader"
                row["strategic_priority"] = "Defend and Expand"
            elif market_share >= 15:
                row["market_position"] = "Strong Competitor"
                row["strategic_priority"] = "Grow Market Share"
            elif market_share >= 5:
                row["market_position"] = "Niche Player"
                row["strategic_priority"] = "Focus and Differentiate"
            else:
                row["market_position"] = "Challenger"
                row["strategic_priority"] = "Disrupt or Exit"
            
            row["market_share_percent"] = round(market_share, 2)
            
            return row

Best Practices for BI Implementation
-----------------------------------

**For Data Engineers:**

* **Pipeline reliability**: Implement robust error handling and data quality checks
* **Performance optimization**: Pre-aggregate common metrics for faster BI tool consumption
* **Data freshness**: Balance update frequency with processing costs
* **Scalability planning**: Design pipelines that handle growing data volumes

**For Data Scientists:**

* **Statistical rigor**: Apply appropriate statistical methods for business insights
* **Model integration**: Combine traditional BI with predictive analytics
* **Validation frameworks**: Implement comprehensive testing for analytical logic
* **Documentation**: Maintain clear documentation of analytical methodologies

**For Business Stakeholders:**

* **Metric definition**: Clearly define business metrics and calculation methods
* **Visualization strategy**: Structure data for effective dashboard presentation
* **Action orientation**: Design analytics to support specific business decisions
* **Performance monitoring**: Track the business impact of data-driven insights

Export and Integration Patterns
-------------------------------

**Dashboard Data Export**

Prepare data for consumption by various dashboard and reporting tools:

.. code-block:: python

    def export_dashboard_data(executive_metrics, customer_analytics, product_performance):
        """Export processed data for dashboard consumption"""
        
        # Export executive summary as JSON for web dashboards
        executive_metrics.write_json("s3://dashboards/executive/summary.json")
        
        # Export customer analytics as CSV for Excel integration
        customer_analytics.write_csv("s3://dashboards/customers/analytics.csv")
        
        # Export product performance as Parquet for analytical tools
        product_performance.write_parquet("s3://dashboards/products/performance.parquet")

**Performance Optimization for BI Tools**

Optimize data structure and format for BI tool performance:

.. code-block:: python

    def optimize_for_bi_consumption(processed_data):
        """Optimize data structure for BI tool performance"""
        
        # Create pre-aggregated summaries for fast loading
        daily_summary = processed_data.groupby("date").aggregate(
            Sum("revenue"),
            Count("transactions")
        )
        
        # Create dimension tables for efficient joins
        customer_dimension = processed_data.select_columns([
            "customer_id", "customer_name", "segment", "tier"
        ]).unique("customer_id")
        
        # Export optimized datasets
        daily_summary.write_parquet("s3://bi-optimized/daily_summary.parquet")
        customer_dimension.write_csv("s3://bi-optimized/customer_dimension.csv")

Next Steps
----------

* Learn about :ref:`Business Intelligence <business-intelligence>` for comprehensive BI capabilities
* Explore :ref:`Advanced Analytics <advanced-analytics>` for statistical analysis techniques
* See :ref:`Performance Optimization <performance-optimization>` for scaling BI workloads
* Review :ref:`Integration Examples <integration-examples>` for connecting with BI tools
