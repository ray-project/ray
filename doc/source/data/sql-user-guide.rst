.. _data_sql_user_guide:

===============
SQL User Guide
===============

This guide covers advanced SQL operations, optimization techniques, and best practices for using Ray Data SQL in production workloads.

.. contents::
   :local:
   :depth: 2

Advanced SQL operations
=======================

Complex joins
-------------

Ray Data SQL supports various join types for combining datasets:

.. testcode::

    import ray.data
    from ray.data.sql import register_table, sql

    # Sample data for advanced join examples
    employees = ray.data.from_items([
        {"id": 1, "name": "Alice", "dept_id": 10, "salary": 70000, "manager_id": None},
        {"id": 2, "name": "Bob", "dept_id": 20, "salary": 60000, "manager_id": 1},
        {"id": 3, "name": "Charlie", "dept_id": 10, "salary": 55000, "manager_id": 1},
        {"id": 4, "name": "Diana", "dept_id": 30, "salary": 80000, "manager_id": None}
    ])
    
    departments = ray.data.from_items([
        {"id": 10, "name": "Engineering", "budget": 500000},
        {"id": 20, "name": "Marketing", "budget": 200000},
        {"id": 30, "name": "Sales", "budget": 300000}
    ])

    register_table("employees", employees)
    register_table("departments", departments)

    # Self-join to find manager-employee relationships
    result = sql("""
        SELECT e.name as employee, 
               m.name as manager,
               e.salary
        FROM employees e
        LEFT JOIN employees m ON e.manager_id = m.id
    """)

    # Multi-table joins with aggregation
    result = sql("""
        SELECT d.name as department,
               d.budget,
               COUNT(e.id) as employee_count,
               AVG(e.salary) as avg_salary,
               SUM(e.salary) as total_salary_cost,
               (d.budget - SUM(e.salary)) as budget_remaining
        FROM departments d
        LEFT JOIN employees e ON d.id = e.dept_id
        GROUP BY d.id, d.name, d.budget
        ORDER BY budget_remaining DESC
    """)

Subqueries and CTEs
-------------------

Use subqueries and Common Table Expressions for complex data analysis:

.. testcode::

    # Subquery in WHERE clause
    high_performers = sql("""
        SELECT name, salary, dept_id
        FROM employees
        WHERE salary > (
            SELECT AVG(salary) * 1.1 
            FROM employees
        )
    """)

    # Correlated subquery
    above_dept_avg = sql("""
        SELECT e1.name, e1.salary, e1.dept_id
        FROM employees e1
        WHERE e1.salary > (
            SELECT AVG(e2.salary)
            FROM employees e2
            WHERE e2.dept_id = e1.dept_id
        )
    """)

    # Common Table Expression (CTE)
    result = sql("""
        WITH dept_stats AS (
            SELECT dept_id,
                   AVG(salary) as avg_salary,
                   COUNT(*) as employee_count
            FROM employees
            GROUP BY dept_id
        ),
        enriched_employees AS (
            SELECT e.name,
                   e.salary,
                   d.name as dept_name,
                   ds.avg_salary,
                   (e.salary - ds.avg_salary) as salary_diff
            FROM employees e
            JOIN departments d ON e.dept_id = d.id
            JOIN dept_stats ds ON e.dept_id = ds.dept_id
        )
        SELECT *
        FROM enriched_employees
        WHERE salary_diff > 0
        ORDER BY salary_diff DESC
    """)

Window functions and analytics
-------------------------------

Perform advanced analytics with window functions:

.. testcode::

    # Note: Window functions support may vary - check current capabilities
    
    # Ranking within groups
    ranked_salaries = sql("""
        SELECT name,
               salary,
               dept_id,
               ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) as rank_in_dept,
               LAG(salary) OVER (PARTITION BY dept_id ORDER BY salary) as prev_salary
        FROM employees
    """)

    # Running totals and percentiles
    analytics = sql("""
        SELECT name,
               salary,
               SUM(salary) OVER (ORDER BY salary ROWS UNBOUNDED PRECEDING) as running_total,
               PERCENT_RANK() OVER (ORDER BY salary) as salary_percentile
        FROM employees
        ORDER BY salary
    """)

Data types and type handling
=============================

Schema inference
----------------

Ray Data SQL automatically infers data types from your datasets:

.. testcode::

    from ray.data.sql import get_schema

    # Check inferred schema
    schema = get_schema("employees")
    print(f"Employees schema: {schema}")

    # Handle mixed types gracefully
    mixed_data = ray.data.from_items([
        {"id": 1, "value": 100, "note": "first"},
        {"id": 2, "value": 200.5, "note": "second"},
        {"id": 3, "value": "300", "note": None}  # String number and null
    ])
    
    register_table("mixed_data", mixed_data)

Type conversions
----------------

Perform explicit type conversions in your queries:

.. testcode::

    # Cast operations
    result = sql("""
        SELECT id,
               CAST(value AS FLOAT) as numeric_value,
               COALESCE(note, 'no note') as note_with_default,
               CASE 
                   WHEN value > 150 THEN 'high'
                   WHEN value > 100 THEN 'medium'
                   ELSE 'low'
               END as category
        FROM mixed_data
    """)

Performance optimization
========================

Query optimization strategies
----------------------------

Follow these best practices for optimal performance:

.. testcode::

    from ray.data.sql import SQLConfig, LogLevel

    # Enable query optimization
    config = SQLConfig(
        enable_optimization=True,
        enable_sqlglot_optimizer=True,
        log_level=LogLevel.DEBUG  # To see optimization details
    )

    # Apply filters early to reduce data processing
    # GOOD: Filter before expensive operations
    optimized_query = sql("""
        SELECT d.name, AVG(e.salary) as avg_salary
        FROM employees e
        JOIN departments d ON e.dept_id = d.id
        WHERE e.salary > 50000  -- Filter early
        GROUP BY d.name
    """)

    # BAD: Filter after expensive operations
    # This processes all data before filtering
    inefficient_query = sql("""
        SELECT dept_name, avg_salary
        FROM (
            SELECT d.name as dept_name, AVG(e.salary) as avg_salary
            FROM employees e
            JOIN departments d ON e.dept_id = d.id
            GROUP BY d.name
        ) t
        WHERE avg_salary > 50000  -- Filter late
    """)

Column pruning
--------------

Select only the columns you need:

.. testcode::

    # GOOD: Select specific columns
    result = sql("SELECT name, salary FROM employees WHERE dept_id = 10")
    
    # AVOID: Select all columns when you don't need them
    # result = sql("SELECT * FROM employees WHERE dept_id = 10")

Partitioning and data layout
----------------------------

For large datasets, consider data partitioning:

.. testcode::

    # Create partitioned dataset
    large_dataset = ray.data.from_items([
        {"date": "2024-01-01", "sales": 1000, "region": "west"},
        {"date": "2024-01-01", "sales": 1500, "region": "east"},
        # ... many more records
    ])
    
    # Partition by date for time-series queries
    partitioned = large_dataset.repartition(keys=["date"])
    register_table("sales", partitioned)
    
    # Queries on partitioned data are more efficient
    result = sql("""
        SELECT region, SUM(sales) as total_sales
        FROM sales
        WHERE date = '2024-01-01'  -- Efficient partition pruning
        GROUP BY region
    """)

Configuration and tuning
=========================

Engine configuration
--------------------

Ray Data SQL provides extensive configuration options for different environments and use cases:

.. testcode::

    from ray.data.sql import SQLConfig, LogLevel
    from ray.data import DataContext

    # Development configuration - verbose logging and strict checking
    dev_config = SQLConfig(
        # Logging and debugging
        log_level=LogLevel.DEBUG,
        enable_query_timing=True,
        enable_execution_stats=True,
        
        # Query behavior
        case_sensitive=True,            # Strict column name matching
        strict_mode=True,              # Strict SQL compliance
        enable_optimization=True,
        enable_sqlglot_optimizer=True,
        
        # Development safety
        max_join_partitions=50,        # Prevent expensive operations
        enable_auto_registration=True, # Convenient for experimentation
        warn_on_large_results=True     # Warn about large result sets
    )

    # Production configuration - optimized for performance and reliability
    production_config = SQLConfig(
        # Performance optimizations
        log_level=LogLevel.WARNING,    # Reduce logging overhead
        enable_optimization=True,
        enable_sqlglot_optimizer=True,
        enable_predicate_pushdown=True,
        enable_column_pruning=True,
        
        # Resource management
        max_join_partitions=200,       # Higher limits for production
        max_memory_usage_gb=32,        # Memory limit for operations
        enable_streaming_execution=True, # Handle large datasets
        
        # Behavior settings
        case_sensitive=False,          # More forgiving for user queries
        strict_mode=False,            # Allow type coercion
        enable_auto_registration=False, # Security: explicit registration only
        
        # Error handling
        continue_on_error=False,       # Fail fast in production
        max_retry_attempts=3          # Retry transient failures
    )

    # Apply configuration for a session
    with DataContext() as ctx:
        ctx.sql_config = production_config
        result = sql("SELECT * FROM employees")

SQL dialect handling
--------------------

Ray Data SQL uses SQLGlot for parsing and supports multiple SQL dialects:

.. testcode::

    # Configure dialect handling
    dialect_config = SQLConfig(
        # Input dialect parsing
        sqlglot_read_dialect="duckdb",     # Default: DuckDB dialect
        # Alternative options: "mysql", "postgres", "sqlite", "bigquery", "snowflake"
        
        # Output dialect for optimization
        sqlglot_write_dialect="duckdb",    # Keep as DuckDB for execution
        
        # Compatibility settings
        enable_dialect_conversion=True,    # Auto-convert between dialects
        strict_ansi_compliance=False,      # Allow dialect-specific features
        
        # MySQL compatibility
        enable_mysql_compatibility=False,  # MySQL-specific functions
        mysql_mode="ANSI",                 # MySQL SQL mode
        
        # PostgreSQL compatibility  
        enable_postgres_compatibility=False, # PostgreSQL-specific features
        postgres_array_syntax=True,       # Support PostgreSQL arrays
        
        # BigQuery compatibility
        enable_bigquery_compatibility=False, # BigQuery-specific SQL
        bigquery_legacy_sql=False         # Use standard SQL, not legacy
    )

**Example: Converting from PostgreSQL to DuckDB dialect**

.. testcode::

    # PostgreSQL-style query with specific syntax
    postgres_query = """
        SELECT employee_id,
               STRING_AGG(skill, ', ' ORDER BY skill) as skills
        FROM employee_skills
        GROUP BY employee_id
    """
    
    # Configure for PostgreSQL input, DuckDB execution
    config = SQLConfig(
        sqlglot_read_dialect="postgres",
        sqlglot_write_dialect="duckdb",
        enable_dialect_conversion=True
    )
    
    with DataContext() as ctx:
        ctx.sql_config = config
        # Query is automatically converted to DuckDB-compatible syntax
        result = sql(postgres_query)

Advanced Configuration Options
------------------------------

**Memory and Resource Management**

.. testcode::

    memory_config = SQLConfig(
        # Memory limits
        max_memory_usage_gb=16,           # Maximum memory per operation
        enable_memory_monitoring=True,    # Track memory usage
        memory_pressure_threshold=0.8,    # Threshold for memory warnings
        
        # Streaming and batching
        enable_streaming_execution=True,  # Process data in streams
        default_batch_size=10000,         # Default batch size for operations
        adaptive_batch_sizing=True,       # Adjust batch size dynamically
        
        # Spill-to-disk settings
        enable_disk_spill=True,          # Spill to disk when memory is full
        spill_directory="/tmp/ray_sql",   # Directory for spill files
        max_spill_size_gb=100            # Maximum disk usage for spill
    )

**Query Optimization and Execution**

.. testcode::

    optimization_config = SQLConfig(
        # Query optimization
        enable_optimization=True,
        enable_sqlglot_optimizer=True,
        enable_cost_based_optimization=True, # Cost-based query planning
        
        # Pushdown optimizations
        enable_predicate_pushdown=True,     # Push filters to data sources
        enable_projection_pushdown=True,    # Push column selection down
        enable_limit_pushdown=True,         # Push LIMIT to data sources
        
        # Join optimization
        enable_join_reordering=True,        # Reorder joins for efficiency
        prefer_broadcast_joins=True,        # Use broadcast for small tables
        broadcast_join_threshold_mb=100,    # Size threshold for broadcast
        
        # Aggregate optimization
        enable_partial_aggregation=True,    # Pre-aggregate before shuffle
        aggregation_batch_size=50000       # Batch size for aggregations
    )

**Security and Access Control**

.. testcode::

    security_config = SQLConfig(
        # Table access control
        enable_auto_registration=False,     # Require explicit registration
        allow_dynamic_tables=False,         # Prevent dynamic table creation
        restricted_table_patterns=[],       # Patterns for restricted tables
        
        # Query restrictions
        max_query_complexity=1000,          # Limit query complexity
        allowed_functions=["COUNT", "SUM", "AVG"], # Whitelist functions
        blocked_keywords=["DROP", "DELETE"], # Block dangerous keywords
        
        # Resource limits
        max_execution_time_seconds=300,     # Query timeout
        max_result_rows=1000000,           # Limit result size
        enable_query_logging=True          # Log all queries for audit
    )

Memory Management
-----------------

Handle large datasets efficiently:

.. testcode::

    # For very large datasets, use streaming processing
    large_result = sql("""
        SELECT user_id, COUNT(*) as action_count
        FROM user_actions
        GROUP BY user_id
    """)
    
    # Process in batches to manage memory
    for batch in large_result.iter_batches(batch_size=1000):
        # Process each batch
        print(f"Processing batch with {len(batch)} rows")

API Limitations and Workarounds
=================================

Understanding Current Limitations
---------------------------------

**SQL Feature Limitations**

.. testcode::

    # ❌ NOT SUPPORTED: Window functions (limited support)
    try:
        result = sql("""
            SELECT name, salary,
                   ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) as rank
            FROM employees
        """)
    except Exception as e:
        print(f"Window function error: {e}")
        
        # ✅ WORKAROUND: Use Ray Data operations
        employees_ds = sql("SELECT * FROM employees")
        ranked = employees_ds.groupby("dept_id").map_groups(
            lambda group: group.sort("salary", ascending=False)
                              .with_column("rank", range(1, len(group) + 1))
        )

    # ❌ NOT SUPPORTED: User-defined functions
    try:
        result = sql("SELECT custom_function(name) FROM employees")
    except Exception:
        # ✅ WORKAROUND: Use Ray Data map operations
        result = sql("SELECT name FROM employees")
        transformed = result.map(lambda row: {"custom_result": custom_function(row["name"])})

    # ❌ NOT SUPPORTED: Recursive CTEs
    try:
        result = sql("""
            WITH RECURSIVE employee_hierarchy AS (
                SELECT id, name, manager_id, 0 as level FROM employees WHERE manager_id IS NULL
                UNION ALL
                SELECT e.id, e.name, e.manager_id, eh.level + 1
                FROM employees e JOIN employee_hierarchy eh ON e.manager_id = eh.id
            )
            SELECT * FROM employee_hierarchy
        """)
    except Exception:
        # ✅ WORKAROUND: Implement recursion with Ray Data
        print("Use iterative processing with Ray Data operations")

**Performance Limitations and Solutions**

.. testcode::

    # ❌ ISSUE: Large cross-joins are expensive
    # This can cause memory issues and poor performance
    expensive_query = sql("""
        SELECT a.id, b.id
        FROM large_table_a a
        CROSS JOIN large_table_b b
    """)
    
    # ✅ SOLUTION: Add filters to reduce cardinality
    optimized_query = sql("""
        SELECT a.id, b.id
        FROM large_table_a a
        CROSS JOIN large_table_b b
        WHERE a.category = 'active' AND b.status = 'valid'
    """)
    
    # ❌ ISSUE: Complex subqueries in SELECT clauses
    slow_query = sql("""
        SELECT name,
               (SELECT AVG(salary) FROM employees e2 WHERE e2.dept_id = e1.dept_id) as dept_avg
        FROM employees e1
    """)
    
    # ✅ SOLUTION: Use JOINs with aggregation
    fast_query = sql("""
        SELECT e.name, da.dept_avg
        FROM employees e
        JOIN (
            SELECT dept_id, AVG(salary) as dept_avg
            FROM employees
            GROUP BY dept_id
        ) da ON e.dept_id = da.dept_id
    """)

Data Type Limitations
---------------------

**Supported vs. Unsupported Data Types**

.. testcode::

    # ✅ WELL SUPPORTED: Basic types
    supported_data = ray.data.from_items([
        {
            "int_col": 42,
            "float_col": 3.14,
            "string_col": "hello",
            "bool_col": True,
            "date_col": "2024-01-01"  # String dates work well
        }
    ])
    
    # ⚠️ LIMITED SUPPORT: Complex nested types
    nested_data = ray.data.from_items([
        {
            "id": 1,
            "nested_dict": {"key": "value", "num": 123},
            "array_col": [1, 2, 3, 4],
            "struct_col": {"a": 1, "b": "text"}
        }
    ])
    
    register_table("nested_data", nested_data)
    
    # ✅ WORKS: Simple field access
    result = sql("SELECT id, nested_dict FROM nested_data")
    
    # ❌ LIMITED: Complex nested operations
    try:
        result = sql("SELECT nested_dict.key FROM nested_data")
    except Exception:
        # ✅ WORKAROUND: Use Ray Data for complex nested access
        result = nested_data.map(lambda row: {"key": row["nested_dict"]["key"]})

Dialect Compatibility Matrix
----------------------------

**Supported SQL Dialects and Features**

.. list-table:: SQL Dialect Support
   :header-rows: 1
   :widths: 20 15 15 15 15 20

   * - Feature
     - DuckDB
     - PostgreSQL  
     - MySQL
     - BigQuery
     - Notes
   * - Basic SELECT/WHERE
     - ✅ Full
     - ✅ Full
     - ✅ Full
     - ✅ Full
     - Core features
   * - JOINs
     - ✅ Full
     - ✅ Full
     - ✅ Full
     - ✅ Partial
     - Some BigQuery syntax differs
   * - Window Functions
     - ⚠️ Limited
     - ⚠️ Limited
     - ⚠️ Limited
     - ⚠️ Limited
     - Basic support only
   * - CTEs
     - ✅ Full
     - ✅ Full
     - ✅ Partial
     - ✅ Full
     - MySQL: Version dependent
   * - Array Operations
     - ⚠️ Limited
     - ✅ Good
     - ❌ Minimal
     - ✅ Good
     - Use Ray Data for complex arrays
   * - JSON Functions
     - ⚠️ Limited
     - ✅ Good
     - ✅ Good
     - ✅ Good
     - Basic JSON support
   * - String Functions
     - ✅ Good
     - ✅ Good
     - ✅ Good
     - ✅ Good
     - Most functions supported

**Dialect-Specific Examples**

.. testcode::

    # PostgreSQL-style array operations
    postgres_config = SQLConfig(
        sqlglot_read_dialect="postgres",
        enable_postgres_compatibility=True
    )
    
    # MySQL-style string functions
    mysql_config = SQLConfig(
        sqlglot_read_dialect="mysql",
        enable_mysql_compatibility=True
    )
    
    # BigQuery-style analytics functions
    bigquery_config = SQLConfig(
        sqlglot_read_dialect="bigquery",
        enable_bigquery_compatibility=True
    )

Error Handling and Debugging
============================

Common Error Patterns and Solutions
-----------------------------------

**SQL Syntax Errors**

.. testcode::

    try:
        # Common syntax error: missing quotes
        result = sql("SELECT name FROM employees WHERE dept = Engineering")
    except Exception as e:
        print(f"Syntax error: {e}")
        # Fix: Add quotes around string literal
        result = sql("SELECT name FROM employees WHERE dept = 'Engineering'")

    try:
        # Common error: column name typos
        result = sql("SELECT employe_name FROM employees")  # Typo in column name
    except ValueError as e:
        print(f"Column error: {e}")
        # Fix: Check available columns
        from ray.data.sql import get_schema
        schema = get_schema("employees")
        print(f"Available columns: {schema.column_names}")
        result = sql("SELECT employee_name FROM employees")

**Table and Registration Errors**

.. testcode::

    try:
        result = sql("SELECT * FROM nonexistent_table")
    except ValueError as e:
        print(f"Table error: {e}")
        # Check what tables are available
        from ray.data.sql import list_tables
        print(f"Available tables: {list_tables()}")

**Memory and Performance Errors**

.. testcode::

    try:
        # Query that might run out of memory
        large_result = sql("""
            SELECT a.*, b.*
            FROM large_table_a a
            CROSS JOIN large_table_b b
        """)
    except MemoryError as e:
        print(f"Memory error: {e}")
        # Use streaming or add filters
        config = SQLConfig(enable_streaming_execution=True)
        with DataContext() as ctx:
            ctx.sql_config = config
            result = sql("""
                SELECT a.id, b.id
                FROM large_table_a a
                JOIN large_table_b b ON a.key = b.key
                WHERE a.active = true
            """)

Advanced Debugging Techniques
-----------------------------

**Query Execution Analysis**

.. testcode::

    from ray.data.sql import SQLConfig, LogLevel
    import time

    # Comprehensive debugging configuration
    debug_config = SQLConfig(
        log_level=LogLevel.DEBUG,
        enable_query_timing=True,
        enable_execution_stats=True,
        enable_memory_monitoring=True
    )

    def debug_sql_query(query, description=""):
        """Execute SQL with comprehensive debugging."""
        print(f"\n{'='*60}")
        print(f"Debugging Query: {description}")
        print(f"{'='*60}")
        
        start_time = time.time()
        
        try:
            with DataContext() as ctx:
                ctx.sql_config = debug_config
                
                # Execute query
                result = sql(query)
                
                # Get execution stats
                execution_time = time.time() - start_time
                row_count = result.count()
                
                print(f"✅ Query succeeded:")
                print(f"   - Execution time: {execution_time:.3f}s")
                print(f"   - Rows returned: {row_count}")
                print(f"   - Memory usage: {result.size_bytes() / 1024 / 1024:.1f} MB")
                
                return result
                
        except Exception as e:
            execution_time = time.time() - start_time
            print(f"❌ Query failed after {execution_time:.3f}s:")
            print(f"   - Error: {str(e)}")
            print(f"   - Error type: {type(e).__name__}")
            raise

    # Example usage
    debug_sql_query(
        "SELECT dept_id, AVG(salary) FROM employees GROUP BY dept_id",
        "Department salary analysis"
    )

Integration Patterns
====================

Mixing SQL with Ray Data Operations
-----------------------------------

Combine SQL queries with Ray Data transformations:

.. testcode::

    # SQL -> Ray Data -> SQL pipeline
    
    # 1. Start with SQL aggregation
    dept_summary = sql("""
        SELECT dept_id, 
               AVG(salary) as avg_salary,
               COUNT(*) as employee_count
        FROM employees
        GROUP BY dept_id
    """)
    
    # 2. Apply complex transformations with Ray Data
    enriched = dept_summary.map(lambda row: {
        **row,
        "budget_efficiency": row["avg_salary"] / 1000,  # Custom calculation
        "size_category": "large" if row["employee_count"] > 2 else "small"
    })
    
    # 3. Register transformed data and continue with SQL
    register_table("dept_analysis", enriched)
    
    final_report = sql("""
        SELECT size_category,
               COUNT(*) as dept_count,
               AVG(budget_efficiency) as avg_efficiency
        FROM dept_analysis
        GROUP BY size_category
    """)

Batch Processing Workflows
--------------------------

Handle large-scale data processing:

.. testcode::

    # Process daily batch files
    def process_daily_data(date_str):
        # Load data for the date
        daily_data = ray.data.read_parquet(f"s3://bucket/data/{date_str}/")
        register_table("daily_events", daily_data)
        
        # SQL aggregation
        summary = sql(f"""
            SELECT event_type,
                   COUNT(*) as event_count,
                   AVG(value) as avg_value,
                   '{date_str}' as date
            FROM daily_events
            GROUP BY event_type
        """)
        
        # Save results
        summary.write_parquet(f"s3://bucket/summaries/{date_str}/")
        
        return summary

    # Process multiple days
    dates = ["2024-01-01", "2024-01-02", "2024-01-03"]
    summaries = [process_daily_data(date) for date in dates]

Best Practices
==============

SQL Style Guidelines
--------------------

Follow these conventions for maintainable SQL:

.. code-block:: sql

    -- Use meaningful aliases
    SELECT e.name as employee_name,
           d.name as department_name,
           e.salary
    FROM employees e
    JOIN departments d ON e.dept_id = d.id
    WHERE e.salary > 50000
    ORDER BY e.salary DESC;

    -- Format complex queries clearly
    WITH high_earners AS (
        SELECT dept_id, 
               AVG(salary) as avg_salary
        FROM employees
        WHERE salary > 60000
        GROUP BY dept_id
    )
    SELECT d.name,
           he.avg_salary,
           d.budget
    FROM high_earners he
    JOIN departments d ON he.dept_id = d.id;

Resource Management
-------------------

.. testcode::

    # Clean up tables when done
    from ray.data.sql import clear_tables, list_tables

    # Check current tables
    print(f"Active tables: {list_tables()}")
    
    # Clean up specific tables
    # (Note: Individual table cleanup not in current API - would be a good addition)
    
    # Clean up all tables
    clear_tables()

Production Considerations
========================

Monitoring and Observability
----------------------------

Monitor SQL query performance in production:

.. testcode::

    import time
    from ray.data.sql import SQLConfig, LogLevel

    # Enable detailed logging for production monitoring
    config = SQLConfig(
        log_level=LogLevel.INFO,
        enable_optimization=True
    )

    def monitored_sql(query):
        start_time = time.time()
        try:
            result = sql(query)
            execution_time = time.time() - start_time
            row_count = result.count()
            
            print(f"Query executed successfully:")
            print(f"  - Execution time: {execution_time:.3f}s")
            print(f"  - Rows returned: {row_count}")
            
            return result
        except Exception as e:
            execution_time = time.time() - start_time
            print(f"Query failed after {execution_time:.3f}s: {e}")
            raise

Testing SQL Queries
-------------------

Test your SQL queries systematically:

.. testcode::

    def test_employee_aggregations():
        # Setup test data
        test_employees = ray.data.from_items([
            {"id": 1, "dept_id": 10, "salary": 50000},
            {"id": 2, "dept_id": 10, "salary": 60000},
            {"id": 3, "dept_id": 20, "salary": 70000}
        ])
        
        register_table("test_employees", test_employees)
        
        # Test query
        result = sql("""
            SELECT dept_id, AVG(salary) as avg_salary
            FROM test_employees
            GROUP BY dept_id
        """)
        
        rows = result.take_all()
        
        # Assertions
        assert len(rows) == 2
        dept_10_avg = next(r["avg_salary"] for r in rows if r["dept_id"] == 10)
        assert dept_10_avg == 55000
        
        clear_tables()
        print("Test passed!")

    test_employee_aggregations()

What's Next?
============

- **API Reference**: Explore the complete :ref:`SQL API Reference <data_sql_api>` for detailed documentation.

- **Ray Data Features**: Learn about other Ray Data capabilities in the main :ref:`Ray Data documentation <data>`.

- **Performance Tuning**: Check out Ray Data's :ref:`performance tips <performance-tips>` for general optimization strategies.

- **Examples**: Find more complex SQL examples in the :ref:`Ray Data Examples <examples>` section. 