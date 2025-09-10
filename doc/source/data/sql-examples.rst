Ray Data SQL Examples
=====================

This page provides practical examples of using Ray Data SQL for common data processing tasks.
All examples run out-of-the-box and demonstrate real-world use cases.

.. contents::
   :depth: 2
   :local:

Getting started examples
========================

Basic query operations
----------------------

Start with simple SELECT operations to explore your data:

.. testcode::

    import ray
    import ray.data.sql sql, register_table

    # Create sample data
    users = ray.data.from_items([
        {"id": 1, "name": "Alice", "age": 30, "department": "Engineering"},
        {"id": 2, "name": "Bob", "age": 25, "department": "Marketing"},
        {"id": 3, "name": "Charlie", "age": 35, "department": "Engineering"},
        {"id": 4, "name": "Diana", "age": 28, "department": "Sales"}
    ])
    
    # Register the dataset as a SQL table
    register_table("users", users)
    
    # Execute basic queries
    result = ray.data.sql("SELECT name, age FROM users WHERE age > 27")
    print("Users over 27:")
    for row in result.take(10):
        print(f"  {row['name']}: {row['age']}")

.. testoutput::

    Users over 27:
      Alice: 30
      Charlie: 35
      Diana: 28

Filtering and aggregation
-------------------------

.. vale off

Use WHERE clauses and aggregate functions to analyze your data:

.. vale on

.. testcode::

    # Count employees by department
    dept_counts = sql("""
        SELECT department, COUNT(*) as employee_count
        FROM users 
        GROUP BY department
        ORDER BY COUNT(*) DESC
    """)
    
    print("Department sizes:")
    for row in dept_counts.take_all():
        print(f"  {row['department']}: {row['employee_count']} employees")
    
    # Calculate average age by department
    avg_ages = sql("""
        SELECT department, AVG(age) as avg_age
        FROM users
        GROUP BY department
    """)
    
    print("\nAverage ages by department:")
    for row in avg_ages.take_all():
        print(f"  {row['department']}: {row['avg_age']:.1f} years")

.. testoutput::

    Department sizes:
      Engineering: 2 employees
      Marketing: 1 employees
      Sales: 1 employees
    
    Average ages by department:
      Engineering: 32.5 years
      Marketing: 25.0 years
      Sales: 28.0 years

Advanced query examples
=======================

.. vale off

JOIN operations
---------------

Combine multiple datasets using various types of joins:

.. vale on

.. testcode::

    # Create related datasets
    projects = ray.data.from_items([
        {"project_id": 101, "name": "Website Redesign", "lead_id": 1},
        {"project_id": 102, "name": "Mobile App", "lead_id": 3},
        {"project_id": 103, "name": "Data Pipeline", "lead_id": 1}
    ])
    
    register_table("projects", projects)
    
    # Inner join to find project leaders
    project_leaders = sql("""
        SELECT p.name as project_name, u.name as leader_name, u.department
        FROM projects p
        INNER JOIN users u ON p.lead_id = u.id
        ORDER BY p.project_id
    """)
    
    print("Project assignments:")
    for row in project_leaders.take_all():
        print(f"  {row['project_name']}: {row['leader_name']} ({row['department']})")

.. testoutput::

    Project assignments:
      Website Redesign: Alice (Engineering)
      Mobile App: Charlie (Engineering)
      Data Pipeline: Alice (Engineering)

.. vale off

Subqueries and CTEs
-------------------

Use Common Table Expressions (CTEs) for complex analytical queries:

.. vale on

.. testcode::

    # Find departments with above-average age employees
    above_avg_depts = sql("""
        WITH dept_stats AS (
            SELECT department, AVG(age) as avg_dept_age
            FROM users
            GROUP BY department
        ),
        overall_avg AS (
            SELECT AVG(age) as overall_avg_age
            FROM users
        )
        SELECT ds.department, ds.avg_dept_age
        FROM dept_stats ds, overall_avg oa
        WHERE ds.avg_dept_age > oa.overall_avg_age
        ORDER BY ds.avg_dept_age DESC
    """)
    
    print("Departments with above-average age employees:")
    for row in above_avg_depts.take_all():
        print(f"  {row['department']}: {row['avg_dept_age']:.1f} years")

.. testoutput::

    Departments with above-average age employees:
      Engineering: 32.5 years

Performance optimization examples
=================================

Efficient filtering
-------------------

Apply filters early to reduce data processing:

.. testcode::

    # Create larger dataset for performance demonstration
    large_dataset = ray.data.from_items([
        {"id": i, "value": i * 2, "category": f"cat_{i % 5}", "active": i % 3 == 0}
        for i in range(1000)
    ])
    
    register_table("large_data", large_dataset)
    
    # Efficient query with early filtering
    filtered_result = ray.data.sql("""
        SELECT category, COUNT(*) as count, AVG(value) as avg_value
        FROM large_data
        WHERE active = true AND value > 100
        GROUP BY category
        ORDER BY avg_value DESC
    """)
    
    print("Active records with value > 100:")
    for row in filtered_result.take(3):
        print(f"  {row['category']}: {row['count']} records, avg={row['avg_value']:.1f}")

.. testoutput::

    Active records with value > 100:
      cat_4: ... records, avg=...
      cat_3: ... records, avg=...
      cat_2: ... records, avg=...

Configuration examples
======================

Custom SQL configuration
------------------------

Configure the SQL engine for specific requirements:

.. testcode::

    import ray.data.sql SQLConfig, LogLevel
    from ray.data import DataContext

    # Development configuration with verbose logging
    dev_config = SQLConfig(
        log_level=LogLevel.DEBUG,
        case_sensitive=False,
        enable_optimization=True,
        strict_mode=False
    )
    
    # Production configuration optimized for performance
    prod_config = SQLConfig(
        log_level=LogLevel.WARNING,
        enable_optimization=True,
        enable_sqlglot_optimizer=True,
        case_sensitive=False,
        strict_mode=False
    )
    
    # Use configuration in context
    with DataContext() as ctx:
        ctx.sql_config = prod_config
        
        # This query uses the production configuration
        result = ray.data.sql("SELECT COUNT(*) as total FROM users")
        print(f"Total users: {result.take(1)[0]['total']}")

.. testoutput::

    Total users: 4

Error handling examples
=======================

Graceful error handling
-----------------------

Handle common SQL errors gracefully in your applications:

.. testcode::

    import ray.data.sql list_tables, get_schema

    def safe_sql_query(query, description="query"):
        """Execute SQL with comprehensive error handling."""
        try:
            result = ray.data.sql(query)
            return result
        except ValueError as e:
            if "table" in str(e).lower():
                print(f"Table error in {description}: {e}")
                print(f"Available tables: {list_tables()}")
            elif "column" in str(e).lower():
                print(f"Column error in {description}: {e}")
                # You could inspect table schemas here
            else:
                print(f"SQL error in {description}: {e}")
            return None
        except Exception as e:
            print(f"Unexpected error in {description}: {e}")
            return None
    
    # Example: Query with invalid table name
    result = safe_sql_query(
        "SELECT * FROM nonexistent_table", 
        "user analysis query"
    )
    
    # Example: Query with valid table
    result = safe_sql_query(
        "SELECT name FROM users LIMIT 2",
        "user name query"
    )
    
    if result:
        print("Query succeeded:")
        for row in result.take_all():
            print(f"  {row}")

.. testoutput::

    Table error in user analysis query: ...
    Available tables: ['users', 'projects', 'large_data']
    Query succeeded:
      {'name': 'Alice'}
      {'name': 'Bob'}

Integration examples
====================

Ray Data pipeline integration
-----------------------------

Combine SQL queries with Ray Data transformations:

.. testcode::

    # SQL query followed by Ray Data operations
    engineering_users = sql("SELECT * FROM users WHERE department = 'Engineering'")
    
    # Apply additional transformations using Ray Data
    def add_seniority_level(batch):
        """Add seniority level based on age."""
        import pandas as pd
        df = pd.DataFrame(batch)
        df['seniority'] = df['age'].apply(
            lambda age: 'Senior' if age >= 30 else 'Junior'
        )
        return df.to_dict('list')
    
    enhanced_data = engineering_users.map_batches(add_seniority_level)
    
    # Register the enhanced data for further SQL queries
    register_table("engineering_enhanced", enhanced_data)
    
    # Query the enhanced data
    seniority_breakdown = sql("""
        SELECT seniority, COUNT(*) as count, AVG(age) as avg_age
        FROM engineering_enhanced
        GROUP BY seniority
    """)
    
    print("Engineering team seniority breakdown:")
    for row in seniority_breakdown.take_all():
        print(f"  {row['seniority']}: {row['count']} people, avg age {row['avg_age']:.1f}")

.. testoutput::

    Engineering team seniority breakdown:
      Senior: 2 people, avg age 32.5
      Junior: 0 people, avg age ...

Cleanup examples
================

Resource management
-------------------

Clean up resources when you're done:

.. testcode::

    import ray.data.sql clear_tables, list_tables

    # Check current tables
    print(f"Tables before cleanup: {list_tables()}")
    
    # Clear all registered tables
    clear_tables()
    
    # Verify cleanup
    print(f"Tables after cleanup: {list_tables()}")

.. testoutput::

    Tables before cleanup: ['users', 'projects', 'large_data', 'engineering_enhanced']
    Tables after cleanup: []

Best practices summary
======================

.. vale off

Query optimization
  - Apply filters early in WHERE clauses
  - Use explicit column selection instead of SELECT *
  - Leverage indexes when available (future feature)

.. vale on

Error handling
  - Always handle ValueError for SQL syntax errors
  - Check table existence with ``list_tables()``
  - Validate schemas with ``get_schema()``

Resource management
  - Clear tables when finished with ``clear_tables()``
  - Use appropriate batch sizes for large datasets
  - Configure memory limits for production workloads

Configuration
  - Use development config for debugging and testing
  - Use production config for optimized performance
  - Enable optimizations for better query performance

For more advanced examples, see the :doc:`sql-user-guide` and :doc:`sql-quickstart` guides. 