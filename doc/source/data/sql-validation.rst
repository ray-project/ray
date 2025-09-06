SQL Feature Validation
======================

Ray Data SQL includes comprehensive validation to help identify unsupported features during development, with helpful suggestions for alternatives.

Overview
--------

The validation system provides:

- **Whitelist validation**: Ensures only supported SQL features are used
- **Blacklist detection**: Identifies unsupported features with helpful error messages
- **Alternative suggestions**: Provides guidance on how to achieve similar results with supported features
- **Comprehensive error reporting**: Lists all validation issues in a single error message

The validation happens during SQL parsing, catching issues before query execution.

Getting Supported Features
--------------------------

You can programmatically check what SQL features are supported:

.. testcode::

    from ray.data.sql.utils import get_supported_sql_features
    
    features = get_supported_sql_features()
    
    print("Supported aggregates:", features["aggregates"])
    print("Supported functions:", features["functions"])
    print("Supported operators:", features["operators"])
    print("Supported constructs:", features["constructs"])
    print("Supported data types:", features["data_types"])

Example output::

    Supported aggregates: ['avg', 'count', 'max', 'mean', 'min', 'std', 'stddev', 'sum']
    Supported functions: ['abs', 'acos', 'asin', 'atan', 'cast', 'ceil', 'coalesce', ...]
    Supported operators: ['Arithmetic: +, -, *, /, %', 'Comparison: =, !=, <>, <, <=, >, >=', ...]

Validating SQL Queries
----------------------

Validate SQL queries before execution to catch unsupported features:

**Strict Mode (Default)**

Raises errors for unsupported features:

.. testcode::

    from ray.data.sql.utils import validate_sql_feature_support
    
    try:
        validate_sql_feature_support("SELECT MEDIAN(price) FROM sales")
    except NotImplementedError as e:
        print("Validation failed:", e)

**Non-Strict Mode**

Returns warnings without raising errors:

.. testcode::

    warnings = validate_sql_feature_support(
        "SELECT MEDIAN(price) FROM sales", 
        strict_mode=False
    )
    
    if warnings:
        for warning in warnings:
            print("Warning:", warning)

**Multiple Issues**

The validator catches all issues in a single pass:

.. testcode::

    complex_sql = """
    WITH cte AS (SELECT DISTINCT category FROM sales)
    SELECT MEDIAN(price), LAG(price) OVER (ORDER BY date)
    FROM sales WHERE id IN (1,2,3)
    """
    
    warnings = validate_sql_feature_support(complex_sql, strict_mode=False)
    for i, warning in enumerate(warnings, 1):
        print(f"{i}. {warning}")

Example output::

    1. Common Table Expressions (WITH) not supported. Try: Break into multiple steps using intermediate tables
    2. DISTINCT not supported. Try: Use GROUP BY or unique() operations on Dataset  
    3. Aggregate function 'MEDIAN' not supported. Try: Use approximation with percentile or sorting operations
    4. Window functions not supported. Try: Use GROUP BY or manual partitioning operations
    5. IN operator not supported. Try: Use multiple OR conditions or JOIN operations

Getting Feature Suggestions
---------------------------

Get suggested alternatives for specific unsupported features:

.. testcode::

    from ray.data.sql.utils import get_feature_suggestion
    
    # Aggregate functions
    suggestion = get_feature_suggestion("median")
    print("MEDIAN alternative:", suggestion)
    # Output: "Use approximation with percentile or sorting operations"
    
    # Window functions  
    suggestion = get_feature_suggestion("lag")
    print("LAG alternative:", suggestion)
    # Output: "Use self-joins with ordering operations"
    
    # SQL constructs
    suggestion = get_feature_suggestion("union")
    print("UNION alternative:", suggestion)
    # Output: "Use Dataset.union() method instead"

Supported Features Reference
---------------------------

**Aggregate Functions**
  ``SUM``, ``MIN``, ``MAX``, ``COUNT``, ``AVG``, ``MEAN``, ``STD``, ``STDDEV``

**String Functions**
  ``UPPER``, ``LOWER``, ``LENGTH``, ``SUBSTR``, ``SUBSTRING``, ``TRIM``, ``LTRIM``, ``RTRIM``, ``CONCAT``, ``REPLACE``, ``LIKE``

**Mathematical Functions**
  ``ABS``, ``CEIL``, ``FLOOR``, ``ROUND``, ``SQRT``, ``POW``, ``POWER``, ``EXP``, ``LOG``, ``LN``, ``SIN``, ``COS``, ``TAN``, ``ASIN``, ``ACOS``, ``ATAN``

**Date/Time Functions**
  ``DATE``, ``YEAR``, ``MONTH``, ``DAY``, ``HOUR``, ``MINUTE``, ``SECOND``

**Type Conversion**
  ``CAST``, ``CONVERT``

**Conditional Functions**
  ``COALESCE``, ``NULLIF``, ``GREATEST``, ``LEAST``, ``CASE``, ``WHEN``, ``IF``

**Operators**
  - Arithmetic: ``+``, ``-``, ``*``, ``/``, ``%``
  - Comparison: ``=``, ``!=``, ``<>``, ``<``, ``<=``, ``>``, ``>=``
  - Logical: ``AND``, ``OR``, ``NOT``
  - Pattern: ``LIKE``, ``ILIKE``
  - Null: ``IS NULL``, ``IS NOT NULL``
  - Range: ``BETWEEN``

.. vale off

**SQL Constructs**
  - ``SELECT``, ``FROM``, ``WHERE``, ``GROUP BY``, ``HAVING``, ``ORDER BY``, ``LIMIT``
  - ``JOIN`` (``INNER``, ``LEFT``, ``RIGHT``, ``FULL OUTER``)
  - ``CASE`` expressions
  - Subqueries (limited)

.. vale on
  - Column aliases

**Data Types**
  - Numeric: ``INT``, ``BIGINT``, ``FLOAT``, ``DOUBLE``, ``DECIMAL``
  - String: ``STRING``, ``VARCHAR``, ``CHAR``, ``TEXT``
  - Boolean: ``BOOLEAN``, ``BOOL``
  - Date/Time: ``DATE``, ``TIME``, ``TIMESTAMP``, ``DATETIME``
  - Binary: ``BINARY``, ``VARBINARY``

Unsupported Features with Alternatives
--------------------------------------

**Aggregate Functions**

- **MEDIAN** → Use approximation with percentile or sorting operations
- **MODE** → Use GROUP BY with COUNT to find most frequent values  
- **VARIANCE/VAR** → Use STD() for standard deviation instead
- **PERCENTILE** → Use approximation with sorting operations
- **ARRAY_AGG** → Use Dataset groupby with collect operations
- **STRING_AGG** → Use map operations to concatenate strings

**Window Functions**

- **LAG/LEAD** → Use self-joins with ordering operations
- **RANK/DENSE_RANK** → Use ROW_NUMBER() with ordering  
- **NTILE** → Use manual bucketing with modulo operations

.. vale off

**SQL Constructs**

- **Common Table Expressions (WITH)** → Break into multiple steps using intermediate tables
- **UNION** → Use Dataset.union() method instead
- **INTERSECT** → Use JOIN operations to find common records
- **EXCEPT** → Use LEFT JOIN with WHERE IS NULL to exclude records
- **DISTINCT** → Use GROUP BY or unique() operations on Dataset

**Operators**

- **IN** → Use multiple OR conditions or JOIN operations
- **EXISTS** → Use JOIN operations instead

.. vale on
- **ALL/ANY** → Use aggregation with MIN/MAX functions

Integration with Development Workflow
-------------------------------------

**During Development**

Use validation to catch issues early:

.. testcode::

    # Check query before building complex logic
    sql = "SELECT customer_id, MEDIAN(order_amount) FROM orders GROUP BY customer_id"
    
    warnings = validate_sql_feature_support(sql, strict_mode=False)
    if warnings:
        print("Issues found:")
        for warning in warnings:
            print(f"  - {warning}")
        
        # Get suggestion for MEDIAN
        suggestion = get_feature_suggestion("median")
        print(f"Alternative: {suggestion}")

**In Testing**

Validate SQL queries in test suites:

.. testcode::

    def test_sql_query_compatibility():
        """Ensure all SQL queries use supported features."""
        test_queries = [
            "SELECT * FROM sales WHERE price > 100",
            "SELECT SUM(amount) FROM transactions GROUP BY category",
            # Add your queries here
        ]
        
        for sql in test_queries:
            try:
                validate_sql_feature_support(sql)
                print(f"✓ Query valid: {sql[:50]}...")
            except NotImplementedError as e:
                print(f"✗ Query invalid: {sql[:50]}...")
                print(f"  Error: {e}")

**Error Handling**

Handle validation errors gracefully in applications:

.. testcode::

    def safe_sql_execution(sql_query):
        """Execute SQL with validation and helpful error messages."""
        try:
            validate_sql_feature_support(sql_query)
            # Execute the query
            return sql(sql_query)
        except NotImplementedError as e:
            print(f"SQL not supported: {e}")
            # Suggest checking the documentation
            print("See Ray Data SQL documentation for supported features.")
            return None 