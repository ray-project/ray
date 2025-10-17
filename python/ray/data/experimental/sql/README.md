# Ray Data SQL API

:::note
This API is experimental and may change in future releases.
:::

## What is the Ray Data SQL API?

The Ray Data SQL API provides SQL query execution on Ray Datasets using standard SQL syntax. You can query distributed datasets with familiar SQL while leveraging Ray Data's distributed execution, resource management, and backpressure control.

The API uses a pluggable optimizer architecture that supports multiple query engines:
- SQLGlot for multi-dialect SQL parsing
- Apache DataFusion for advanced cost-based optimization

All queries execute using Ray Dataset native operations (filter, join, groupby, aggregate) with automatic distribution across your cluster.

## When to use the SQL API

Use the SQL API when you:
- Want to query Ray Datasets using familiar SQL syntax
- Need multi-dialect SQL support (MySQL, PostgreSQL, Spark SQL, BigQuery)
- Have complex join queries that benefit from cost-based optimization
- Want to migrate SQL workloads to Ray Data with minimal code changes
- Prefer declarative SQL over imperative Ray Data operations

## Optimizer engines

The SQL API supports two pluggable optimizer engines:

**SQLGlot engine (fallback):**
- Supports 20+ SQL dialects including MySQL, PostgreSQL, Spark SQL, BigQuery, and Snowflake
- Fast pure-Python implementation
- Rule-based query optimization
- Always available as required dependency

**Apache DataFusion engine (default):**
- Advanced cost-based query optimizer with table statistics
- Intelligent join reordering based on cardinality estimates
- Predicate and projection pushdown optimization
- Proportional sampling that maintains table size ratios for accurate optimization
- Executes with Ray Data for distributed processing

## How the SQL API works

```
User SQL Query
  ↓
[If DataFusion enabled & available]
  DataFusion CBO → Optimization hints → Ray Data execution
[Else]
  SQLGlot → Ray Data execution
  ↓
Ray Dataset Operations (filter, join, groupby, aggregate, sort, limit)
  ↓
Distributed Execution + Resource Management + Backpressure Control
```

## Get started with the SQL API

This example shows basic SQL query execution on Ray Datasets:

```python
import ray.data

# Create datasets
users = ray.data.from_items([
    {"id": 1, "name": "Alice", "age": 30},
    {"id": 2, "name": "Bob", "age": 25}
])

orders = ray.data.from_items([
    {"user_id": 1, "amount": 100},
    {"user_id": 2, "amount": 200}
])

# Execute SQL query
result = ray.data.sql("""
    SELECT u.name, o.amount
    FROM users u
    JOIN orders o ON u.id = o.user_id
    WHERE u.age > 25
""")

rows = result.take_all()
# Returns: [{'name': 'Alice', 'amount': 100}]
```

The SQL API automatically selects the best available optimizer engine based on your configuration.

## Configure the SQL API

You can configure SQL behavior through Ray Data's DataContext:

```python
from ray.data import DataContext

ctx = DataContext.get_current()

# Optimizer selection
ctx.sql_use_datafusion = True  # Use DataFusion CBO (default)
ctx.sql_use_datafusion = False # Use SQLGlot only

# SQL dialect for parsing
ctx.sql_dialect = "mysql"      # MySQL syntax
ctx.sql_dialect = "postgres"   # PostgreSQL syntax
ctx.sql_dialect = "spark"      # Spark SQL syntax
ctx.sql_dialect = "bigquery"   # BigQuery syntax

# SQLGlot optimization
ctx.sql_enable_sqlglot_optimizer = True

# Other settings
ctx.sql_case_sensitive = True
ctx.sql_max_join_partitions = 20
```

You can also use convenience functions:

```python
import ray.data

# Configure SQL settings
ray.data.sql.configure(
    sql_use_datafusion=True,
    dialect="mysql",
    enable_sqlglot_optimizer=True
)

# View current configuration
config = ray.data.sql.get_config_summary()
print(config)
```

## Supported SQL features

The SQL API supports standard SQL operations:

**Query operations:**
- SELECT for column projection
- FROM for table references
- WHERE for row filtering
- JOIN operations including INNER, LEFT, RIGHT, and FULL OUTER
- GROUP BY for aggregation grouping
- HAVING for post-aggregation filtering
- ORDER BY for sorting
- LIMIT for result limiting

**Aggregate functions:**
- COUNT(*) and COUNT(column)
- SUM(column)
- AVG(column) and MEAN(column)
- MIN(column) and MAX(column)
- STD(column) and STDDEV(column)

**Scalar functions:**
- String functions including UPPER, LOWER, LENGTH, SUBSTRING, TRIM, and CONCAT
- Math functions including ABS, ROUND, CEIL, FLOOR, SQRT, LOG, and EXP
- Conditional functions including CASE WHEN, COALESCE, and NULLIF

## Architecture

### Directory Structure

```
sql/
├── engines/ (Pluggable optimizer backends)
│   ├── base.py (OptimizerBackend protocol)
│   ├── factory.py (Engine selector)
│   ├── sqlglot/ (Multi-dialect support)
│   └── datafusion/ (Cost-based optimization)
├── execution/ (Query execution)
│   ├── executor.py (Main executor)
│   ├── handlers.py (Operation handlers)
│   └── analyzers.py (Query analyzers)
├── compiler/ (Expression compilation)
├── registry/ (Table management)
├── schema/ (Schema management)
└── validators/ (Query validation)
```

### Adding New Optimizer Engines

The architecture is designed to be pluggable. To add a new engine:

1. Create `engines/your_engine/` directory
2. Implement `YourEngineBackend(OptimizerBackend)`
3. Return `QueryOptimizations` from `optimize_query()`
4. Done! The core engine automatically works with it

## Performance

### DataFusion Optimization Benefits

- **Join Reordering:** Optimal join order based on table sizes (small → large)
- **Early Filtering:** Filters applied before joins (reduces data volume)
- **Column Pruning:** Only necessary columns read from storage
- **Proportional Sampling:** Maintains table size ratios for accurate CBO

### Ray Data Execution Benefits

- **Distributed Execution:** Scales across cluster nodes
- **Resource Management:** CPU/GPU/memory budgets prevent over-allocation
- **Backpressure Control:** 3 policies prevent OOM errors
- **Streaming Execution:** Block-based, lazy evaluation, no materialization
- **Fault Tolerance:** Automatic task retries

## Examples

### Multi-Dialect Support

```python
# MySQL syntax
ctx.sql_dialect = "mysql"
result = ray.data.sql("SELECT IFNULL(name, 'Unknown') FROM users")

# Spark SQL syntax
ctx.sql_dialect = "spark"
result = ray.data.sql("SELECT name WHERE age <=> NULL FROM users")

# BigQuery syntax
ctx.sql_dialect = "bigquery"
result = ray.data.sql("SELECT ARRAY_AGG(name) FROM users GROUP BY age")
```

### Complex Queries

```python
# Multi-table join with aggregation
result = ray.data.sql("""
    SELECT
        u.name,
        COUNT(*) as order_count,
        SUM(o.amount) as total_amount
    FROM users u
    JOIN orders o ON u.id = o.user_id
    JOIN products p ON o.product_id = p.id
    WHERE u.age > 25 AND o.status = 'completed'
    GROUP BY u.name
    HAVING total_amount > 1000
    ORDER BY total_amount DESC
    LIMIT 10
""")

# DataFusion optimizes join order and predicate placement
# Ray Data executes with distributed backpressure control
```

### Optimizer Selection

```python
# Use DataFusion for complex queries (automatic)
ctx.sql_use_datafusion = True  # Default
result = ray.data.sql(complex_multi_join_query)
# DataFusion CBO optimizes, Ray Data executes

# Use SQLGlot only for simple queries or compatibility
ctx.sql_use_datafusion = False
result = ray.data.sql(simple_query)
# SQLGlot optimizes and executes
```

## API Reference

### Main Functions

- `ray.data.sql(query, **datasets)` - Execute SQL query
- `ray.data.register(name, dataset)` - Register dataset as table
- `ray.data.clear_tables()` - Clear all registered tables
- `ray.data.list_tables()` - List registered tables

### Configuration

- `ray.data.sql.configure(**kwargs)` - Configure SQL settings
- `ray.data.sql.get_config_summary()` - Get current configuration
- `ray.data.sql.reset_config()` - Reset to defaults

## Experimental Warning

This API is experimental and may change in future releases. Use with caution in production environments.

## Implementation Details

### DataFusion Integration

DataFusion provides query optimization through cost-based analysis:

1. **Proportional Sampling:** Estimates all table sizes, calculates global sampling fraction, maintains size ratios
2. **Plan Optimization:** DataFusion applies CBO to generate optimal plan
3. **Extraction:** Parse plan to extract filter placement, join order, projections
4. **Execution:** Apply hints using existing QueryExecutor (no code duplication)

### Streaming Semantics

Both engines preserve Ray Data's streaming execution:
- No `count()` calls (expensive)
- No `take_all()` calls (materialization)
- Uses `limit()` for samples only
- Lazy evaluation throughout
- Block-based parallelism

## Testing

Run SQL API tests:

```bash
pytest python/ray/data/tests/test_sql_api.py -v
pytest python/ray/data/tests/test_sql_datafusion.py -v
```

## DataFusion optimization status

### Currently applied
- Predicate pushdown: Ray Data's FilterHandler applies filters early (validated by DataFusion)
- Projection pushdown: Ray Data's ProjectionAnalyzer selects columns early (validated by DataFusion)
- Proportional sampling: Maintains accurate table size ratios for CBO decisions
- Dialect translation: Automatic translation from any SQL dialect to PostgreSQL

### Future enhancements
- Join reordering: Apply DataFusion's cost-based join order decisions (currently extracted but not enforced)
- Aggregation strategy: Use DataFusion's hash vs sort aggregation hints (currently extracted but not applied)
- Join algorithm selection: Apply DataFusion's join algorithm choices (requires Ray Data API changes)
- Complete plan tree walking: Use DataFusion's plan tree API instead of string parsing

The current implementation provides a solid foundation with DataFusion CBO validation.
Full optimization application requires additional Ray Data API enhancements.
