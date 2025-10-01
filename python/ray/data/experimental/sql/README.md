# Ray Data SQL API

**Status:** Experimental (Alpha)

Execute SQL queries on Ray Datasets using standard SQL syntax with advanced query optimization.

## Features

### Multi-Engine Architecture

The SQL API supports two pluggable optimizer engines:

**1. SQLGlot (Default Fallback)**
- Supports 20+ SQL dialects (MySQL, PostgreSQL, Spark SQL, BigQuery, etc.)
- Fast pure-Python implementation
- Rule-based query optimization
- Always available (required dependency)

**2. Apache DataFusion (Default)**
- Advanced cost-based query optimizer (CBO)
- Intelligent join reordering based on table statistics
- Predicate and projection pushdown optimization
- Proportional sampling maintains table size ratios for accurate optimization
- Executes with Ray Data for distributed processing

### Hybrid Execution Model

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

## Quick Start

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

# Execute SQL - automatically uses best available optimizer
result = ray.data.sql("""
    SELECT u.name, o.amount
    FROM users u
    JOIN orders o ON u.id = o.user_id
    WHERE u.age > 25
""")

rows = result.take_all()
```

## Configuration

### Via DataContext (Recommended)

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

### Via SQL API

```python
import ray.data

# Configure SQL settings
ray.data.sql.configure(
    sql_use_datafusion=True,
    dialect="mysql",
    enable_sqlglot_optimizer=True
)

# Check configuration
config = ray.data.sql.get_config_summary()
print(config)
```

## Supported SQL Features

### Basic Operations
- `SELECT` - Column projection
- `FROM` - Table references
- `WHERE` - Row filtering
- `JOIN` (INNER, LEFT, RIGHT, FULL OUTER)
- `GROUP BY` - Aggregation grouping
- `HAVING` - Post-aggregation filtering
- `ORDER BY` - Sorting
- `LIMIT` - Result limiting

### Aggregations
- `COUNT(*)`, `COUNT(column)`
- `SUM(column)`
- `AVG(column)`, `MEAN(column)`
- `MIN(column)`, `MAX(column)`
- `STD(column)`, `STDDEV(column)`

### Functions
- String: `UPPER`, `LOWER`, `LENGTH`, `SUBSTRING`, `TRIM`, `CONCAT`
- Math: `ABS`, `ROUND`, `CEIL`, `FLOOR`, `SQRT`, `LOG`, `EXP`
- Conditional: `CASE WHEN`, `COALESCE`, `NULLIF`

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

## Future Enhancements

- Complete DataFusion plan tree walking (vs string parsing)
- Additional optimizer engines (e.g., Apache Calcite)
- Query result caching
- Materialized view support
- Pushdown to data sources (Parquet, Delta Lake, etc.)

