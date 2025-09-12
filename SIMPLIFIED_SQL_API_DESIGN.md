# Ray Data SQL API Simplification - Design Document

## 🎯 **Goal: Create the Simplest, Most Pythonic SQL API**

Based on analysis of DuckDB, PySpark, Dask, and other successful Python data frameworks, redesign Ray Data SQL API to be intuitive, simple, and context-integrated.

## 📊 **Current API Analysis**

### ❌ **Current Complexity Issues**
```python
# Current Ray Data SQL (TOO COMPLEX):
import ray.data.sql

# 1. Manual table registration required
ray.data.sql.register_table("users", users_dataset)
ray.data.sql.register_table("orders", orders_dataset)

# 2. Many configuration functions
ray.data.sql.configure(dialect="postgres", log_level="debug")
ray.data.sql.enable_optimization(True)
ray.data.sql.enable_predicate_pushdown(True)
ray.data.sql.set_join_partitions(50)
ray.data.sql.set_query_timeout(300)

# 3. Complex engine management
engine = ray.data.sql.RaySQL(config)
result = engine.sql("SELECT * FROM users")

# 4. Many exports (24+ functions in __all__)
```

**Problems**:
- Too many configuration functions (15+)
- Manual table registration required
- Complex engine creation
- Not integrated with Ray Data Context
- Verbose and not Pythonic

## 🌟 **Best Practices from Other Frameworks**

### **DuckDB Pattern** ✅ **EXCELLENT SIMPLICITY**
```python
import duckdb

# Direct SQL execution - no setup required
result = duckdb.sql("SELECT * FROM 'data.csv'")

# Automatic DataFrame integration
df = pd.DataFrame({"a": [1, 2, 3]})
result = duckdb.sql("SELECT * FROM df")  # df automatically available!

# Simple configuration via connection
con = duckdb.connect()
con.execute("SET memory_limit='1GB'")
result = con.sql("SELECT * FROM df")
```

### **PySpark Pattern** ✅ **CONTEXT INTEGRATION**
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MyApp").getOrCreate()

# DataFrames automatically available as SQL tables
df = spark.read.parquet("data.parquet")
df.createOrReplaceTempView("users")

# Simple SQL execution through context
result = spark.sql("SELECT * FROM users")

# Configuration through context
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

### **Dask Pattern** ✅ **PYTHONIC SIMPLICITY**
```python
import dask.dataframe as dd

# Simple query method on DataFrames
df = dd.read_parquet("data.parquet")
result = df.query("age > 25")  # Pandas-style

# SQL-like operations
result = df.groupby("city").agg({"age": "mean"})
```

## 🎨 **Proposed Simplified Ray Data SQL API**

### **Design Principles**
1. **Automatic Integration**: No manual table registration
2. **Context-Based Configuration**: Settings through Ray Data Context
3. **Minimal Public API**: 3-5 core functions maximum
4. **Pythonic Patterns**: Follow Python data framework conventions
5. **Seamless Ray Integration**: Works naturally with Ray Datasets

### **New API Design**

#### **Pattern 1: Direct SQL Function (DuckDB-inspired)**
```python
import ray.data

# Automatic dataset registration - no manual setup!
users = ray.data.read_parquet("users.parquet")
orders = ray.data.read_parquet("orders.parquet")

# Direct SQL execution - datasets automatically available
result = ray.data.sql("""
    SELECT u.name, o.amount 
    FROM users u 
    JOIN orders o ON u.id = o.user_id
""")
```

#### **Pattern 2: Context-Based Configuration (Spark-inspired)**
```python
import ray.data

# Configure SQL through Ray Data context
ray.data.context.sql_config.dialect = "postgres"
ray.data.context.sql_config.case_sensitive = False
ray.data.context.sql_config.optimization_level = "aggressive"

# SQL execution uses context settings
result = ray.data.sql("SELECT * FROM users WHERE age > 25")
```

#### **Pattern 3: Dataset Method (Pandas/Dask-inspired)**
```python
import ray.data

users = ray.data.read_parquet("users.parquet")

# SQL method directly on datasets
result = users.sql("SELECT * FROM this WHERE age > 25")

# Or register and query
users.register_as("users")
result = ray.data.sql("SELECT * FROM users WHERE age > 25")
```

### **Simplified Public API**

#### **Core Functions (Only 3-4 needed)**
```python
# ray.data.sql module
def sql(query: str, **datasets) -> Dataset:
    """Execute SQL query with automatic dataset registration."""
    
def register(name: str, dataset: Dataset) -> None:
    """Register a dataset for SQL queries (optional)."""
    
def clear() -> None:
    """Clear all registered datasets."""

# Configuration through Ray Data Context
# ray.data.context.sql_config.dialect = "postgres"
# ray.data.context.sql_config.case_sensitive = False
```

#### **Context Integration**
```python
# Add to ray.data.context
class SQLConfig:
    dialect: str = "duckdb"
    case_sensitive: bool = True
    optimization_enabled: bool = True
    query_timeout: Optional[int] = None
    
class DataContext:
    # ... existing context
    sql_config: SQLConfig = SQLConfig()
```

## 🔄 **API Comparison**

### **Before (Complex)**
```python
# 15+ functions, manual setup, verbose
import ray.data.sql

ray.data.sql.configure(dialect="postgres", log_level="debug")
ray.data.sql.enable_optimization(True)
ray.data.sql.enable_predicate_pushdown(True)
ray.data.sql.set_join_partitions(50)
ray.data.sql.register_table("users", users)
ray.data.sql.register_table("orders", orders)

result = ray.data.sql.sql("SELECT * FROM users JOIN orders ON users.id = orders.user_id")
```

### **After (Simple)**
```python
# 3 functions, automatic setup, Pythonic
import ray.data

# Configuration through context (optional)
ray.data.context.sql_config.dialect = "postgres"
ray.data.context.sql_config.optimization_enabled = True

# Automatic dataset registration
users = ray.data.read_parquet("users.parquet")
orders = ray.data.read_parquet("orders.parquet")

# Direct SQL execution
result = ray.data.sql("SELECT * FROM users JOIN orders ON users.id = orders.user_id")
```

## 🚀 **Implementation Plan**

### **Phase 1: Context Integration**
1. Add `SQLConfig` to Ray Data Context
2. Update SQL engine to read from context
3. Maintain backward compatibility

### **Phase 2: Automatic Registration**
1. Implement automatic dataset discovery
2. Use variable names for table names
3. Support explicit registration as fallback

### **Phase 3: API Simplification**
1. Reduce public API to 3-4 core functions
2. Move advanced features to context configuration
3. Update all documentation and examples

### **Phase 4: Enhanced Usability**
1. Add dataset.sql() method for Pandas-style usage
2. Implement smart error messages
3. Add query result caching

## 📋 **Proposed New API Structure**

### **Minimal Public API**
```python
# ray.data.sql module (simplified)
def sql(query: str, **kwargs) -> Dataset:
    """Execute SQL query on Ray Datasets."""

def register(name: str, dataset: Dataset) -> None:
    """Register dataset with custom name."""
    
def clear() -> None:
    """Clear registered datasets."""

# Context-based configuration
# ray.data.context.sql_config.*
```

### **Context Configuration**
```python
# ray.data.context.sql_config
class SQLConfig:
    dialect: str = "duckdb"           # SQL dialect
    case_sensitive: bool = True       # Case sensitivity
    optimization_enabled: bool = True # Query optimization
    query_timeout: int = None         # Timeout in seconds
    max_join_partitions: int = 20     # Join parallelism
    
    # Advanced settings (hidden from basic users)
    _enable_predicate_pushdown: bool = True
    _enable_projection_pushdown: bool = True
    _strict_mode: bool = False
```

### **Usage Examples**
```python
# Example 1: Simplest possible usage
import ray.data

users = ray.data.read_parquet("users.parquet")
result = ray.data.sql("SELECT * FROM users WHERE age > 25")

# Example 2: Multi-table queries
users = ray.data.read_parquet("users.parquet") 
orders = ray.data.read_parquet("orders.parquet")
result = ray.data.sql("""
    SELECT u.name, SUM(o.amount) as total
    FROM users u JOIN orders o ON u.id = o.user_id
    GROUP BY u.name
""")

# Example 3: Configuration
ray.data.context.sql_config.dialect = "postgres"
ray.data.context.sql_config.case_sensitive = False
result = ray.data.sql("select * from users")  # lowercase works

# Example 4: Dataset method (Pandas-style)
users = ray.data.read_parquet("users.parquet")
result = users.sql("SELECT * FROM this WHERE age > 25")
```

## 🎯 **Benefits of Simplified API**

1. **✅ Pythonic**: Follows Python data framework conventions
2. **✅ Simple**: 3 core functions vs 15+ current functions
3. **✅ Automatic**: No manual table registration required
4. **✅ Context-Integrated**: Configuration through Ray Data Context
5. **✅ Familiar**: Similar to DuckDB/PySpark patterns users know
6. **✅ Powerful**: Same functionality with much simpler interface
7. **✅ Maintainable**: Much smaller public API surface

This design would make Ray Data SQL as easy to use as DuckDB while maintaining all the power of distributed Ray Dataset operations!
