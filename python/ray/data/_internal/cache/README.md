# Ray Data Petabyte-Scale Caching System

This document describes Ray Data's stage caching system, designed to handle datasets of any size including petabyte-scale workloads.

## Overview

Ray Data's stage cache provides automatic acceleration for repeated Dataset operations by caching execution metadata and ObjectRef handles rather than raw data. This approach allows efficient caching regardless of dataset size.

## Key Design Principles

### Petabyte-Scale Architecture
- **Never cache raw data** - Only metadata and ObjectRef handles
- **Constant memory footprint** - Memory usage independent of dataset size  
- **Leverage Ray's object store** - Use distributed storage as primary data layer
- **Minimal network overhead** - Only lightweight metadata transferred

### Cache Tiers
1. **Plan Cache**: Stores ObjectRef handles and execution metadata for large datasets
2. **Result Cache**: Stores actual data only for small computed results (<1MB)

## Configuration

### Basic Configuration

```python
import ray
from ray.data.context import DataContext

# Enable petabyte-scale caching
ctx = DataContext.get_current()
ctx.enable_stage_cache = True
```

### Available Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `enable_stage_cache` | `False` | Enable/disable stage caching |

## Usage Examples

### Basic Usage

```python
import ray
from ray.data.context import DataContext

# Enable caching
ctx = DataContext.get_current()
ctx.enable_stage_cache = True

# First execution - computes and caches metadata/ObjectRefs
ds1 = ray.data.read_parquet("s3://bucket/large-dataset/")
result1 = ds1.materialize()  # Takes full execution time

# Second identical execution - reuses cached ObjectRefs
ds2 = ray.data.read_parquet("s3://bucket/large-dataset/")
result2 = ds2.materialize()  # Nearly instant due to ObjectRef reuse
```

### Complex Transformations

```python
# Complex pipeline that benefits from caching
def create_pipeline():
    return (ray.data.read_parquet("s3://bucket/data/")
            .map_batches(expensive_transform)
            .filter(lambda x: x["score"] > 0.8)
            .repartition(1000))

# First execution - full computation and caching
ds1 = create_pipeline()
result1 = ds1.materialize()

# Second execution - reuses cached intermediate results
ds2 = create_pipeline()  
result2 = ds2.materialize()  # Much faster
```

### Cache Statistics

```python
from ray.data._internal.cache.stage_cache import get_stage_cache_stats

# Get detailed cache statistics
stats = get_stage_cache_stats()
print(f"Hit rate: {stats['hit_rate']:.1%}")
print(f"Memory usage: {stats['total_memory_bytes']:,} bytes")
print(f"Plan cache entries: {stats['plan_cache_entries']}")

# Cache statistics are also included in Dataset.stats()
ds = ray.data.range(1000).materialize()
print(ds.stats())  # Includes cache statistics at the end
```

## Performance Characteristics

### Memory Usage
- **Plan cache**: ~1KB per cached dataset regardless of size
- **Result cache**: Only for small results (<1MB each)
- **Total footprint**: O(number of operations), not O(data size)

### Cache Operations
- **Lookup**: O(1) hash table lookup + O(1) ObjectRef verification
- **Storage**: O(1) metadata extraction + O(1) ObjectRef storage
- **Fingerprint**: O(1) using plan hash functions

### Scalability
- **Dataset size**: No limit - works with petabyte datasets
- **Memory usage**: Constant regardless of dataset size
- **Network overhead**: Minimal - only ObjectRef handles transferred

## Cache Invalidation

### Automatic Invalidation
Cache entries are automatically invalidated when:
- ObjectRefs become invalid (garbage collected)
- DataContext settings change that affect execution

### Manual Invalidation

```python
from ray.data._internal.cache.stage_cache import clear_stage_cache, invalidate_stage_cache

# Clear entire cache
clear_stage_cache()

# Invalidate specific patterns (clears all for now)
invalidate_stage_cache()
```

## Monitoring and Observability

### Cache Statistics
The cache provides comprehensive statistics for monitoring:

```python
stats = get_stage_cache_stats()

# Key metrics for petabyte-scale monitoring
print(f"Plan hits: {stats['plan_hits']}")           # ObjectRef reuse hits
print(f"Result hits: {stats['result_hits']}")       # Small result hits  
print(f"Hit rate: {stats['hit_rate']:.1%}")         # Overall efficiency
print(f"Memory usage: {stats['total_memory_bytes']:,} bytes")  # Should be minimal
```

### Performance Monitoring
```python
# Memory usage should be constant regardless of dataset size
assert stats['total_memory_bytes'] < 50 * 1024 * 1024  # < 50MB always

# Hit rate should be high for repeated operations
assert stats['hit_rate'] > 0.8  # > 80% for typical workloads
```

## Best Practices

### When to Enable Caching
- ✅ **Repeated identical operations** on large datasets
- ✅ **Interactive data exploration** with frequent re-execution
- ✅ **ML training pipelines** with repeated data loading
- ✅ **ETL workflows** with repeated transformations

### When NOT to Enable Caching  
- ❌ **One-time operations** that won't be repeated
- ❌ **Rapidly changing datasets** where cache would be stale
- ❌ **Memory-constrained environments** (though overhead is minimal)

### Performance Tips
1. **Enable early**: Turn on caching before running expensive operations
2. **Monitor memory**: Check cache statistics to ensure minimal footprint
3. **Clear when needed**: Clear cache when switching to different datasets
4. **Use with materialize()**: Cache works best with explicit materialization

## Comparison with Other Caching Systems

| Feature | Ray Data Stage Cache | Spark Cache | Databricks Disk Cache |
|---------|---------------------|-------------|----------------------|
| **Storage** | ObjectRef handles | In-memory blocks | Local disk files |
| **Scope** | Any Dataset operation | DataFrames/RDDs | Parquet files only |
| **Trigger** | Automatic on materialize | Manual `.cache()` | Automatic on read |
| **Eviction** | LRU by entry count | LRU by memory | LRU by disk space |
| **Scale** | Unlimited (petabyte+) | Memory limited | Disk limited |
| **Overhead** | Minimal (~KB per dataset) | High (data size) | Medium (disk I/O) |

## Troubleshooting

### Common Issues

**Cache not working?**
- Check `ctx.enable_stage_cache = True` is set
- Verify operations are truly identical (same plan + context)
- Check cache statistics with `get_stage_cache_stats()`

**Memory usage growing?**  
- This should not happen - contact Ray team if memory > 50MB
- Clear cache with `clear_stage_cache()` as workaround

**Poor hit rates?**
- Operations may not be identical due to context differences
- Check fingerprint computation with debug logging
- Consider if operations are actually repeated

### Debug Logging

Enable debug logging to see cache operations:

```python
import logging
logging.getLogger("ray.data._internal.cache.stage_cache").setLevel(logging.DEBUG)
```

## Implementation Details

### Architecture
```
Dataset.materialize()
    ↓
Check stage cache for plan fingerprint
    ↓
If hit: Reconstruct MaterializedDataset from cached ObjectRefs
    ↓  
If miss: Execute plan and cache resulting ObjectRefs
```

### Cache Storage
- **Plan fingerprint**: Hash of logical plan + critical context
- **Cached metadata**: Schema, stats, context, ObjectRef handles
- **Memory footprint**: ~1KB per cached dataset operation

### Thread Safety
- Uses Ray's actor model and ObjectRef system for thread safety
- No traditional threading primitives required
- Concurrent access handled by Ray's distributed architecture

This caching system provides transparent acceleration for Ray Data workflows while maintaining constant memory usage regardless of dataset size, making it suitable for production petabyte-scale workloads.
