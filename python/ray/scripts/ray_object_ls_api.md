# Ray Object Listing API: Enhanced `ray object ls`

## 🎯 Motivation

Ray's object inspection capabilities were previously fragmented across multiple commands with overlapping but incomplete functionality. This enhancement consolidates and improves object analysis into a single, powerful interface.

### The Problem: Fragmented Functionality

Before this enhancement, users had to navigate between different commands, each with limitations:

| Feature | `ray list objects` | `ray memory` | Problems |
|---------|-------------------|--------------|----------|
| **Basic object listing** | ✅ | ❌ | No simple tabular output in `ray memory` |
| **Filtering by column value** | ✅ | ❌ | No filtering possible in `ray memory` |
| **Sorting by size (asc)** | ❌ | ✅ | No sorting capability in `ray list objects` |
| **Sorting by size (desc)** | ❌ | ❌ | No way to easily see top-N objects by size in either |
| **Grouping & aggregation** | ❌ | ✅ | No way to group objects in basic listing |
| **Reference type analysis** | ❌ | ✅ | No reference type stats in basic listing |

### The Solution: Unified `ray object ls`

The enhanced `ray object ls` combines the best of both worlds:
- **Maintains backward compatibility** with existing `ray list objects`
- **Adds powerful analytical features** from `ray memory`
- **Provides a clean, intuitive interface** for all object inspection needs
- **Supports both programmatic (JSON/YAML) and analytical (grouped) workflows**

---

## 📖 API Reference

### Basic Usage

```bash
ray object ls [OPTIONS]
```

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--format` | `default\|json\|yaml\|table` | `default` | Output format |
| `--filter` | `key=value` | - | Filter objects (multiple allowed) |
| `--limit` | `int` | `100` | Maximum objects to return |
| `--detail` | `flag` | `false` | Include detailed information |
| `--sort-by` | `object_size` | - | Sort by object size |
| `--order` | `asc\|desc` | `asc` | Sort order |
| `--group-by` | `call_site\|node_ip\|reference_type\|pid` | - | Group objects with summaries |
| `--timeout` | `float` | `30.0` | Request timeout in seconds |
| `--address` | `string` | - | Ray cluster address |

### Important Constraints

- **Grouping Limitation**: `--group-by` is only compatible with `default` and `table` formats (not `json`/`yaml`)
- **Sorting Scope**: `--sort-by` only supports `object_size` (use `--group-by` for categorical analysis)

---

## 🚀 Usage Examples

### 1. Basic Object Listing (Backward Compatible)

```bash
# List all objects (same as before)
ray object ls

# List with JSON output
ray object ls --format json

# Filter by reference type
ray object ls --filter "reference_type=PINNED_IN_MEMORY" --limit 20
```

### 2. Size-Based Analysis

```bash
# Find the 10 largest objects in the cluster
ray object ls --sort-by object_size --order desc --limit 10

# Smallest objects first
ray object ls --sort-by object_size --order asc --limit 5
```

### 3. Grouping and Aggregation

#### Group by Call Site (Memory Leak Detection)
```bash
# See where objects are being created
ray object ls --group-by call_site --limit 5

# Output shows:
# --- Group: script.py:42|train_model ---
# Objects: 1250
# Total Size: 2.3 GB
# Reference Types: PINNED_IN_MEMORY: 1000, LOCAL_REFERENCE: 250
# [object details table]
```

#### Group by Reference Type (Memory Usage Patterns)
```bash
# Analyze how objects are being held in memory
ray object ls --group-by reference_type
```

#### Group by Node (Cluster Distribution)
```bash
# See object distribution across cluster nodes
ray object ls --group-by node_ip --limit 3
```

#### Group by Process (Per-Process Analysis)
```bash
# Objects per process/worker
ray object ls --group-by pid --limit 10
```

### 4. Combined Analysis (The Power Combo)

```bash
# Top 3 largest objects per call site
ray object ls --sort-by object_size --order desc --group-by call_site --limit 3

# This gives you:
# 1. Objects sorted by size (largest first)
# 2. Grouped by where they were created
# 3. Limited to top 3 per group
# 4. Aggregate statistics per group
```

### 5. Memory Debugging Workflow

```bash
# Step 1: Overview of memory usage by reference type
ray object ls --group-by reference_type

# Step 2: Find largest objects
ray object ls --sort-by object_size --order desc --limit 20

# Step 3: Analyze by creation location
ray object ls --group-by call_site --limit 5

# Step 4: Check specific node if needed
ray object ls --group-by node_ip --filter "ip=192.168.1.10"
```

---

## 🔄 Migration Guide

### From `ray list objects`

✅ **No changes needed** - all existing commands work exactly the same:

```bash
# These work unchanged:
ray object ls
ray object ls --format json
ray object ls --filter "state=READY" --limit 50
```

### From `ray memory`

Replace memory-specific commands with object ls equivalents:

| Old (`ray memory`) | New (`ray object ls`) |
|-------------------|----------------------|
| `ray memory --group-by STACK_TRACE` | `ray object ls --group-by call_site` |
| `ray memory --group-by NODE_ADDRESS` | `ray object ls --group-by node_ip` |
| `ray memory --sort-by OBJECT_SIZE` | `ray object ls --sort-by object_size --order desc` |
| `ray memory --num-entries 10` | `ray object ls --limit 10` |

### Key Differences

1. **JSON/YAML Support**: Unlike `ray memory`, `ray object ls` supports programmatic formats (except for grouped output)
2. **Backward Compatibility**: Unlike `ray memory`, existing scripts continue to work
3. **Simplified Interface**: No sorting by categorical fields, we just support grouping by them

---

## 🎛️ Technical Implementation

### Operation Precedence

The processing pipeline follows this order:
1. **Filtering** (server-side, fast)
2. **Sorting** (client-side, when needed)
3. **Grouping** (client-side, with aggregation)
4. **Limiting** (per-group when grouping, global when not)

### Smart Limit Strategy

- **Simple queries**: Limit applied server-side for performance
- **Sort/Group queries**: Higher server limit, user limit applied client-side for correctness

---

## 🔮 Future Enhancements

This unified API provides a foundation for future object inspection features:

- **Time-series analysis**: Track object lifecycle over time
- **Cross-references**: See relationships between objects
- **Custom grouping**: User-defined grouping functions
- **Export capabilities**: Save analysis results to files
- **Integration**: Better integration with Ray Dashboard and monitoring tools

---
