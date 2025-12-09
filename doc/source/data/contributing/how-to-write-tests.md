(how-to-write-tests)=
# How to write tests

:::{note}
**Disclaimer**: There are no hard rules in software engineering. Use your judgment when 
applying these.
:::

Flaky or brittle tests (the kind that break when assumptions shift) slow development. 
Nobody likes getting stuck on a PR because a test failed for reasons unrelated to their
change.

This guide is a collection of practices to help you write tests that support the Ray 
Data project, not slow it down.

## General good practices

### Prefer unit tests over integration tests

Unit tests give faster feedback and make it easier to pinpoint failures. They run in 
milliseconds, not seconds, and don’t depend on Ray clusters, external systems, or 
timing. This keeps the test suite fast, reliable, and easy to maintain.

:::{note}
Put unit tests in `python/ray/data/tests/unit`.
:::

### Use fixtures, skip try-finally

Fixtures make tests cleaner, more reusable, and better isolated. They’re the right tool 
for setup and teardown, especially for things like `monkeypatch`.

`try-finally` works, but fixtures make intent clearer and avoid boilerplate.

**Original code**
```python
def test_dynamic_block_split(ray_start_regular_shared):
    ctx = ray.data.context.DataContext.get_current()
    original_target_max_block_size = ctx.target_max_block_size

    ctx.target_max_block_size = 1
    try: 
        ...
    finally:
        ctx.target_max_block_size = original_target_max_block_size
```

**Better**
```python
def test_dynamic_block_split(ray_start_regular_shared, restore_data_context):
    ctx = ray.data.context.DataContext.get_current()
    target_max_block_size = ctx.target_max_block_size
    ... # No need for try-finally
```

## Ray-specific practices

### Don't assume Datasets produce outputs in a specific order

Unless you set `preserve_order=True` in the `DataContext`, Ray Data doesn’t guarantee 
an output order. If your test relies on order without explicitly asking for it, you’re 
setting yourself up for brittle failures.

**Original code**
```python
ds_dfs = []
for path in os.listdir(out_path):
    assert path.startswith("data_") and path.endswith(".parquet")
    ds_dfs.append(pd.read_parquet(os.path.join(out_path, path)))

ds_df = pd.concat(ds_dfs).reset_index(drop=True)
df = pd.concat([df1, df2]).reset_index(drop=True)
assert ds_df.equals(df)
```

**Better**
```python
from ray.data._internal.util import rows_same

actual_data = pd.read_parquet(out_path)
expected_data = pd.concat([df1, df2]
assert rows_same(actual_data, expected_data)
```

:::{tip}
Use the `ray.data._internal.util.rows_same` utility function to compare pandas 
DataFrames for equality while ignoring indices and order.
:::

### Prefer shared cluster fixtures

Prefer shared cluster fixtures like `ray_start_regular_shared` over isolated cluster
fixtures like `shutdown_only` and `ray_start_regular`.

`shutdown_only` and `ray_start_regular` restart the Ray cluster after each test
finishes. Starting and stopping Ray can take over a second — which sounds small, but 
across thousands of tests (plus parameterizations) it adds up fast.

Only use isolated clusters when your test truly needs a fresh cluster.

:::{note}
There's an inherent tradeoff between isolation and speed here. For this specific case, 
choose to prioritize speed.
:::

**Original code**
```python
@pytest.mark.parametrize("concurrency", [-1, 1.5], ids=["negative", "float"])
def test_invalid_concurrency_raises(shutdown_only, concurrency):
    ds = ray.data.range(1)  # Each parametrization restarts the Ray cluster!
    with pytest.raises(ValueError):
        ds.map(lambda row: row, concurrency=concurrency)
```

**Better**
```python
@pytest.mark.parametrize("concurrency", [-1, 1.5], ids=["negative", "float"])
def test_invalid_concurrency_raises(ray_start_regular_shared, concurrency):
    ds = ray.data.range(1)  # Each parametrization reuses the same Ray cluster.
    with pytest.raises(ValueError):
        ds.map(lambda row: row, concurrency=concurrency)
```

## Avoid testing against repr outputs to validate specific data

`repr` output isn’t part of any interface contract — it can change at any time.
Besides, tests that assert against repr often hide the real intent: are you trying to
check the data, or just how it happens to print? Be explicit about what you care about.



**Original code**
```python
assert str(ds) == "Dataset(num_rows=6, schema={one: int64, two: string})", ds
```

**Better**
```python
assert ds.schema() == Schema(pa.schema({"one": pa.int64(), "two": pa.string()}))
assert ds.count() == 6
```

## Avoid assumptions about the number or size of blocks

Unless you’re testing an API like `repartition`, don’t lock your test to a specific 
number or size of blocks. Both can change depending on the implementation or the cluster 
config — and that’s usually fine.

**Original code**

```python
ds = ray.data.read_parquet(paths + [txt_path], filesystem=fs)
assert ds._plan.initial_num_blocks() == 2  # Where does 2 come from?
assert rows_same(ds.to_pandas(), expected_data)
```

**Better**

```python
ds = ray.data.read_parquet(paths + [txt_path], filesystem=fs)
# Assertion about number of blocks has been removed.
assert rows_same(ds.to_pandas(), expected_data)
```

**Original code**
```python
ds2 = ds.repartition(5)
assert ds2._plan.initial_num_blocks() == 5
assert ds2._block_num_rows() == [10, 10, 0, 0, 0]  # Magic numbers?
```

**Better**
```python
ds2 = ds.repartition(5)
assert sum(len(bundle.blocks) for bundle in ds.iter_internal_ref_bundles()) == 5
# Assertion about the number of rows in each block has been removed.
```

## Avoid testing that the DAG looks a particular way

The operators in the execution plan can shift over time as the implementation evolves. 
Unless you’re specifically testing optimization rules or working at the operator level,
tests shouldn’t expect a particular DAG structure.

**Original code**
```python
# Check that metadata fetch is included in stats.
assert "FromArrow" in ds.stats()
# Underlying implementation uses `FromArrow` operator
assert ds._plan._logical_plan.dag.name == "FromArrow"
```

**Better**
```python
# (Assertions removed). 
```
