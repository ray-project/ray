# Agent Instructions for Ray Data Tests

> These instructions apply to AI-assisted work under `python/ray/data/tests/`.
> They add to [`python/ray/data/AGENTS.md`](../AGENTS.md) and the root
> [`AGENTS.md`](../../../../AGENTS.md), which always apply.

Keep tests short: shared setup goes in fixtures, and asserts check what a user
sees. These rules are for new and changed tests. Don't rewrite tests the PR
doesn't otherwise touch.

## Ray cluster

- Use `ray_start_regular_shared`, or `ray_start_2_cpus_shared` /
  `ray_start_10_cpus_shared` when the CPU count matters. They start Ray once
  per module. Starting Ray takes seconds, so a cluster per test or per
  parametrized case slows the suite and pushes targets past their timeout.
- Don't call `ray.init()` in a test. If a test needs its own `ray.init()`
  arguments, such as a `runtime_env`, use `shutdown_only`.
- Use `ray_start_cluster` only when the test checks multi-node placement. Keep
  it to one test, not one per parametrized case.

## DataContext and V1/V2 readers

- Change `DataContext` through `restore_data_context`, which resets it after
  the test, or `data_context_override` with `indirect=True`. Don't
  `monkeypatch.setattr` fields of `DataContext.get_current()`.
- To run a read through both the V1 reader and DataSourceV2, take the
  `datasource_v2` fixture. For a V2-only test, write
  `@pytest.mark.parametrize("datasource_v2", [True], indirect=True)`. Don't
  set `use_datasource_v2` yourself.

## Test data

- Write files under `tmp_path`. For S3, use `s3_fs` and `s3_path`, which run
  against a local mock S3 server.
- Write test files with the real writer (`pq.write_table`,
  `ray.data.from_items(...).write_parquet(...)`), not hand-built bytes or
  checked-in binary files.
- Before writing a helper, look in `conftest.py`, `util.py` and the
  `*_test_utils.py` modules.
- Setup that two tests in a file share becomes a fixture in that file. Setup
  that test files share goes in a `*_test_utils.py` module.
- Use `scope="module"` for an expensive fixture no test changes. Keep the
  default function scope for anything a test changes.

## Parametrize

- Write one test over a table of cases, not one test per case. Name each case:
  `pytest.param(..., id="null_partition")`.
- Add a parameter only if the code under test branches on it. Avoid cross
  products of parameters.
- Don't branch on a parameter inside a test (`if param: ... else: ...`) to
  pick different setup or asserts. Write two tests.

## Asserts

- Compare the whole result in one assert, such as
  `assert rows_same(ds.to_pandas(), expected_df)`. Ray Data doesn't keep row
  order, so sort first or use `rows_same`.
- Drop asserts an earlier assert already implies, such as `isinstance` after
  unpacking or `is not None` before an equality check.
- Check errors with `pytest.raises(SomeError, match="...")`.
- To check logs with `caplog`, also take `propagate_logs`. Ray Data's loggers
  don't propagate to the root logger.

## Mocks

- Mock only what a test can't run for real: external services, clocks, or a
  failure to inject.
- Don't patch Ray (`ray.get`, `ray.wait`, `ray.remote`, `cached_remote_fn`) or
  Ray Data internals to drive a test, and don't assert on private attributes.
  Test through `ray.data.read_*` and `Dataset` methods.
- Tests in `tests/unit/` follow [`unit/README.md`](unit/README.md): no Ray
  runtime, no file or network IO, no sleeps.

## Example

Two tests that each start Ray, set the context by hand and check fields one by
one:

```python
def test_read_v1(tmp_path):
    ray.init(num_cpus=2)
    DataContext.get_current().use_datasource_v2 = False
    pq.write_table(pa.table({"id": [1, 2]}), tmp_path / "a.parquet")
    rows = ray.data.read_parquet(str(tmp_path)).take_all()
    assert len(rows) == 2
    assert rows[0]["id"] == 1
    assert rows[1]["id"] == 2
    ray.shutdown()


def test_read_v2(tmp_path):
    ...  # The same, with use_datasource_v2 = True.
```

become one test on the shared cluster:

```python
def test_read(ray_start_regular_shared, tmp_path, datasource_v2):
    pq.write_table(pa.table({"id": [1, 2]}), tmp_path / "a.parquet")
    actual = ray.data.read_parquet(str(tmp_path)).to_pandas()
    assert rows_same(actual, pd.DataFrame({"id": [1, 2]}))
```
