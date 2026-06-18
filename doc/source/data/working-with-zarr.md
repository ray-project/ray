(working_with_zarr)=

# Working with Zarr

Ray Data reads [Zarr v2](https://zarr.readthedocs.io/) stores — chunked, compressed,
N-dimensional arrays on local disk or cloud object storage — with
{func}`ray.data.read_zarr` (zarr-python 2.x / Zarr v2 stores).

This guide covers:

- [The two output schemas](#output-schemas) — long-form (default) and aligned wide-form
- [Selecting arrays and metadata discovery](#selecting-arrays-and-metadata-discovery)
- [Controlling chunk size](#controlling-chunk-size)
- [Reading row-aligned arrays](#reading-row-aligned-arrays)
- [Custom codecs](#custom-codecs)
- [Cloud storage and credentials](#cloud-storage-and-credentials)

For the full parameter reference, see {func}`ray.data.read_zarr`.

## Output schemas

`read_zarr` produces one of two schemas, selected by `align_axis_0`.

### Long-form (default)

By default each output row is **one chunk of one array**, with columns:

- `array` — the array's path in the store (for example `"data/camera0_rgb"`, or `""` for a root-level array).
- `chunk_index` — the N-D index of the chunk in its array's chunk grid.
- `chunk_slices` — per-axis `(start, stop)` of the chunk in the array's coordinate space.
- `chunk` — the chunk's data at its natural shape (trailing-edge chunks may be shorter; no padding).

Arrays read in the same call need not share any dimension — different ranks, shapes,
dtypes, and native chunk sizes coexist as separate rows.

```python
import ray

ds = ray.data.read_zarr("s3://anonymous@ray-example-data/mnist-tiny.zarr")
```

```{note}
The `chunk` column is a tensor, and tensors of different rank or dtype can't be
combined into one batch. Consume long-form **per array** (filter on the `array`
column first), or — when arrays are row-aligned (share `shape[0]`) — use
`align_axis_0=True` so each array becomes its own column, which is batch-safe.
```

### Aligned wide-form (`align_axis_0=True`)

With `align_axis_0=True` each row is **one axis-0 chunk shared across the selected
arrays**, with columns:

- `t_start`, `t_stop` — the global axis-0 range of the row.
- one column per selected array, holding that array's `[t_start:t_stop, ...]` slice.

All selected arrays must share `shape[0]` and resolve to the same axis-0 chunk size
(after any `chunk_shapes` override). Use `array_paths` to choose which arrays participate —
`align_axis_0` itself doesn't filter.

```python
ds = ray.data.read_zarr(
    "s3://anonymous@ray-example-data/mnist-tiny.zarr",
    align_axis_0=True,
    chunk_shapes=[50],
)
```

## Selecting arrays and metadata discovery

By default `read_zarr` reads every array it discovers. Pass `array_paths` to read a
subset:

```python
ds = ray.data.read_zarr(store_uri, array_paths=["images", "labels"])
```

Discovery follows these rules:

- If the store has consolidated `.zmetadata`, it's the canonical array list (filtered by
  `array_paths` if given). This is the fast path.
- Otherwise, if `array_paths` is given, each requested array's metadata is read directly
  — no `.zmetadata` required.
- Otherwise, if `allow_full_metadata_scan=True`, the store is recursively scanned for
  arrays. This can be slow or costly on large remote stores, so it's off by default;
  prefer consolidating metadata with `zarr.consolidate_metadata` ahead of time.

## Controlling chunk size

Zarr stores are often chunked finely (for example one image per chunk). 
You can use `chunk_shapes` to chunk the leading axes **at read
time** to coarsen (or refine) the granularity at which reading happens.
Note that this does not affect downstream batchsizes and is internal to the reading operation.
Finely chunked reading can hurt performance.

- A **sequence** applies as a shared prefix across all selected arrays, overriding the
  leading axes and keeping trailing axes native. `chunk_shapes=[16]` turns native chunks
  `(1, 224, 224, 3)` into `(16, 224, 224, 3)` and `(50,)` into `(16,)`.
- A **dict** overrides per array; arrays absent from it keep native chunks.

```python
# Coarsen every array's axis 0 to 16-element chunks.
ds = ray.data.read_zarr(store_uri, chunk_shapes=[16])

# Different overrides per array.
ds = ray.data.read_zarr(store_uri, chunk_shapes={"images": [16], "labels": [64]})
```

## Reading row-aligned arrays

When arrays share an axis-0 (for example a timestep axis), `align_axis_0=True`
co-iterates them as the [wide-form schema](#output-schemas) above — one row per axis-0
chunk, one column per array.

For sliding-window pipelines, `overlap` extends each row's per-array data forward by `N`
timesteps from the next row's range (clipped at the end of the store). With
`overlap=K-1`, any window of length `K` that starts in a row's owned `[t_start, t_stop)`
fits entirely within that row's slice.

```python
ds = ray.data.read_zarr(
    store_uri,
    align_axis_0=True,
    chunk_shapes=[50],
    overlap=9,  # length-10 windows fit within a row
)
```

## Custom codecs

Stores compressed with non-stdlib codecs (for example `imagecodecs` JPEG-XL) need the
codec package imported and registered **in every Ray worker**, not just the driver.
Register it with a `worker_process_setup_hook` — pass an importable callable or its
dotted path (a string of code isn't accepted; a string is interpreted as an import
path):

```python
import ray

ray.init(runtime_env={
    "worker_process_setup_hook": "imagecodecs.numcodecs.register_codecs",
})
```

This is a particularity of the underlying Zarr library.


## Zarr's .zattrs

`read_zarr` doesn't surface each array's `.zattrs` (Zarr user attributes) in the row
schema — they're invariant per array, so repeating them on every row would just bloat
the output. Read them separately (for example with the `zarr` package) if your job
needs them.

