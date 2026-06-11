"""Generate parquet files for read-path benchmarks with four independent knobs:

    --file-size           target ON-DISK size of each file        (e.g. 256MiB)
    --num-files           how many files to produce
    --row-group-size      target ON-DISK size of each row group   (e.g. 16MiB)
    --compression-ratio   in-memory(uncompressed) / on-disk        (>= 1.0)

The compression-ratio knob controls the disk-vs-memory gap that a reader pays
when it materialises a row group. R = 1.0 means incompressible (random) data:
on-disk ~= in-memory. R = 4.0 means each row group decompresses to ~4x its
on-disk footprint, so a 16 MiB row group balloons to ~64 MiB of RAM when read.

How the ratio is achieved
-------------------------
Each row's binary value is `rand_bytes(r) + zero_bytes(z)`. The random head is
incompressible so it survives the codec ~1:1; the zero tail collapses to almost
nothing. With total per-row payload P = r + z held fixed at R * (on-disk row
size), tuning r alone moves the *on-disk* size without changing the *in-memory*
size -- which is exactly the (disk, memory) pair we want to pin independently.

Because real codecs add framing/page overhead and don't compress zeros to
literally zero, the initial guess r = rg_disk/rows is only approximate. So we
CALIBRATE: write one row-group table to a buffer, measure the compressed size,
rescale r toward the target, and repeat a few times until the row group lands
within tolerance. Reusing that single calibrated row-group table for every row
group of every file caps generation memory at one row group (R * row_group_size)
rather than one whole file.

Output goes to a local directory or an s3:// prefix (auto-detected). S3 PUTs are
parallelised; the in-memory row-group body is generated once and reused.

Usage:
    python parquet_dataset_gen.py \\
        --file-size 256MiB --num-files 40 \\
        --row-group-size 16MiB --compression-ratio 4 \\
        --output ./out/text/m/medium

    python parquet_dataset_gen.py \\
        --file-size 1GiB --num-files 100 \\
        --row-group-size 64MiB --compression-ratio 1 --codec none \\
        --output s3://anyscale-data-benchmarks/read_path/parquet/m/large
"""
import argparse
import io
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Tuple

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


# --- size parsing ---------------------------------------------------------
_UNITS = {
    "": 1,
    "b": 1,
    "kib": 1024,
    "mib": 1024**2,
    "gib": 1024**3,
    "tib": 1024**4,
    "kb": 1000,
    "mb": 1000**2,
    "gb": 1000**3,
    "tb": 1000**4,
    "k": 1024,
    "m": 1024**2,
    "g": 1024**3,
    "t": 1024**4,
}


def parse_size(s: str) -> int:
    """'16MiB' -> 16777216. Accepts IEC (KiB/MiB/GiB), SI (KB/MB/GB) and bare
    ints. Single-letter K/M/G are treated as binary (KiB/MiB/GiB)."""
    m = re.fullmatch(r"\s*([\d.]+)\s*([a-zA-Z]*)\s*", str(s))
    if not m:
        raise argparse.ArgumentTypeError(f"cannot parse size: {s!r}")
    value, unit = float(m.group(1)), m.group(2).lower()
    if unit not in _UNITS:
        raise argparse.ArgumentTypeError(f"unknown size unit: {unit!r}")
    return int(value * _UNITS[unit])


def human(n: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024 or unit == "TiB":
            return f"{n:.2f} {unit}"
        n /= 1024


# --- row-group construction & calibration ---------------------------------
def _build_row_group_table(
    rows: int, payload_bytes: int, random_bytes: int, rng: np.random.Generator
) -> pa.Table:
    """One row-group table. Each value = `random_bytes` of incompressible noise
    followed by `payload_bytes - random_bytes` zeros, so the codec compresses it
    down to roughly `random_bytes` per row. Distinct random heads keep the
    column off the dictionary/RLE fast-paths."""
    random_bytes = max(0, min(random_bytes, payload_bytes))
    zero_tail = b"\x00" * (payload_bytes - random_bytes)
    if random_bytes:
        noise = rng.integers(0, 256, size=rows * random_bytes, dtype=np.uint8).tobytes()
        values = [
            noise[i * random_bytes : (i + 1) * random_bytes] + zero_tail
            for i in range(rows)
        ]
    else:
        values = [zero_tail] * rows
    return pa.table({"data": pa.array(values, type=pa.binary())})


def _encode_one_row_group(table: pa.Table, codec: str) -> bytes:
    """Write a single-row-group parquet file to memory and return the bytes.
    Dictionary encoding is disabled so the codec, not the encoding, governs the
    compression ratio -- which is what the knob is meant to control."""
    buf = io.BytesIO()
    pq.write_table(
        table,
        buf,
        compression=codec,
        use_dictionary=False,
        row_group_size=table.num_rows,  # force exactly one row group
    )
    return buf.getvalue()


def _calibrate(
    rg_disk_target: int,
    rows: int,
    payload_bytes: int,
    codec: str,
    rng: np.random.Generator,
    max_iters: int = 6,
    tol: float = 0.03,
) -> Tuple[pa.Table, bytes, int]:
    """Find the per-row random-byte count that makes one row group compress to
    ~rg_disk_target. Returns (table, encoded_row_group_bytes, random_bytes).
    payload_bytes (the in-memory per-row size) is held FIXED throughout, so only
    the on-disk size moves and the memory ratio stays pinned at the requested R.
    """
    # Initial guess: surviving bytes per row == on-disk bytes per row.
    random_bytes = max(0, min(payload_bytes, rg_disk_target // max(1, rows)))
    table = _build_row_group_table(rows, payload_bytes, random_bytes, rng)
    body = _encode_one_row_group(table, codec)

    for it in range(max_iters):
        actual = len(body)
        err = actual / rg_disk_target - 1.0
        print(
            f"  [calibrate {it}] random={random_bytes}B/row -> row group "
            f"{human(actual)} (target {human(rg_disk_target)}, err {err:+.1%})",
            flush=True,
        )
        if abs(err) <= tol:
            break
        # On-disk size is ~ (rows * random_bytes) + ~constant overhead. Estimate
        # the overhead from this sample and solve for the random_bytes that hits
        # the target, instead of a naive proportional rescale (faster to settle
        # when fixed page/footer overhead is a big fraction of a small target).
        overhead = max(0, actual - rows * random_bytes)
        need = rg_disk_target - overhead
        new_random = int(round(need / max(1, rows)))
        new_random = max(0, min(payload_bytes, new_random))
        if new_random == random_bytes:
            break  # clamped or converged; nothing more to do
        random_bytes = new_random
        table = _build_row_group_table(rows, payload_bytes, random_bytes, rng)
        body = _encode_one_row_group(table, codec)

    return table, body, random_bytes


# --- output backends ------------------------------------------------------
class _Backend:
    def exists_nonempty(self, key: str) -> bool:
        ...

    def put(self, key: str, body: bytes) -> None:
        ...

    def size(self, key: str) -> Optional[int]:
        ...

    def describe(self, key: str) -> str:
        ...


class _LocalBackend(_Backend):
    def __init__(self, root: str):
        self.root = root

    def _path(self, key: str) -> str:
        return os.path.join(self.root, key)

    def exists_nonempty(self, key: str) -> bool:
        if not os.path.isdir(self.root):
            return False
        return (
            any(os.scandir(self.root)) if not key else os.path.exists(self._path(key))
        )

    def put(self, key: str, body: bytes) -> None:
        path = self._path(key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(body)

    def size(self, key: str) -> Optional[int]:
        try:
            return os.path.getsize(self._path(key))
        except OSError:
            return None

    def describe(self, key: str) -> str:
        return self._path(key)


class _S3Backend(_Backend):
    def __init__(self, bucket: str, prefix: str):
        import boto3  # local import: only needed for s3:// outputs

        self.s3 = boto3.client("s3")
        self.bucket = bucket
        self.prefix = prefix

    def exists_nonempty(self, key: str) -> bool:
        resp = self.s3.list_objects_v2(
            Bucket=self.bucket, Prefix=self.prefix + key, MaxKeys=1
        )
        return bool(resp.get("KeyCount"))

    def put(self, key: str, body: bytes) -> None:
        self.s3.put_object(Bucket=self.bucket, Key=self.prefix + key, Body=body)

    def size(self, key: str) -> Optional[int]:
        try:
            return self.s3.head_object(Bucket=self.bucket, Key=self.prefix + key)[
                "ContentLength"
            ]
        except Exception:
            return None

    def describe(self, key: str) -> str:
        return f"s3://{self.bucket}/{self.prefix}{key}"


def _make_backend(output: str) -> _Backend:
    if output.startswith("s3://"):
        bucket, _, prefix = output.removeprefix("s3://").partition("/")
        prefix = (prefix.rstrip("/") + "/") if prefix else ""
        return _S3Backend(bucket, prefix)
    return _LocalBackend(output.rstrip("/"))


# --- file writing ---------------------------------------------------------
def _build_file_body(table: pa.Table, codec: str, row_groups: int) -> bytes:
    """Concatenate `row_groups` copies of the calibrated row-group table into a
    single parquet file. ParquetWriter emits one row group per write_table call,
    so we get exactly `row_groups` row groups of the calibrated size."""
    buf = io.BytesIO()
    writer = pq.ParquetWriter(
        buf, table.schema, compression=codec, use_dictionary=False
    )
    for _ in range(row_groups):
        writer.write_table(table, row_group_size=table.num_rows)
    writer.close()
    return buf.getvalue()


# --- driver ---------------------------------------------------------------
_PUT_PARALLELISM = int(os.environ.get("DATASET_GEN_PARALLELISM", "32"))

# Default logical memory passed to read_parquet when --read-memory is omitted.
DEFAULT_READ_MEMORY_BYTES = 4 * 1024**3  # 4 GiB


def _read_back(path: str, total_disk_bytes: Optional[int], memory: int) -> None:
    """Post-generation read benchmark. Reads the just-written parquet with Ray
    Data and drains it through internal ref bundles -- the iter_bundles consume
    mode: blocks are materialised by the read tasks but never shipped to the
    driver, so this measures read+decode throughput rather than transfer. Read
    config is fixed except for `memory` (logical bytes passed to read_parquet):
    format=parquet, iter_bundles."""
    try:
        import ray
    except ImportError:
        print(
            "[read] ray not installed; skipping read-back "
            "(`pip install 'ray[data]'` to enable).",
            flush=True,
        )
        return

    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)

    print(
        f"[read] ray.data.read_parquet(memory={human(memory)}) <- {path}",
        flush=True,
    )
    ds = ray.data.read_parquet(path, memory=memory)

    t0 = time.time()
    num_bundles = num_rows = 0
    for bundle in ds.iter_internal_ref_bundles():
        num_bundles += 1
        try:  # RefBundle.num_rows() may be None if metadata is unknown
            r = bundle.num_rows()
            if r is not None:
                num_rows += r
        except Exception:
            pass
    elapsed = time.time() - t0

    rows_s = num_rows / elapsed if elapsed else 0.0
    msg = (
        f"[read done] {num_bundles} bundles, {num_rows} rows in {elapsed:.2f}s "
        f"({rows_s:,.0f} rows/s"
    )
    if total_disk_bytes:
        msg += f", {human(total_disk_bytes / elapsed)}/s on-disk" if elapsed else ""
    print(msg + ")", flush=True)


def generate(
    output: str,
    file_size: int,
    num_files: int,
    row_group_size: int,
    compression_ratio: float,
    rows_per_row_group: int,
    codec: str,
    force: bool,
    read_back: bool,
    read_memory: int,
) -> None:
    backend = _make_backend(output)

    if not force and backend.exists_nonempty(""):
        print(f"[skip] {output} already populated (use --force to overwrite).")
        if read_back:
            _read_back(output, None, read_memory)  # benchmark the existing data
        return

    if codec == "none" and compression_ratio != 1.0:
        print(
            f"[warn] codec=none cannot compress; clamping compression-ratio "
            f"{compression_ratio} -> 1.0",
            flush=True,
        )
        compression_ratio = 1.0
    if compression_ratio < 1.0:
        raise SystemExit("compression-ratio must be >= 1.0")

    rows = rows_per_row_group
    # In-memory per-row payload is fixed by (target on-disk row, ratio); only the
    # random fraction (hence on-disk size) is calibrated, so memory stays pinned.
    rg_disk_per_row = row_group_size / rows
    payload_bytes = max(1, int(round(rg_disk_per_row * compression_ratio)))
    row_groups_per_file = max(1, round(file_size / row_group_size))

    print(
        f"[plan] {num_files} file(s) -> ~{human(file_size)} each\n"
        f"       row group ~{human(row_group_size)} on disk x "
        f"{row_groups_per_file}/file, {rows} rows/group\n"
        f"       compression ratio R={compression_ratio} (codec={codec}); "
        f"in-memory per row group ~{human(payload_bytes * rows)}",
        flush=True,
    )

    rng = np.random.default_rng(seed=0)
    table, _, random_bytes = _calibrate(row_group_size, rows, payload_bytes, codec, rng)

    body = _build_file_body(table, codec, row_groups_per_file)
    file_disk = len(body)
    mem_per_group = payload_bytes * rows
    achieved_ratio = mem_per_group / (file_disk / row_groups_per_file)
    print(
        f"[built] file body {human(file_disk)} on disk; "
        f"achieved row-group ratio {achieved_ratio:.2f}x "
        f"(in-memory/on-disk), random={random_bytes}B/row",
        flush=True,
    )

    items = [(f"part-{i:08d}.parquet", body) for i in range(num_files)]
    _put_all(backend, items)
    _verify(backend, num_files, file_disk)
    print(f"[done] {output}", flush=True)

    if read_back:
        _read_back(output, file_disk * num_files, read_memory)


def _put_all(backend: _Backend, items: List[Tuple[str, bytes]]) -> None:
    total = len(items)
    step = max(1, total // 20)
    done = [0]
    t0 = time.time()

    def _one(item):
        key, body = item
        backend.put(key, body)
        done[0] += 1
        if done[0] % step == 0 or done[0] == total:
            elapsed = time.time() - t0
            rate = done[0] / elapsed if elapsed else 0
            print(
                f"  [put] {done[0]}/{total} ({rate:.1f}/s, {elapsed:.0f}s)", flush=True
            )

    with ThreadPoolExecutor(max_workers=_PUT_PARALLELISM) as ex:
        list(ex.map(_one, items))


def _verify(backend: _Backend, num_files: int, file_disk: int) -> None:
    """HEAD/stat first, middle, last so a partial run or an odd file is caught."""
    sample = sorted({0, num_files // 2, num_files - 1})
    sizes = []
    for i in sample:
        key = f"part-{i:08d}.parquet"
        sz = backend.size(key)
        if sz is None:
            print(f"  [verify] FAIL missing {backend.describe(key)}", flush=True)
            return
        sizes.append((i, sz))
    avg = sum(sz for _, sz in sizes) / len(sizes)
    ratio = avg / file_disk
    status = "ok" if 0.97 <= ratio <= 1.03 else "WARN"
    pretty = ", ".join(f"part-{i:08d}={human(sz)}" for i, sz in sizes)
    print(f"  [verify {status}] {num_files} files; sample: {pretty}", flush=True)


def main():
    p = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description=__doc__,
    )
    p.add_argument(
        "--file-size",
        type=parse_size,
        required=True,
        help="Target on-disk size per file, e.g. 256MiB, 1GiB.",
    )
    p.add_argument("--num-files", type=int, required=True)
    p.add_argument(
        "--row-group-size",
        type=parse_size,
        required=True,
        help="Target on-disk size per row group, e.g. 16MiB.",
    )
    p.add_argument(
        "--compression-ratio",
        type=float,
        default=1.0,
        help="In-memory(uncompressed) / on-disk. 1.0 = incompressible.",
    )
    p.add_argument(
        "--rows-per-row-group",
        type=int,
        default=1024,
        help="Rows per row group; sets per-row payload granularity.",
    )
    p.add_argument(
        "--codec",
        default="snappy",
        choices=["snappy", "zstd", "gzip", "lz4", "brotli", "none"],
    )
    p.add_argument(
        "--output",
        required=True,
        help="Local dir or s3:// prefix. Files land as part-NNNNNNNN.parquet.",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Generate even if the target already has objects.",
    )
    p.add_argument(
        "--read-back",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="After generating, read the files back with Ray Data "
        "(read_parquet, iter_bundles) and report throughput. "
        "Use --no-read-back to skip.",
    )
    p.add_argument(
        "--read-memory",
        type=parse_size,
        default=DEFAULT_READ_MEMORY_BYTES,
        help="Logical memory passed to read_parquet during read-back, "
        "e.g. 4GiB, 512MiB.",
    )
    args = p.parse_args()

    if args.row_group_size > args.file_size:
        print(
            "[warn] row-group-size > file-size; each file will be one row group.",
            flush=True,
        )

    generate(
        output=args.output,
        file_size=args.file_size,
        num_files=args.num_files,
        row_group_size=args.row_group_size,
        compression_ratio=args.compression_ratio,
        rows_per_row_group=args.rows_per_row_group,
        codec=args.codec,
        force=args.force,
        read_back=args.read_back,
        read_memory=args.read_memory,
    )


if __name__ == "__main__":
    main()
