"""Real HTTP S3 range reads, including planning sampling and worker execution."""

import io
import json
import logging
import random
import statistics
import threading
import time
from contextlib import nullcontext
from typing import Any, Dict, List, Tuple, cast
from unittest.mock import patch

import boto3
import pyarrow as pa
import pyarrow.dataset as pds
import pyarrow.fs as pafs
import pyarrow.orc as orc
import pytest
from moto.moto_server.werkzeug_app import (
    DomainDispatcherApplication,
    create_backend_app,
)
from werkzeug.serving import make_server

import ray
from ray.data._internal.datasource_v2.common.file_reader import FileFormat
from ray.data._internal.datasource_v2.common.size_estimators import (
    SamplingInMemorySizeEstimator,
)
from ray.data._internal.datasource_v2.formats.orc.orc_file_reader import OrcFileReader
from ray.data._internal.datasource_v2.formats.orc.orc_scanner import OrcScanner
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data.expressions import col, lit

logger = logging.getLogger(__name__)


@pytest.fixture
def measured_s3(monkeypatch, request):
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    delay_seconds = getattr(request, "param", 0.0)
    records = []
    phase = ["setup"]
    idle = threading.Condition()
    pending = [0]
    failure = {"remaining": 0}
    application = DomainDispatcherApplication(create_backend_app)

    def recorder(environ, start_response):
        with idle:
            pending[0] += 1
        entry = {
            "phase": phase[0],
            "method": environ["REQUEST_METHOD"],
            "path": environ["PATH_INFO"],
            "range": environ.get("HTTP_RANGE"),
            "bytes": 0,
            "delay_seconds": delay_seconds if phase[0] != "setup" else 0.0,
        }
        if entry["delay_seconds"]:
            time.sleep(entry["delay_seconds"])

        def response(status, headers, exc_info=None):
            entry["status"] = int(status.split()[0])
            return start_response(status, headers, exc_info)

        with idle:
            inject = (
                failure["remaining"] > 0
                and failure.get("phase") == entry["phase"]
                and entry["method"] == "GET"
                and entry["range"]
                and entry["path"].endswith("candidate.orc")
            )
            if inject:
                failure["remaining"] -= 1
        if inject:
            body = b"<Error><Code>ServiceUnavailable</Code><Message>Test transient</Message></Error>"
            response(
                "503 Service Unavailable",
                [
                    ("Content-Type", "application/xml"),
                    ("Content-Length", str(len(body))),
                ],
            )
            iterable = [body]
        else:
            iterable = application(environ, response)
        try:
            for chunk in iterable:
                entry["bytes"] += len(chunk)
                yield chunk
        finally:
            if hasattr(iterable, "close"):
                iterable.close()
            records.append(entry)
            with idle:
                pending[0] -= 1
                idle.notify_all()

    server = make_server("127.0.0.1", 0, recorder, threaded=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    endpoint = f"http://127.0.0.1:{server.server_port}"
    # Local-only test credentials; no ambient user credentials or storage.
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )
    client.create_bucket(Bucket="orc-statistics")
    filesystem = pafs.S3FileSystem(
        access_key="testing",
        secret_key="testing",
        region="us-east-1",
        endpoint_override=endpoint,
        scheme="http",
        allow_bucket_creation=False,
    )
    try:

        def wait_for_responses():
            with idle:
                assert idle.wait_for(lambda: pending[0] == 0, timeout=10)

        yield client, filesystem, records, phase, wait_for_responses, failure
    finally:
        server.shutdown()
        thread.join(timeout=10)
        server.server_close()


def _run_s3_matrix(
    measured_s3,
    codec,
    trace_application=False,
    inject_failure=False,
    small_files=False,
    extended_predicate=False,
):
    regions = {}

    def classify(path, offset, size):
        layout = regions.get("/" + path.lstrip("/"))
        if layout is None or offset is None:
            return {"stripe_region_bytes": 0, "tail_bytes": 0, "other_bytes": size}
        stripes, tail_start, file_size = layout

        def overlap(start, end):
            return max(0, min(offset + size, end) - max(offset, start))

        stripe_bytes = sum(overlap(start, end) for start, end, _ in stripes)
        tail_bytes = overlap(tail_start, file_size)
        return {
            "stripe_region_bytes": stripe_bytes,
            "tail_bytes": tail_bytes,
            "other_bytes": size - stripe_bytes - tail_bytes,
        }

    def counted(filesystem, callback):
        if not trace_application:
            return callback(filesystem), None
        from fsspec.implementations.arrow import ArrowFSWrapper

        from ray.data._internal.datasource_v2.formats.orc import orc_metadata

        events = []
        opens = []
        purpose = threading.local()

        class File(io.RawIOBase):
            def __init__(self, source, path):
                self.source = source
                self.path = path

            def readable(self):
                return True

            def seekable(self):
                return True

            def seek(self, *args):
                return self.source.seek(*args)

            def tell(self):
                return self.source.tell()

            def read(self, size=-1):
                offset = self.tell()
                data = self.source.read(size)
                events.append(
                    (getattr(purpose, "name", "decoder"), self.path, offset, len(data))
                )
                return data

            def readinto(self, buffer):
                data = self.read(len(buffer))
                buffer[: len(data)] = data
                return len(data)

            def close(self):
                if not self.closed:
                    self.source.close()
                super().close()

        class Filesystem(ArrowFSWrapper):
            def _open(self, *args, **kwargs):
                opens.append(args[0])
                return File(super()._open(*args, **kwargs), args[0])

        original_read = orc_metadata._read

        def metadata_read(source, offset: int, length: int) -> bytes:
            purpose.name = "statistics"
            try:
                return original_read(source, offset, length)
            finally:
                purpose.name = "decoder"

        orc_metadata._read = metadata_read
        try:
            observed = pafs.PyFileSystem(pafs.FSSpecHandler(Filesystem(filesystem)))
            result = callback(observed)
        finally:
            orc_metadata._read = original_read
        metrics: Dict[str, Any] = {}
        for name in ("statistics", "decoder"):
            reads = [event for event in events if event[0] == name]
            metrics[name] = {
                "reads": len(reads),
                "bytes": sum(event[3] for event in reads),
            }
            classifications = [
                classify(path, offset, size) for _, path, offset, size in reads
            ]
            metrics[name].update(
                {
                    key: sum(value[key] for value in classifications)
                    for key in ("stripe_region_bytes", "tail_bytes", "other_bytes")
                }
            )
            assert (
                sum(
                    metrics[name][key]
                    for key in ("stripe_region_bytes", "tail_bytes", "other_bytes")
                )
                == metrics[name]["bytes"]
            )
        metrics["opens"] = len(opens)
        return result, metrics

    # Nested functions are serialized by value, so workers do not need pytest's
    # driver-only import path for this test module.
    def _worker_read(scanner, manifest):
        from dataclasses import replace

        streamed, decoded = [], []
        original_scan = OrcFileReader._scan_with_statistics
        original_file = orc.ORCFile

        def scan(self, fragment, *args, **kwargs):
            class Fragment:
                def __getattr__(self, name):
                    return getattr(fragment, name)

                def scanner(self, **scanner_kwargs):
                    native = fragment.scanner(**scanner_kwargs)

                    class Scanner:
                        def scan_batches(self):
                            streamed.append(fragment.path)
                            yield from native.scan_batches()

                    return Scanner()

            yield from original_scan(self, Fragment(), *args, **kwargs)

        class TrackingFile:
            def __init__(self, source):
                self.file = original_file(source)

            def read_stripe(self, index, columns=None):
                decoded.append(index)
                return self.file.read_stripe(index, columns=columns)

        def execute(filesystem):
            return [
                value
                for batch in replace(scanner, filesystem=filesystem)
                .create_reader()
                .read(manifest)
                for value in batch["x"].to_pylist()
            ]

        with (
            patch.object(orc, "ORCFile", TrackingFile)
            if extended_predicate
            else nullcontext()
        ):
            if extended_predicate:
                OrcFileReader._scan_with_statistics = scan
            try:
                rows, application = counted(scanner.filesystem, execute)
            finally:
                OrcFileReader._scan_with_statistics = original_scan
        if extended_predicate:
            assert application is not None
            application["streamed_files"] = streamed
            application["decoded_stripes"] = decoded
        return rows, application

    def _worker_plan(filesystem, manifest):
        def execute(observed):
            reader = OrcFileReader(format=FileFormat.ORC, filesystem=observed)
            return (
                SamplingInMemorySizeEstimator(reader)
                .estimate_in_memory_sizes(manifest)
                .tolist()
            )

        return counted(filesystem, execute)

    def _worker_discover(filesystem, manifest):
        def execute(observed):
            files = observed.get_file_info(pafs.FileSelector("orc-statistics"))
            schema = pds.dataset(
                manifest.paths.tolist(), filesystem=observed, format="orc"
            ).schema
            return len(files), schema.names

        return counted(filesystem, execute)

    client, filesystem, records, phase, wait_for_responses, failure = measured_s3
    rng = random.Random(66995)
    row_count, width = (64, 32) if small_files else (8192, 256)
    threshold = 56 if small_files else 7000
    payload = [rng.randbytes(width) for _ in range(row_count)]
    predicate = col("x") >= threshold
    if extended_predicate:
        assert trace_application
        predicate = col("flag").is_not_null() & ~(col("flag") != lit(True))
    expected_stripes = {}
    paths, sizes = [], []
    for name, values in (
        ("excluded", range(-row_count, 0)),
        ("candidate", range(row_count)),
    ):
        table = pa.table({"x": values, "payload": payload})
        if extended_predicate:
            table = table.append_column(
                "flag",
                pa.array(
                    [True if value >= threshold else None for value in values],
                    type=pa.bool_(),
                ),
            )
        sink = pa.BufferOutputStream()
        orc.write_table(
            table, sink, compression=codec, stripe_size=32768, batch_size=256
        )
        data = sink.getvalue().to_pybytes()
        stripe_count = orc.ORCFile(pa.BufferReader(data)).nstripes
        assert stripe_count == 1 if small_files else stripe_count > 2
        key = name + ".orc"
        from ray.data._internal.datasource_v2.formats.orc.orc_metadata import (
            read_metadata,
        )

        metadata = read_metadata(pa.BufferReader(data))
        if extended_predicate:
            offset, selected = 0, []
            for index, (_, _, rows) in enumerate(metadata.stripes):
                if table.slice(offset, rows).filter(predicate.to_pyarrow()).num_rows:
                    selected.append(index)
                offset += rows
            expected_stripes[name] = selected
            if name == "candidate":
                assert 0 < len(selected) < len(metadata.stripes)
        regions["/orc-statistics/" + key] = (
            metadata.stripes,
            metadata.metadata_offset,
            len(data),
        )
        client.put_object(Bucket="orc-statistics", Key=key, Body=data)
        paths.append("orc-statistics/" + key)
        sizes.append(len(data))
    manifest = FileManifest.construct_manifest(
        paths=paths, sizes=sizes, chunk_metadatas=[None, None]
    )
    read = ray.remote(_worker_read)
    plan = ray.remote(_worker_plan)
    discover = ray.remote(_worker_discover)
    metrics: Dict[str, Any] = {}
    for mode in ("off", "file", "stripe"):
        application: Dict[str, Any] = {}
        phase[0] = mode + ":discovery"
        discovered, application["discovery"] = ray.get(
            discover.remote(filesystem, manifest)
        )
        wait_for_responses()
        assert discovered == (2, table.schema.names)
        phase[0] = mode + ":sampling"
        planned, application["sampling"] = ray.get(plan.remote(filesystem, manifest))
        wait_for_responses()
        assert planned
        phase[0] = mode + ":execution"
        if mode == "stripe" and inject_failure:
            failure.update(remaining=1, phase=phase[0])
        scanner = OrcScanner(
            schema=table.schema,
            filesystem=filesystem,
            predicate=predicate,
            statistics_pruning=mode,
            batch_size=127,
        )
        rows, application["execution"] = ray.get(read.remote(scanner, manifest))
        wait_for_responses()
        assert rows == list(range(threshold, row_count))
        phases: Dict[str, Any] = {}
        for stage in ("discovery", "sampling", "execution"):
            entries = [
                record for record in records if record["phase"] == mode + ":" + stage
            ]
            phases[stage] = {
                "requests": len(entries),
                "bytes": sum(record["bytes"] for record in entries),
                "range_gets": sum(
                    record["method"] == "GET" and bool(record["range"])
                    for record in entries
                ),
            }
            classifications = []
            for entry in entries:
                offset = None
                if entry["method"] == "GET" and entry["status"] in (200, 206):
                    if entry["range"] and entry["range"].startswith("bytes="):
                        start = entry["range"][6:].split("-")[0]
                        offset = int(start) if start.isdigit() else None
                    elif not entry["range"]:
                        offset = 0
                classifications.append(classify(entry["path"], offset, entry["bytes"]))
            phases[stage].update(
                {
                    key: sum(value[key] for value in classifications)
                    for key in ("stripe_region_bytes", "tail_bytes", "other_bytes")
                }
            )
            assert (
                sum(
                    phases[stage][key]
                    for key in ("stripe_region_bytes", "tail_bytes", "other_bytes")
                )
                == phases[stage]["bytes"]
            )
            assert phases[stage]["range_gets"] > 0
        phases["planning"] = {
            name: phases["discovery"][name] + phases["sampling"][name]
            for name in ("requests", "bytes", "range_gets")
        }
        phases["total_bytes"] = (
            phases["planning"]["bytes"] + phases["execution"]["bytes"]
        )
        metrics[mode] = phases
        if trace_application:
            phases["application"] = application
            sampling = application["sampling"]
            execution = application["execution"]
            assert sampling is not None and execution is not None
            assert sampling["decoder"]["bytes"] > 0
            assert sampling["statistics"]["bytes"] == 0
            assert (execution["statistics"]["bytes"] == 0) == (mode == "off")
            assert execution["statistics"]["stripe_region_bytes"] == 0
        if extended_predicate:
            execution = application["execution"]
            assert execution is not None
            if mode == "off":
                assert sorted(execution["streamed_files"]) == sorted(paths)
            elif mode == "file":
                assert execution["streamed_files"] == [paths[1]]
            else:
                assert execution["streamed_files"] == []
            assert execution["decoded_stripes"] == (
                expected_stripes["candidate"] if mode == "stripe" else []
            )
    # Planning sampling is deliberately counted, not mislabeled as zero-decode.
    assert all(value["planning"]["bytes"] > 0 for value in metrics.values())
    if not small_files:
        assert (
            metrics["file"]["execution"]["bytes"] < metrics["off"]["execution"]["bytes"]
        )
        assert (
            metrics["stripe"]["execution"]["bytes"]
            < metrics["file"]["execution"]["bytes"]
        )
        assert metrics["file"]["total_bytes"] < metrics["off"]["total_bytes"]
    logger.warning("ORC_S3_METRICS %s %s", codec, json.dumps(metrics, sort_keys=True))
    if inject_failure:
        failed = [record for record in records if record["status"] == 503]
        assert len(failed) == 1 and failure["remaining"] == 0
        assert any(
            record["status"] == 206
            and record["range"] == failed[0]["range"]
            and record["path"] == failed[0]["path"]
            and record["phase"] == failed[0]["phase"]
            for record in records
        )
    return metrics


@pytest.mark.parametrize("codec", ["uncompressed", "zlib", "snappy", "lz4", "zstd"])
def test_orc_s3_planning_and_execution(
    ray_start_regular_shared, measured_s3, codec, record_property
):
    metrics = _run_s3_matrix(measured_s3, codec)
    record_property("orc_s3_metrics", json.dumps(metrics, sort_keys=True))


def test_orc_s3_application_accounting(
    ray_start_regular_shared, measured_s3, record_property
):
    metrics = _run_s3_matrix(measured_s3, "zlib", trace_application=True)
    record_property("orc_s3_application_metrics", json.dumps(metrics, sort_keys=True))


def test_orc_s3_boolean_null_and_not_pruning(
    ray_start_regular_shared, measured_s3, record_property
):
    metrics = _run_s3_matrix(
        measured_s3, "zlib", trace_application=True, extended_predicate=True
    )
    record_property("orc_s3_extended_metrics", json.dumps(metrics, sort_keys=True))


def test_orc_s3_transient_range_failure(
    ray_start_regular_shared, measured_s3, record_property
):
    metrics = _run_s3_matrix(
        measured_s3, "zlib", trace_application=True, inject_failure=True
    )
    record_property("orc_s3_retry_metrics", json.dumps(metrics, sort_keys=True))


def test_orc_s3_small_file_costs(
    ray_start_regular_shared, measured_s3, record_property
):
    # Small files may cost more requests than they save. Retain this negative
    # performance evidence rather than requiring a universal speedup.
    metrics = _run_s3_matrix(
        measured_s3, "zlib", trace_application=True, small_files=True
    )
    record_property("orc_s3_small_file_metrics", json.dumps(metrics, sort_keys=True))


def _measure_default_pruning_costs(measured_s3, workload, delay_seconds):
    """Compare native S3 reads without requiring a timing-dependent speedup."""
    from ray.data._internal.datasource_v2.formats.orc import orc_file_reader

    def plan(filesystem, manifest):
        prefix = manifest.paths.tolist()[0].rsplit("/", 1)[0]
        files = filesystem.get_file_info(pafs.FileSelector(prefix))
        schema = pds.dataset(
            manifest.paths.tolist(), filesystem=filesystem, format="orc"
        ).schema
        reader = OrcFileReader(format=FileFormat.ORC, filesystem=filesystem)
        sizes = SamplingInMemorySizeEstimator(reader).estimate_in_memory_sizes(manifest)
        return len(files), schema.names, sizes.tolist()

    def read(scanner, manifest, expected):
        if scanner is None:
            return
        streamed, statistics_reads = [], []
        original_scan = OrcFileReader._scan_with_statistics
        original_metadata = orc_file_reader.read_metadata

        def metadata(source):
            statistics_reads.append(True)
            return original_metadata(source)

        def scan(self, fragment, *args, **kwargs):
            class Fragment:
                def __getattr__(self, name):
                    return getattr(fragment, name)

                def scanner(self, **scanner_kwargs):
                    native = fragment.scanner(**scanner_kwargs)

                    class Scanner:
                        def scan_batches(self):
                            streamed.append(fragment.path)
                            yield from native.scan_batches()

                    return Scanner()

            yield from original_scan(self, Fragment(), *args, **kwargs)

        OrcFileReader._scan_with_statistics = scan
        orc_file_reader.read_metadata = metadata
        try:
            tables = list(scanner.create_reader().read(manifest))
            actual = pa.concat_tables(tables)
            assert actual.sort_by(
                [("x", "ascending"), ("payload", "ascending")]
            ).equals(expected)
        finally:
            OrcFileReader._scan_with_statistics = original_scan
            orc_file_reader.read_metadata = original_metadata
        return streamed, len(statistics_reads)

    client, filesystem, records, phase, wait_for_responses, _ = measured_s3
    file_count, row_count, width = {
        "retained": (2, 8192, 128),
        "low_pruning": (10, 1024, 128),
        "small_files": (8, 64, 32),
    }[workload]
    # Cost inputs must not enter the bucket listed by the other S3 tests.
    bucket = f"orc-cost-{workload}-{int(delay_seconds * 1000)}".replace("_", "-")
    client.create_bucket(Bucket=bucket)
    rng = random.Random(66995)
    paths, sizes, tables = [], [], []
    for index in range(file_count):
        excluded = workload == "low_pruning" and index == 0
        values = range(-row_count, 0) if excluded else range(row_count)
        table = pa.table(
            {"x": values, "payload": [rng.randbytes(width) for _ in range(row_count)]}
        )
        sink = pa.BufferOutputStream()
        orc.write_table(table, sink, compression="zlib", stripe_size=32768)
        data = sink.getvalue().to_pybytes()
        # Moto's backend can outlive an HTTP fixture; isolate parameter inputs.
        key = f"cost-{workload}-{int(delay_seconds * 1000)}/part-{index:03}.orc"
        client.put_object(Bucket=bucket, Key=key, Body=data)
        paths.append(bucket + "/" + key)
        sizes.append(len(data))
        tables.append(table)
    predicate = col("x") >= 0
    expected = (
        pa.concat_tables(tables)
        .filter(predicate.to_pyarrow())
        .sort_by([("x", "ascending"), ("payload", "ascending")])
    )
    expected_ref = ray.put(expected)
    manifest = FileManifest.construct_manifest(
        paths=paths, sizes=sizes, chunk_metadatas=[None] * file_count
    )
    plan_remote, read_remote = ray.remote(plan), ray.remote(read)
    # Initialize the worker before timing; no extra object-store reads.
    ray.get(read_remote.remote(None, None, None))
    samples = []
    for repetition in range(3):
        for mode in ("off", "file") if repetition % 2 == 0 else ("file", "off"):
            sample: Dict[str, Any] = {"mode": mode, "repetition": repetition}
            for stage in ("planning", "execution"):
                label = f"cost:{repetition}:{mode}:{stage}"
                phase[0] = label
                started = time.perf_counter()
                if stage == "planning":
                    discovered, names, estimated = ray.get(
                        plan_remote.remote(filesystem, manifest)
                    )
                    assert discovered == file_count and names == expected.schema.names
                    assert len(estimated) == file_count
                else:
                    scanner = OrcScanner(
                        schema=expected.schema,
                        filesystem=filesystem,
                        predicate=predicate,
                        statistics_pruning=mode,
                        batch_size=127,
                    )
                    streamed, statistics_reads = cast(
                        Tuple[List[str], int],
                        ray.get(read_remote.remote(scanner, manifest, expected_ref)),
                    )
                    kept = paths[1:] if workload == "low_pruning" else paths
                    assert sorted(streamed) == sorted(paths if mode == "off" else kept)
                    assert statistics_reads == (0 if mode == "off" else file_count)
                    sample["streamed_files"] = streamed
                    sample["statistics_reads"] = statistics_reads
                wait_for_responses()
                elapsed = time.perf_counter() - started
                entries = [entry for entry in records if entry["phase"] == label]
                assert entries and all(entry["status"] < 400 for entry in entries)
                assert all(entry["delay_seconds"] == delay_seconds for entry in entries)
                sample[stage] = {
                    "seconds": elapsed,
                    "requests": len(entries),
                    "head_requests": sum(
                        entry["method"] == "HEAD" for entry in entries
                    ),
                    "range_gets": sum(
                        entry["method"] == "GET" and bool(entry["range"])
                        for entry in entries
                    ),
                    "bytes": sum(entry["bytes"] for entry in entries),
                }
            sample["total"] = {
                key: sample["planning"][key] + sample["execution"][key]
                for key in sample["planning"]
            }
            samples.append(sample)
    summary = {}
    for mode in ("off", "file"):
        group = [sample for sample in samples if sample["mode"] == mode]
        summary[mode] = {
            stage: {
                key: {
                    "median": statistics.median(sample[stage][key] for sample in group),
                    "min": min(sample[stage][key] for sample in group),
                    "max": max(sample[stage][key] for sample in group),
                }
                for key in group[0][stage]
            }
            for stage in ("planning", "execution", "total")
        }
    return {
        "workload": workload,
        "delay_seconds": delay_seconds,
        "file_sizes": sizes,
        "expected_rows": expected.num_rows,
        "samples": samples,
        "summary": summary,
    }


@pytest.mark.parametrize("workload", ["retained", "low_pruning", "small_files"])
@pytest.mark.parametrize(
    "measured_s3,delay_seconds",
    [(0.0, 0.0), (0.05, 0.05)],
    indirect=["measured_s3"],
    ids=["local", "50ms"],
)
def test_orc_s3_default_pruning_costs(
    ray_start_regular_shared, measured_s3, workload, delay_seconds, record_property
):
    metrics = _measure_default_pruning_costs(measured_s3, workload, delay_seconds)
    record_property("orc_s3_default_costs", json.dumps(metrics, sort_keys=True))
    logger.warning("ORC_S3_DEFAULT_COSTS %s", json.dumps(metrics, sort_keys=True))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
