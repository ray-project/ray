"""Stress the HAProxy stream observer through real HTX callbacks.

A raw asyncio backend sends vLLM-shaped SSE with adversarial fragmentation,
large events, marker-like escaped text, untagged/non-200/non-SSE responses.
Clients reuse keep-alive connections and optionally abort mid-stream. Checks:
  * every completed body is byte-identical to what the backend sent;
  * exported TTFT/ITL/request/routing counts equal the backend's ground truth
    (phase without aborts), with zero collection errors;
  * HAProxy stays alive, retires cleanly, and (ASan build) reports no errors.

Usage: python stress_haproxy.py --binary DIST/haproxy --requests N --aborts
"""
import argparse
import asyncio
import hashlib
import json
import os
import random
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import aiohttp
import grpc

parser = argparse.ArgumentParser()
parser.add_argument("--binary", required=True)
parser.add_argument("--requests", type=int, default=2000)
parser.add_argument("--concurrency", type=int, default=64)
parser.add_argument("--aborts", action="store_true")
parser.add_argument("--nbthread", type=int, default=4)
parser.add_argument("--seed", type=int, default=1)
parser.add_argument(
    "--bufsize", type=int, default=0, help="tune.bufsize (0 = HAProxy default)"
)
parser.add_argument(
    "--no-filter", action="store_true", help="baseline without the observer"
)
parser.add_argument(
    "--rounds", type=int, default=1, help="repeat the client load, sampling RSS"
)
parser.add_argument(
    "--events", type=int, default=0, help="fixed events per response (vLLM-like)"
)
parser.add_argument(
    "--pace-ms", type=float, default=0.0, help="delay between events when --events"
)
parser.add_argument("--keep-logs", action="store_true")
args = parser.parse_args()
os.environ["RAY_SERVE_HAPROXY_BINARY_PATH"] = args.binary

from ray.core.generated import reporter_pb2, reporter_pb2_grpc  # noqa: E402
import ray.serve._private.haproxy as haproxy  # noqa: E402
from ray.serve._private.haproxy_stream_metrics import (  # noqa: E402
    render_stream_metrics_export_config,
)

rng = random.Random(args.seed)
IDENTITIES = [
    json.dumps(
        {
            "model_name": "m",
            "ReplicaId": f"r{i}",
            "engine_worker_id": f"w{i}",
            "engine": "0",
        },
        separators=(",", ":"),
    )
    for i in range(3)
]
NOISE = [
    '\\"ray_token_count\\":7',
    '\\"choices\\":[{\\"index\\":3',
    '\\"ray_tok',
    "ray_token_count",
    "\\\\",
    "123",
    '\\"',
    ":[{",
    "\\u0022",
    "x",
    "\\n\\n",
    "é",
]


def make_response(request_id):
    """Returns (headers, body pieces, expected counts or None)."""
    kind = rng.random()
    tagged = kind > 0.05
    status = 500 if 0.05 < kind <= 0.10 else 200
    sse = not (0.10 < kind <= 0.15)
    identity = rng.choice(IDENTITIES)
    events = []
    first_seen = set()
    ttft = itl = 0
    for _ in range(args.events or rng.randint(0, 60)):
        index = 0 if args.events else rng.choice([0, 0, 0, 1, 2, 5])
        count = 1 if args.events else rng.choice([0, 1, 1, 1, 2, 3, rng.randint(0, 50)])
        text = (
            "x" * 40
            if args.events
            else "".join(rng.choice(NOISE) for _ in range(rng.randint(0, 6)))
        )
        if not args.events and rng.random() < 0.05:
            text += "y" * rng.randint(1000, 40000)  # spans HTX buffers
        events.append(
            "data: "
            + json.dumps(
                {
                    "id": request_id,
                    "choices": [
                        {"index": index, "text": "TEXT", "ray_token_count": count}
                    ],
                },
                separators=(",", ":"),
            ).replace("TEXT", text)
            + "\n\n"
        )
        if count:
            if index in first_seen:
                itl += count
            else:
                first_seen.add(index)
                ttft += 1
    events.append(
        'data: {"id":"%s","choices":[],"usage":{"completion_tokens":1}}\n\n'
        % request_id
    )
    events.append("data: [DONE]\n\n")
    body = "".join(events).encode()
    headers = [
        f"HTTP/1.1 {status} {'OK' if status == 200 else 'Internal Server Error'}",
        "content-type: " + ("text/event-stream" if sse else "application/json"),
        "transfer-encoding: chunked",
    ]
    if tagged:
        headers.append("x-ray-llm-metric-tags: " + identity)
    observed = tagged and status == 200 and sse
    expected = {"ttft": ttft, "itl": itl, "req": 1} if observed else None
    return "\r\n".join(headers).encode() + b"\r\n\r\n", body, expected


EXPECTED = {}
SENT_HASH = {}


async def backend(reader, writer):
    try:
        while True:
            head = await reader.readuntil(b"\r\n\r\n")
            line = head.split(b"\r\n", 1)[0].decode()
            request_id = line.split()[1].rsplit("/", 1)[1]
            headers, body, expected = make_response(request_id)
            EXPECTED[request_id] = expected
            SENT_HASH[request_id] = hashlib.sha256(body).hexdigest()
            writer.write(headers)
            if args.events:
                # vLLM-like: one chunked write per SSE event, paced.
                for event in body.split(b"\n\n")[:-1]:
                    piece = event + b"\n\n"
                    writer.write(b"%x\r\n" % len(piece) + piece + b"\r\n")
                    await writer.drain()
                    if args.pace_ms:
                        await asyncio.sleep(args.pace_ms / 1000)
                writer.write(b"0\r\n\r\n")
                await writer.drain()
                continue
            offset = 0
            while offset < len(body):
                size = rng.choice([1, 2, 7, 33, 120, 600, 5000, 20000])
                piece = body[offset : offset + size]
                offset += size
                writer.write(b"%x\r\n" % len(piece) + piece + b"\r\n")
                if rng.random() < 0.3:
                    await writer.drain()
                    await asyncio.sleep(rng.choice([0, 0, 0.0005, 0.002]))
            writer.write(b"0\r\n\r\n")
            await writer.drain()
    except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError):
        pass
    finally:
        writer.close()


class Sink(reporter_pb2_grpc.ReporterServiceServicer):
    def __init__(self):
        self.latest = {}
        self.lock = threading.Lock()

    def ReportOCMetrics(self, request, context):
        with self.lock:
            for metric in request.metrics:
                name = metric.metric_descriptor.name
                for series in metric.timeseries:
                    key = (name, tuple(v.value for v in series.label_values))
                    point = series.points[0]
                    value = (
                        point.int64_value
                        if "collection_errors" in name
                        else point.distribution_value.count
                    )
                    self.latest[key] = value
        return reporter_pb2.ReportOCMetricsReply()

    def totals(self):
        with self.lock:
            out = {}
            for (name, _), value in self.latest.items():
                out[name] = out.get(name, 0) + value
            return out


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def client(session, url, request_id, abort):
    async with session.get(f"{url}/r/{request_id}") as response:
        digest = hashlib.sha256()
        limit = rng.randint(0, 3000) if abort else None
        read = 0
        async for chunk in response.content.iter_any():
            digest.update(chunk)
            read += len(chunk)
            if limit is not None and read >= limit:
                response.close()  # abort mid-stream
                return request_id, None, response.headers
        return request_id, digest.hexdigest(), response.headers


async def main():
    tmp = Path(tempfile.mkdtemp(prefix="sm-stress-", dir="/mnt/local_storage/sm"))
    server = await asyncio.start_server(backend, "127.0.0.1", 0)
    backend_port = server.sockets[0].getsockname()[1]
    sink_impl = Sink()
    sink = grpc.server(ThreadPoolExecutor(max_workers=4))
    reporter_pb2_grpc.add_ReporterServiceServicer_to_server(sink_impl, sink)
    sink_port = sink.add_insecure_port("127.0.0.1:0")
    sink.start()
    socket_path = haproxy.stream_metrics_socket_path(str(tmp / "metrics"))
    lua = (
        (Path(haproxy.__file__).parent / "stream_metrics.lua")
        .read_text()
        .replace(
            "@STREAM_METRICS_EXPORT_CONFIG@",
            render_stream_metrics_export_config(socket_path),
        )
    )
    (tmp / "metrics.lua").write_text(lua)
    http_port = free_port()
    tune = f"\n    tune.bufsize {args.bufsize}" if args.bufsize else ""
    lua_load = "" if args.no_filter else f"\n    lua-load-per-thread {tmp}/metrics.lua"
    flt = "" if args.no_filter else "\n    filter ray_stream_metrics"
    export = (
        ""
        if args.no_filter
        else f"""frontend ray_stream_metrics_export
    bind {socket_path} mode 600 ray-metrics-export
    timeout client 2s
    http-request use-service lua.stream_metrics_status if {{ path /status }}
    default_backend ray_stream_metrics_agent
backend ray_stream_metrics_agent
    timeout connect 1s
    timeout server 1s
    server agent 127.0.0.1:{sink_port} proto h2
"""
    )
    (tmp / "haproxy.cfg").write_text(
        f"""global{lua_load}
    nbthread {args.nbthread}{tune}
defaults
    mode http
    timeout connect 2s
    timeout client 30s
    timeout server 30s
frontend stream
    bind 127.0.0.1:{http_port}{flt}
    http-request set-var(txn.via_ingress_request_router) bool(true)
    http-request set-var(txn.ingress_request_router_latency_us) int(1)
    default_backend engine
backend engine
    http-reuse always
    server engine 127.0.0.1:{backend_port}
{export}"""
    )
    log = open(tmp / "haproxy.log", "w")
    env = dict(
        os.environ, ASAN_OPTIONS="detect_leaks=0:halt_on_error=1:abort_on_error=1"
    )
    proc = subprocess.Popen(
        [args.binary, "-db", "-f", str(tmp / "haproxy.cfg")],
        stdout=log,
        stderr=log,
        env=env,
    )
    await asyncio.sleep(1.0)
    assert proc.poll() is None, (tmp / "haproxy.log").read_text()
    url = f"http://127.0.0.1:{http_port}"
    started = time.monotonic()
    results = []
    rss = []

    def sample_rss():
        fields = dict(
            l.split(":", 1) for l in open(f"/proc/{proc.pid}/status") if ":" in l
        )
        return (
            int(fields["VmRSS"].split()[0]) // 1024,
            int(fields["VmHWM"].split()[0]) // 1024,
        )

    for round_index in range(args.rounds):
        connector = aiohttp.TCPConnector(limit=args.concurrency)
        async with aiohttp.ClientSession(
            connector=connector, timeout=aiohttp.ClientTimeout(total=120)
        ) as session:
            sem = asyncio.Semaphore(args.concurrency)

            async def one(i):
                async with sem:
                    abort = args.aborts and rng.random() < 0.3
                    try:
                        results.append(
                            await client(session, url, f"q{round_index}_{i}", abort)
                        )
                    except aiohttp.ClientError as e:
                        results.append(
                            (f"q{round_index}_{i}", "error:" + type(e).__name__, {})
                        )

            await asyncio.gather(*(one(i) for i in range(args.requests)))
        rss.append(sample_rss())
        print(f"round {round_index}: rss/hwm MB {rss[-1]}", flush=True)
    elapsed = time.monotonic() - started
    mismatched = [
        r
        for r in results
        if r[1] is not None
        and not r[1].startswith("error")
        and r[1] != SENT_HASH.get(r[0])
    ]
    errors = [r for r in results if r[1] is not None and r[1].startswith("error")]
    leaked_header = [r for r in results if "x-ray-llm-metric-tags" in r[2]]
    completed = {
        r[0] for r in results if r[1] is not None and not r[1].startswith("error")
    }
    expected = {"ttft": 0, "itl": 0, "req": 0}
    for request_id in completed:
        e = EXPECTED.get(request_id)
        if e:
            for k in expected:
                expected[k] += e[k]
    # Soft-stop: the final flush must export everything, then the process exits 0.
    proc.send_signal(signal.SIGUSR1)
    try:
        code = proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        proc.kill()
        code = "timeout"
    await asyncio.sleep(0.5)
    totals = sink_impl.totals()
    got = {
        "ttft": totals.get("serve_llm_time_to_first_token_seconds", 0),
        "itl": totals.get("serve_llm_inter_token_latency_seconds", 0),
        "req": totals.get("serve_llm_request_duration_seconds", 0),
        "routing": totals.get("serve_llm_router_overhead_seconds", 0),
        "errors": totals.get("serve_llm_stream_metrics_collection_errors", 0),
    }
    log.close()
    text = (tmp / "haproxy.log").read_text()
    asan = "AddressSanitizer" in text or "runtime error" in text
    report = {
        "binary": args.binary,
        "requests": args.requests,
        "aborts": args.aborts,
        "elapsed_s": round(elapsed, 1),
        "completed": len(completed),
        "client_errors": len(errors),
        "body_mismatches": len(mismatched),
        "tag_header_leaks": len(leaked_header),
        "expected_completed_counts": expected,
        "exported": got,
        "exit_code": code,
        "sanitizer_report": asan,
        "log": str(tmp / "haproxy.log"),
        "rss_hwm_mb_per_round": rss,
    }
    exact = (
        got["ttft"] == expected["ttft"]
        and got["itl"] == expected["itl"]
        and got["req"] == expected["req"]
    )
    if args.aborts:
        # Aborted streams still count whatever completed before the abort.
        exact = (
            got["ttft"] >= expected["ttft"]
            and got["itl"] >= expected["itl"]
            and got["req"] >= expected["req"]
        )
    if args.no_filter:
        exact = True
        got["errors"] = 0
    report["ok"] = bool(
        exact
        and not mismatched
        and not leaked_header
        and code == 0
        and not asan
        and got["errors"] == 0
        and (args.aborts or not errors)
    )
    print(json.dumps(report, indent=1))
    sink.stop(0)
    server.close()
    if report["ok"] and not args.keep_logs:
        import shutil

        shutil.rmtree(tmp, ignore_errors=True)
    sys.exit(0 if report["ok"] else 1)


asyncio.run(main())
