"""Latency HAProxy adds per streamed chunk with and without the stream observer.

Isolates the HAProxy side of the metrics from vLLM's noise. Backend processes
emulate vLLM replicas: every step (default 6.1 ms) each active stream on a
replica gets one SSE event in the same burst, written as one HTTP/1.1 chunk,
carrying the backend's CLOCK_MONOTONIC send time. Client processes keep many
keep-alive streams open and record, per event, receive time minus send time
(one-way latency through HAProxy). Arms alternate on the same binary and the
global/defaults that Ray renders (nbthread 4, 8 MiB buffers, splice-response,
http-no-delay); the observer arm adds the filter, per-thread Lua exporter and a
gRPC sink, and checks exported counts.

Usage: python haproxy_latency.py --binary DIST/haproxy [--reps 5] [--streams 256]
"""
import argparse
import asyncio
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np

parser = argparse.ArgumentParser()
parser.add_argument("--role", default="main", choices=["main", "backend", "client"])
parser.add_argument("--binary")
parser.add_argument("--reps", type=int, default=5)
parser.add_argument("--streams", type=int, default=256)
parser.add_argument("--replicas", type=int, default=2)
parser.add_argument("--clients", type=int, default=8)
parser.add_argument("--events", type=int, default=730, help="events per response")
parser.add_argument("--step-ms", type=float, default=6.1)
parser.add_argument("--text-bytes", type=int, default=6)
parser.add_argument("--duration", type=float, default=40.0)
parser.add_argument(
    "--window", default="10:35", help="measured receive-time window, seconds"
)
parser.add_argument("--out", default="/mnt/local_storage/sm/runs/haproxy_latency")
parser.add_argument(
    "--profile", action="store_true", help="dump HAProxy task profiling per arm"
)
parser.add_argument("--arms", default="off,observer")
parser.add_argument("--bufsize", type=int, default=8388608, help="Ray renders 8 MiB")
parser.add_argument(
    "--haproxy-env", default="", help="KEY=VALUE added to HAProxy's environment"
)
parser.add_argument(
    "--hot-size",
    type=int,
    default=0,
    help="tune.memory.hot-size (both arms); 0 = default",
)
parser.add_argument(
    "--lua-only",
    action="store_true",
    help="observer arm loads Lua but declares no filter",
)
# Internal.
parser.add_argument("--port", type=int)
parser.add_argument("--url")
parser.add_argument("--n", type=int)
parser.add_argument("--start-ns", type=int)
parser.add_argument("--result")
args = parser.parse_args()

IDENTITY = '{"model_name":"m","ReplicaId":"r%d","engine_worker_id":"w%d","engine":"0"}'


def backend(port, replica):
    """One emulated replica: a single step loop writes one event to every active stream."""
    streams = {}
    payload = b"x" * args.text_bytes
    head = (
        b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n"
        b"x-ray-llm-metric-tags: "
        + (IDENTITY % (replica, replica)).encode()
        + b"\r\n\r\n"
    )

    async def handle(reader, writer):
        try:
            while True:
                request = await reader.readuntil(b"\r\n\r\n")
                now = time.monotonic_ns()
                lines = request.split(b"\r\n")
                rid = lines[0].split()[1].rsplit(b"/", 1)[1]
                sent = next(
                    int(l.split(b":", 1)[1])
                    for l in lines
                    if l.lower().startswith(b"x-t:")
                )
                writer.write(head)
                done = asyncio.get_running_loop().create_future()
                streams[writer] = [rid, args.events, (now - sent) // 1000, done]
                await done
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            streams.pop(writer, None)
            writer.close()

    async def step():
        tick = time.monotonic()
        while True:
            tick += args.step_ms / 1000
            delay = tick - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            for writer, state in list(streams.items()):
                rid, left, request_us, done = state
                if writer.is_closing():  # Peer went away (e.g. HAProxy soft-stop).
                    del streams[writer]
                    done.set_result(None)
                    continue
                if left == 0:
                    writer.write(b"e\r\ndata: [DONE]\n\n\r\n0\r\n\r\n")
                    del streams[writer]
                    done.set_result(None)
                    continue
                extra = b',"q":%d' % request_us if left == args.events else b""
                event = (
                    b'data: {"id":"%s","choices":[{"index":0,"text":"%s","ray_token_count":1}],"t":%d%s}\n\n'
                    % (rid, payload, time.monotonic_ns(), extra)
                )
                writer.write(b"%x\r\n%s\r\n" % (len(event), event))
                state[1] = left - 1

    async def main():
        server = await asyncio.start_server(handle, "127.0.0.1", port)
        async with server:
            await step()

    asyncio.run(main())


def client():
    import aiohttp

    lo, hi = (float(x) for x in args.window.split(":"))
    start = args.start_ns
    window = (start + int(lo * 1e9), start + int(hi * 1e9))
    deadline = start + int(args.duration * 1e9)
    lat, first, request, tail = [], [], [], []

    async def stream(session, k):
        await asyncio.sleep(k * 4.5 / args.n)  # Stagger arrivals over one response.
        i = 0
        while time.monotonic_ns() < deadline:
            i += 1
            async with session.get(
                f"{args.url}/r/{os.getpid()}-{k}-{i}",
                headers={"x-t": str(time.monotonic_ns())},
            ) as resp:
                buf = b""
                is_first = True
                async for chunk in resp.content.iter_any():
                    now = time.monotonic_ns()
                    buf += chunk
                    while True:
                        end = buf.find(b"\n\n")
                        if end < 0:
                            break
                        event, buf = buf[:end], buf[end + 2 :]
                        p = event.find(b'"t":')
                        if p < 0:
                            continue
                        q = p + 4
                        while event[q : q + 1].isdigit():
                            q += 1
                        if not window[0] <= now <= window[1]:
                            is_first = False
                            continue
                        us = (now - int(event[p + 4 : q])) / 1000
                        if is_first:
                            first.append(us)
                            r = event.find(b'"q":')
                            request.append(int(event[r + 4 : event.find(b"}", r)]))
                            is_first = False
                        else:
                            lat.append(us)
                            if us > 2000:
                                tail.append(((now - start) / 1e9, us))

    async def main():
        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=0),
            timeout=aiohttp.ClientTimeout(total=300),
        ) as session:
            await asyncio.gather(*(stream(session, k) for k in range(args.n)))

    asyncio.run(main())
    np.savez(
        args.result,
        lat=np.array(lat, np.float32),
        first=np.array(first, np.float32),
        request=np.array(request, np.float32),
        tail=np.array(tail, np.float64).reshape(-1, 2),
    )


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def cpu_seconds(pid):
    fields = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")


def minor_faults(pid):
    return int(open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()[7])


def run_arm(observer, rep, out, backend_ports, sink_port, tmp):
    import ray.serve._private.haproxy as haproxy
    from ray.serve._private.haproxy_stream_metrics import (
        render_stream_metrics_export_config,
    )

    socket_path = haproxy.stream_metrics_socket_path(str(tmp / f"m{rep}{observer}"))
    lua = (
        (Path(haproxy.__file__).parent / "stream_metrics.lua")
        .read_text()
        .replace(
            "@STREAM_METRICS_EXPORT_CONFIG@",
            render_stream_metrics_export_config(socket_path),
        )
    )
    (tmp / "metrics.lua").write_text(lua)
    port = free_port()
    servers = "\n".join(
        f"    server r{i} 127.0.0.1:{p}" for i, p in enumerate(backend_ports)
    )
    export = (
        f"""
frontend ray_stream_metrics_export
    bind {socket_path} mode 600 ray-metrics-export
    timeout client 2s
    http-request use-service lua.stream_metrics_status if {{ path /status }}
    default_backend ray_stream_metrics_agent
backend ray_stream_metrics_agent
    timeout connect 1s
    timeout server 1s
    server agent 127.0.0.1:{sink_port} proto h2
"""
        if observer
        else ""
    )
    profile = (
        f"""
    stats socket {tmp}/admin.sock mode 600 level admin
    profiling.tasks on"""
        if args.profile
        else ""
    )
    cfg = f"""global{profile}
    maxconn 4096
    nbthread 4
    tune.bufsize {args.bufsize}{f'''
    tune.memory.hot-size {args.hot_size}''' if args.hot_size else ''}{f'''
    lua-load-per-thread {tmp}/metrics.lua''' if observer else ''}
defaults
    mode http
    timeout connect 5s
    timeout client 3600s
    timeout server 3600s
    timeout http-keep-alive 90s
    option abortonclose
    option splice-request
    option splice-response
    option http-no-delay
    balance leastconn
frontend stream
    bind 127.0.0.1:{port}{'''
    filter ray_stream_metrics''' if observer and not args.lua_only else '''
    http-response del-header x-ray-llm-metric-tags'''}
    http-request set-var(txn.via_ingress_request_router) bool(true)
    http-request set-var(txn.ingress_request_router_latency_us) int(1)
    default_backend engine
backend engine
    http-reuse always
{servers}
{export}"""
    (tmp / "haproxy.cfg").write_text(cfg)
    log = open(tmp / "haproxy.log", "w")
    env = (
        dict(os.environ, **dict([args.haproxy_env.split("=", 1)]))
        if args.haproxy_env
        else None
    )
    proc = subprocess.Popen(
        [args.binary, "-db", "-f", str(tmp / "haproxy.cfg")],
        stdout=log,
        stderr=log,
        env=env,
    )
    time.sleep(1.0)
    assert proc.poll() is None, (tmp / "haproxy.log").read_text()
    start = time.monotonic_ns() + int(0.5e9)
    per = args.streams // args.clients
    clients = []
    for c in range(args.clients):
        result = tmp / f"client{c}.npz"
        clients.append(
            (
                result,
                subprocess.Popen(
                    [
                        "taskset",
                        "-c",
                        str(100 + c),
                        sys.executable,
                        __file__,
                        "--role",
                        "client",
                        "--url",
                        f"http://127.0.0.1:{port}",
                        "--n",
                        str(per),
                        "--start-ns",
                        str(start),
                        "--duration",
                        str(args.duration),
                        "--window",
                        args.window,
                        "--result",
                        str(result),
                    ]
                ),
            )
        )
    lo, hi = (float(x) for x in args.window.split(":"))
    time.sleep(max(0.0, (start - time.monotonic_ns()) / 1e9 + lo))
    cpu0, flt0 = cpu_seconds(proc.pid), minor_faults(proc.pid)
    time.sleep(hi - lo)
    cpu1, flt1 = cpu_seconds(proc.pid), minor_faults(proc.pid)
    rss_mb = (
        int(
            dict(l.split(":", 1) for l in open(f"/proc/{proc.pid}/status") if ":" in l)[
                "VmRSS"
            ].split()[0]
        )
        // 1024
    )
    if args.profile:
        with socket.socket(socket.AF_UNIX) as admin:
            admin.connect(str(tmp / "admin.sock"))
            admin.sendall(b"show profiling tasks; show pools\n")
            text = b"".join(iter(lambda: admin.recv(65536), b"")).decode()
        (out / f"profile_{'observer' if observer else 'off'}_{rep}.txt").write_text(
            text
        )
        print(text, flush=True)
    for _, p in clients:
        assert p.wait(timeout=args.duration + 120) == 0
    proc.send_signal(signal.SIGUSR1)
    code = proc.wait(timeout=30)
    log.close()
    data = [np.load(r) for r, _ in clients]
    lat = np.concatenate([d["lat"] for d in data])
    first = np.concatenate([d["first"] for d in data])
    request = np.concatenate([d["request"] for d in data])
    tail = np.concatenate([d["tail"] for d in data])
    if tail.size:
        # Cluster tail events (>2 ms) that occur within 20 ms of each other.
        times = np.sort(tail[:, 0])
        starts = times[np.r_[True, np.diff(times) > 0.02]]
        print(
            f"  tail events {len(times)} in {len(starts)} clusters; cluster start times (s): "
            + " ".join(f"{t:.3f}" for t in starts[:40]),
            flush=True,
        )
        if len(starts) > 2:
            gaps = np.diff(starts)
            print(
                f"  cluster gaps: median {np.median(gaps):.3f}s, frac within 0.95-1.05s "
                f"{np.mean((gaps > 0.95) & (gaps < 1.05)):.2f}; per-cluster max latency (ms): "
                + " ".join(
                    f"{tail[(tail[:, 0] >= t) & (tail[:, 0] < t + 0.02), 1].max() / 1000:.1f}"
                    for t in starts[:40]
                ),
                flush=True,
            )
    row = {
        "arm": "observer" if observer else "off",
        "rep": rep,
        "exit": code,
        "events": int(lat.size),
        "events_per_s": lat.size / (hi - lo),
        "lat_mean_us": float(lat.mean()),
        "lat_p50_us": float(np.percentile(lat, 50)),
        "lat_p90_us": float(np.percentile(lat, 90)),
        "lat_p99_us": float(np.percentile(lat, 99)),
        "lat_p999_us": float(np.percentile(lat, 99.9)),
        "first_mean_us": float(first.mean()),
        "first_p50_us": float(np.percentile(first, 50)),
        "request_mean_us": float(request.mean()),
        "request_p50_us": float(np.percentile(request, 50)),
        "haproxy_cores": (cpu1 - cpu0) / (hi - lo),
        "faults_per_s": (flt1 - flt0) / (hi - lo),
        "rss_mb": rss_mb,
    }
    print(
        json.dumps(
            {k: round(v, 2) if isinstance(v, float) else v for k, v in row.items()}
        ),
        flush=True,
    )
    return row


def main():
    import grpc
    from ray.core.generated import reporter_pb2, reporter_pb2_grpc

    class Sink(reporter_pb2_grpc.ReporterServiceServicer):
        def __init__(self):
            self.lock = threading.Lock()
            self.latest = {}

        def ReportOCMetrics(self, request, context):
            with self.lock:
                for metric in request.metrics:
                    for series in metric.timeseries:
                        point = series.points[0]
                        self.latest[
                            (
                                metric.metric_descriptor.name,
                                tuple(v.value for v in series.label_values),
                            )
                        ] = (
                            point.int64_value
                            if "errors" in metric.metric_descriptor.name
                            else point.distribution_value.count
                        )
            return reporter_pb2.ReportOCMetricsReply()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    tmp = Path(tempfile.mkdtemp(prefix="lat-", dir="/mnt/local_storage/sm"))
    sink_impl = Sink()
    sink = grpc.server(ThreadPoolExecutor(max_workers=4))
    reporter_pb2_grpc.add_ReporterServiceServicer_to_server(sink_impl, sink)
    sink_port = sink.add_insecure_port("127.0.0.1:0")
    sink.start()
    ports = [free_port() for _ in range(args.replicas)]
    backends = [
        subprocess.Popen(
            [
                "taskset",
                "-c",
                str(96 + i),
                sys.executable,
                __file__,
                "--role",
                "backend",
                "--port",
                str(p),
                "--n",
                str(i),
                "--events",
                str(args.events),
                "--step-ms",
                str(args.step_ms),
                "--text-bytes",
                str(args.text_bytes),
            ]
        )
        for i, p in enumerate(ports)
    ]
    time.sleep(1.0)
    rows = []
    try:
        for rep in range(args.reps):
            arms = [a == "observer" for a in args.arms.split(",")]
            for observer in arms if rep % 2 == 0 else arms[::-1]:
                rows.append(run_arm(observer, rep, out, ports, sink_port, tmp))
    finally:
        for b in backends:
            b.kill()
        sink.stop(0)
    with sink_impl.lock:
        exported = {}
        for (name, _), v in sink_impl.latest.items():
            exported[name] = exported.get(name, 0) + v
    (out / "rows.json").write_text(json.dumps(rows, indent=1))
    print("exported totals (last observer arm generation):", exported)
    print("\nper-rep observer minus off (us):")
    for rep in range(args.reps):
        on = next((r for r in rows if r["rep"] == rep and r["arm"] == "observer"), None)
        off = next((r for r in rows if r["rep"] == rep and r["arm"] == "off"), None)
        if not on or not off:
            continue
        print(
            f"  rep {rep}: "
            + "  ".join(
                f"{k[:-3]} {on[k] - off[k]:+7.1f}"
                for k in (
                    "lat_mean_us",
                    "lat_p50_us",
                    "lat_p90_us",
                    "lat_p99_us",
                    "first_mean_us",
                    "request_mean_us",
                )
            )
            + f"  cores {off['haproxy_cores']:.2f}->{on['haproxy_cores']:.2f}"
            + f"  ev/s {off['events_per_s']:.0f}/{on['events_per_s']:.0f}"
        )
    import shutil

    shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    if args.role == "backend":
        backend(args.port, args.n)
    elif args.role == "client":
        client()
    else:
        main()
