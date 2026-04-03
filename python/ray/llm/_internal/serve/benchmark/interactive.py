"""Interactive server and client for the multi-turn benchmark.

The interactive server runs a long-lived benchmark loop whose QPS, workload
parameters, and measurement windows are controlled at runtime via a UNIX
domain socket.  The interactive client connects to that socket (either as an
interactive REPL or for one-shot commands).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import random
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

import aiohttp
import numpy as np

from ray.llm._internal.serve.benchmark.metrics import summarize_metrics
from ray.llm._internal.serve.benchmark.models import TurnMetric, WorkloadSpec
from ray.llm._internal.serve.benchmark.text_gen import (
    Conversation,
    TextGenerator,
    conversation_factory,
)
from ray.llm._internal.serve.benchmark.turn import execute_single_turn

try:
    from prompt_toolkit import PromptSession
    from prompt_toolkit.history import FileHistory
except ImportError:
    PromptSession = None  # type: ignore[assignment,misc]
    FileHistory = None  # type: ignore[assignment,misc]

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Control socket path
# ---------------------------------------------------------------------------
_DEFAULT_CONTROL_SOCKET = "/tmp/interactive_rate_bench.sock"


def _control_socket_path() -> str:
    return os.environ.get("RAY_BENCH_CONTROL_SOCKET", _DEFAULT_CONTROL_SOCKET)


# ---------------------------------------------------------------------------
# Process-pool worker helpers (module-level so they are picklable)
# ---------------------------------------------------------------------------
_worker_tokenizer = None
_worker_text_gen: Optional[TextGenerator] = None


def _pool_initializer(tokenizer_name: str, base_seed: int) -> None:
    """Called once per worker process to load the tokenizer and seed RNG."""
    global _worker_tokenizer, _worker_text_gen
    from transformers import AutoTokenizer

    _worker_tokenizer = AutoTokenizer.from_pretrained(
        tokenizer_name, trust_remote_code=True
    )
    _worker_text_gen = TextGenerator(_worker_tokenizer)
    proc_seed = (base_seed + os.getpid()) % (2**32)
    random.seed(proc_seed)
    np.random.seed(proc_seed)


def _create_conv_in_worker(
    session_idx: int,
    spec: WorkloadSpec,
    shared_system_text: str,
) -> Conversation:
    """Create a Conversation inside a worker process."""
    return conversation_factory(session_idx, spec, shared_system_text, _worker_text_gen)


# ============================================================================
# Interactive-mode runtime state & helpers
# ============================================================================


@dataclass
class RuntimeState:
    current_qps: float = 0.0
    total_completed: int = 0
    total_failed: int = 0
    inflight: int = 0
    measurement_active: bool = False
    measurement_start_ns: Optional[int] = None
    measurement_metrics: list[TurnMetric] = field(default_factory=list)
    measurement_target_requests: Optional[int] = None
    last_window_metrics: list[TurnMetric] = field(default_factory=list)
    last_window_elapsed_s: float = 0.0
    last_notice: Optional[str] = None
    save_dir: Optional[str] = None


def _save_window_result(
    path: str,
    args: argparse.Namespace,
    spec: WorkloadSpec,
    metrics: list[TurnMetric],
    elapsed_s: float,
    runtime_qps: float = 0.0,
) -> None:
    payload = {
        "mode": "interactive_rate",
        "saved_at_epoch_s": time.time(),
        "config": {
            "base_url": args.base_url,
            "model": args.model,
            "tokenizer": getattr(args, "tokenizer", None) or args.model,
            "first_chunk_threshold": args.first_chunk_threshold,
            "num_turns": args.num_turns,
            "osl": args.osl,
            "shared_system_prompt_ratio": args.shared_system_prompt_ratio,
            "isl": args.isl,
            "hit_rate": args.hit_rate,
            "runtime_qps": runtime_qps,
        },
        "spec": spec.summary(),
        "window": summarize_metrics(metrics, elapsed_s),
        "raw_metrics": [
            {
                "session_id": m.session_id,
                "turn": m.turn,
                "ttft_ms": round(m.ttft_ms, 2),
                "fc_ms": round(m.fc_ms, 2),
                "itl_ms": round(m.itl_ms, 2),
                "latency_ms": round(m.latency_ms, 2),
                "input_tokens": m.input_tokens,
                "output_tokens": m.output_tokens,
                "start_time_ms": round(m.start_time_ms, 2),
            }
            for m in metrics
        ],
    }
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w") as f:
        json.dump(payload, f, indent=2)
    print(f"Saved measurement window to {path}")


def _build_spec(
    args: argparse.Namespace, overrides: Optional[dict] = None
) -> WorkloadSpec:
    """Build and resolve a WorkloadSpec from args, optionally merging overrides."""
    kw = dict(
        num_sessions=1,
        duration_s=1.0,
        num_turns=args.num_turns,
        osl=args.osl,
        think_time=0.0,
        concurrency=None,
        request_rate=1.0,
        ramp_interval=0.0,
        shared_system_prompt_ratio=args.shared_system_prompt_ratio,
        isl=args.isl,
        hit_rate=args.hit_rate,
    )
    if overrides:
        kw.update(overrides)
    spec = WorkloadSpec(**kw)
    spec.resolve()
    return spec


# ============================================================================
# Command handler (extracted for testability)
# ============================================================================


class CommandHandler:
    """Handles interactive benchmark commands.

    Extracted from the ``run_interactive`` closure so that command parsing,
    state mutation, and response formatting can be unit-tested without
    starting a real server or HTTP session.
    """

    def __init__(
        self,
        runtime: RuntimeState,
        workload: dict,
        args: argparse.Namespace,
        text_gen: Optional[TextGenerator] = None,
        rate_changed: Optional[asyncio.Event] = None,
        workload_changed: Optional[asyncio.Event] = None,
        stop_event: Optional[asyncio.Event] = None,
    ):
        self.runtime = runtime
        self.workload = workload
        self.args = args
        self.text_gen = text_gen
        self.rate_changed = rate_changed or asyncio.Event()
        self.workload_changed = workload_changed or asyncio.Event()
        self.stop_event = stop_event or asyncio.Event()

    def resolve_save_path(self, raw: Optional[str]) -> str:
        if raw:
            expanded = str(Path(raw).expanduser())
            if "/" in expanded or expanded.startswith("."):
                return expanded
            return str(Path(self.runtime.save_dir) / expanded)

        if self.args.save_result:
            return str(Path(self.args.save_result).expanduser())

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        qps_label = f"{self.runtime.current_qps:.2f}".replace(".", "p")
        return str(
            Path(self.runtime.save_dir)
            / f"interactive_measure_qps{qps_label}_{ts}.json"
        )

    async def handle(self, cmd: str) -> str:  # noqa: C901
        """Process a single command string and return the response."""
        cmd = cmd.strip()
        if not cmd:
            return "empty command"

        parts = cmd.split()
        op = parts[0].lower()

        if op == "help":
            return (
                "Commands: help, rate <qps>, start, measure <n>, stop, "
                "status, save [path|name], save-dir <path>, quit\n"
                "Workload: workload [isl=N] [osl=N] [hit-rate=F] "
                "[sharing=F] [num-turns=N]\n"
                "  e.g.  workload isl=2000 osl=200 hit-rate=0.5\n"
                "  All params optional; unspecified ones keep their current values.\n"
                "  workload (no args) prints current workload spec."
            )
        if op == "rate":
            if len(parts) != 2:
                return "Usage: rate <qps>"
            try:
                new_qps = float(parts[1])
                if new_qps < 0:
                    raise ValueError()
            except ValueError:
                return "QPS must be a non-negative number."
            self.runtime.current_qps = new_qps
            self.rate_changed.set()
            return f"Set target qps={new_qps:.3f}"
        if op == "start":
            self.runtime.measurement_active = True
            self.runtime.measurement_start_ns = time.perf_counter_ns()
            self.runtime.measurement_metrics = []
            self.runtime.measurement_target_requests = None
            self.runtime.last_notice = None
            return "Measurement started."
        if op == "measure":
            if len(parts) != 2:
                return "Usage: measure <num_requests>"
            try:
                tgt = int(parts[1])
                if tgt <= 0:
                    raise ValueError()
            except ValueError:
                return "measure requires a positive integer."
            self.runtime.measurement_active = True
            self.runtime.measurement_start_ns = time.perf_counter_ns()
            self.runtime.measurement_metrics = []
            self.runtime.measurement_target_requests = tgt
            self.runtime.last_notice = None
            return f"Measurement started: capturing next {tgt} completed requests."
        if op == "stop":
            if not self.runtime.measurement_active:
                return "Measurement is not active."
            self.runtime.measurement_active = False
            end_ns = time.perf_counter_ns()
            start_ns = self.runtime.measurement_start_ns or end_ns
            self.runtime.last_window_elapsed_s = (end_ns - start_ns) / 1e9
            self.runtime.last_window_metrics = list(self.runtime.measurement_metrics)
            self.runtime.measurement_target_requests = None
            summary = summarize_metrics(
                list(self.runtime.last_window_metrics),
                self.runtime.last_window_elapsed_s,
            )
            return f"Measurement stopped.\n{json.dumps(summary, indent=2)}"
        if op == "status":
            cur = self.workload["spec"]
            status = (
                f"qps={self.runtime.current_qps:.2f} "
                f"inflight={self.runtime.inflight} "
                f"completed={self.runtime.total_completed} "
                f"failed={self.runtime.total_failed} "
                f"measured={len(self.runtime.measurement_metrics)} "
                f"active={self.runtime.measurement_active} "
                f"target={self.runtime.measurement_target_requests} "
                f"save_dir={self.runtime.save_dir}\n"
                f"workload: isl={cur.isl} osl={cur.osl} hit-rate={cur.hit_rate} "
                f"sharing={cur.shared_system_prompt_ratio} num-turns={cur.num_turns}"
            )
            if self.runtime.last_notice:
                status += f"\n{self.runtime.last_notice}"
                self.runtime.last_notice = None
            return status
        if op == "save-dir":
            if len(parts) != 2:
                return "Usage: save-dir <path>"
            new_dir = str(Path(parts[1]).expanduser())
            self.runtime.save_dir = new_dir
            return f"Set save_dir={self.runtime.save_dir}"
        if op == "save":
            if len(parts) > 2:
                return "Usage: save [path.json|name.json]"

            if (
                self.runtime.measurement_active
                and self.runtime.measurement_start_ns is not None
            ):
                el = (time.perf_counter_ns() - self.runtime.measurement_start_ns) / 1e9
                mlist = list(self.runtime.measurement_metrics)
            else:
                el = self.runtime.last_window_elapsed_s
                mlist = list(self.runtime.last_window_metrics)
            if not mlist:
                return "No measured window data to save."
            save_path = self.resolve_save_path(parts[1] if len(parts) == 2 else None)
            _save_window_result(
                save_path,
                self.args,
                self.workload["spec"],
                mlist,
                el,
                runtime_qps=self.runtime.current_qps,
            )
            return f"Saved measurement window to {save_path}"
        if op == "workload":
            cur = self.workload["spec"]
            if len(parts) == 1:
                return (
                    f"isl={cur.isl} osl={cur.osl} hit-rate={cur.hit_rate} "
                    f"sharing={cur.shared_system_prompt_ratio} num-turns={cur.num_turns}"
                )
            _param_aliases = {
                "isl": "isl",
                "osl": "osl",
                "hit-rate": "hit_rate",
                "hitrate": "hit_rate",
                "hit_rate": "hit_rate",
                "sharing": "shared_system_prompt_ratio",
                "shared-system-prompt-ratio": "shared_system_prompt_ratio",
                "shared_system_prompt_ratio": "shared_system_prompt_ratio",
                "num-turns": "num_turns",
                "num_turns": "num_turns",
            }
            overrides: dict = {}
            errors: list[str] = []
            for token in parts[1:]:
                if "=" not in token:
                    errors.append(f"bad token {token!r} (expected key=value)")
                    continue
                k, _, v = token.partition("=")
                mapped = _param_aliases.get(k.lower())
                if mapped is None:
                    errors.append(f"unknown param {k!r}")
                    continue
                try:
                    overrides[mapped] = (
                        int(v) if mapped in ("isl", "osl", "num_turns") else float(v)
                    )
                except ValueError:
                    errors.append(f"invalid value for {k}: {v!r}")
            if errors:
                return "Error: " + "; ".join(errors)
            merged = dict(
                isl=cur.isl,
                osl=cur.osl,
                hit_rate=cur.hit_rate,
                shared_system_prompt_ratio=cur.shared_system_prompt_ratio,
                num_turns=cur.num_turns,
            )
            merged.update(overrides)
            try:
                new_spec = _build_spec(self.args, merged)
            except Exception as e:
                return f"Invalid workload spec: {e}"
            if self.text_gen is not None:
                new_sst = self.text_gen.generate(new_spec.shared_s)
            else:
                new_sst = ""
            self.workload["spec"] = new_spec
            self.workload["shared_system_text"] = new_sst
            self.workload_changed.set()
            new_spec.print_summary()
            return (
                f"Workload updated: isl={new_spec.isl} osl={new_spec.osl} "
                f"hit-rate={new_spec.hit_rate} "
                f"sharing={new_spec.shared_system_prompt_ratio} "
                f"num-turns={new_spec.num_turns}"
            )
        if op in ("quit", "exit"):
            self.stop_event.set()
            return "Stopping benchmark..."
        return f"Unknown command: {op}"


# ============================================================================
# Interactive server
# ============================================================================


async def run_interactive(args: argparse.Namespace) -> None:
    spec = _build_spec(args)
    spec.print_summary()
    print("Interactive mode: starts idle. Use 'rate <qps>' to begin sending traffic.")

    from concurrent.futures import ProcessPoolExecutor

    from transformers import AutoTokenizer

    tokenizer_name: str = args.tokenizer if args.tokenizer else args.model

    if args.seed is None:
        args.seed = random.randint(0, 2**31 - 1)
    random.seed(args.seed)
    np.random.seed(args.seed % (2**32))
    print(f"Seed: {args.seed}")

    print(f"Loading tokenizer: {tokenizer_name}")
    tokenizer = AutoTokenizer.from_pretrained(tokenizer_name, trust_remote_code=True)
    text_gen = TextGenerator(tokenizer)

    shared_system_text = text_gen.generate(spec.shared_s)
    bench_start_ns = time.perf_counter_ns()

    workload: dict = {"spec": spec, "shared_system_text": shared_system_text}
    workload_changed = asyncio.Event()

    default_save_dir = args.save_dir
    if default_save_dir is None and args.save_result:
        default_save_dir = str(Path(args.save_result).parent)
    if default_save_dir is None:
        default_save_dir = os.getcwd()

    runtime = RuntimeState(
        current_qps=0.0,
        save_dir=str(Path(default_save_dir).expanduser()),
    )
    stop_event = asyncio.Event()
    rate_changed = asyncio.Event()
    ready_queue: asyncio.Queue[tuple[Conversation, int]] = asyncio.Queue()
    next_session_idx = 0
    running_tasks: set[asyncio.Task] = set()

    num_workers = args.num_workers
    print(f"Starting process pool with {num_workers} workers")
    cpu_pool = ProcessPoolExecutor(
        max_workers=num_workers,
        initializer=_pool_initializer,
        initargs=(tokenizer_name, args.seed),
    )
    loop = asyncio.get_running_loop()

    def _next_session_idx() -> int:
        nonlocal next_session_idx
        idx = next_session_idx
        next_session_idx += 1
        return idx

    async def next_conv_async() -> Conversation:
        idx = _next_session_idx()
        s = workload["spec"]
        sst = workload["shared_system_text"]
        return await loop.run_in_executor(
            cpu_pool,
            _create_conv_in_worker,
            idx,
            s,
            sst,
        )

    async def prefill_queue() -> None:
        while not stop_event.is_set():
            if workload_changed.is_set():
                workload_changed.clear()
                drained = 0
                while not ready_queue.empty():
                    try:
                        ready_queue.get_nowait()
                        drained += 1
                    except asyncio.QueueEmpty:
                        break
                if drained:
                    print(
                        f"[workload] drained {drained} stale conversations from queue.",
                        flush=True,
                    )

            qps = runtime.current_qps
            if qps <= 0:
                await asyncio.sleep(0.2)
                continue
            s = workload["spec"]
            sst = workload["shared_system_text"]
            target = max(8, int(qps * 2))
            current = ready_queue.qsize()
            if current < target:
                batch_size = min(target - current, num_workers * 2)
                idxs = [_next_session_idx() for _ in range(batch_size)]
                futs = [
                    loop.run_in_executor(
                        cpu_pool,
                        _create_conv_in_worker,
                        idx,
                        s,
                        sst,
                    )
                    for idx in idxs
                ]
                for fut in asyncio.as_completed(futs):
                    try:
                        conv = await fut
                        await ready_queue.put((conv, 0))
                    except Exception as e:
                        logger.warning("Failed to create conversation in worker: %s", e)
            await asyncio.sleep(0.02)

    async def execute_turn(
        conv: Conversation, turn_idx: int, http_session: aiohttp.ClientSession
    ) -> None:
        cur_spec = workload["spec"]
        runtime.inflight += 1
        try:
            outcome = await execute_single_turn(
                http_session=http_session,
                conv=conv,
                turn_idx=turn_idx,
                base_url=args.base_url,
                model=args.model,
                max_tokens=cur_spec.osl,
                bench_start_ns=bench_start_ns,
                first_chunk_threshold=args.first_chunk_threshold,
                api_key=getattr(args, "api_key", None),
            )
            metric = outcome.metric
            auto_complete_summary: Optional[str] = None
            runtime.total_completed += 1
            if runtime.measurement_active:
                target = runtime.measurement_target_requests
                if target is None:
                    runtime.measurement_metrics.append(metric)
                elif len(runtime.measurement_metrics) < target:
                    runtime.measurement_metrics.append(metric)

                if target is not None and len(runtime.measurement_metrics) >= target:
                    runtime.measurement_active = False
                    end_ns = time.perf_counter_ns()
                    start_ns = runtime.measurement_start_ns or end_ns
                    runtime.last_window_elapsed_s = (end_ns - start_ns) / 1e9
                    runtime.last_window_metrics = list(
                        runtime.measurement_metrics[:target]
                    )
                    runtime.measurement_target_requests = None
                    summary = summarize_metrics(
                        runtime.last_window_metrics,
                        runtime.last_window_elapsed_s,
                    )
                    auto_complete_summary = json.dumps(summary, indent=2)
                    runtime.last_notice = (
                        f"measurement auto-complete ({target} req):\n"
                        f"{auto_complete_summary}"
                    )

            if auto_complete_summary is not None:
                print("Measurement auto-complete:")
                print(auto_complete_summary)

            next_turn = turn_idx + 1
            if not stop_event.is_set():
                if next_turn < cur_spec.num_turns:
                    await ready_queue.put((conv, next_turn))
                else:
                    conv = await next_conv_async()
                    await ready_queue.put((conv, 0))
        except Exception as e:
            if args.log_failures:
                print(
                    f"[request-failed] session={conv.session_id} turn={turn_idx}: {e}"
                )
            runtime.total_failed += 1
            if not stop_event.is_set():
                conv = await next_conv_async()
                await ready_queue.put((conv, 0))
        finally:
            runtime.inflight -= 1

    async def pacer(http_session: aiohttp.ClientSession) -> None:
        next_dispatch = time.perf_counter()
        while not stop_event.is_set():
            qps = runtime.current_qps

            if qps <= 0:
                await asyncio.sleep(0.1)
                next_dispatch = time.perf_counter() + 0.05
                continue

            if rate_changed.is_set():
                rate_changed.clear()
                next_dispatch = time.perf_counter() + (1.0 / qps)

            now = time.perf_counter()
            if next_dispatch < now - 1.0:
                next_dispatch = now

            wait = next_dispatch - now
            if wait > 0:
                await asyncio.sleep(wait)
                if stop_event.is_set():
                    break

            try:
                conv, turn_idx = ready_queue.get_nowait()
            except asyncio.QueueEmpty:
                next_dispatch += 1.0 / qps
                continue

            t = asyncio.create_task(execute_turn(conv, turn_idx, http_session))
            running_tasks.add(t)
            t.add_done_callback(running_tasks.discard)
            next_dispatch += 1.0 / qps

    async def reporter() -> None:
        if args.status_interval <= 0:
            return
        while not stop_event.is_set():
            await asyncio.sleep(args.status_interval)
            print(
                "status: "
                f"qps={runtime.current_qps:.2f} "
                f"inflight={runtime.inflight} "
                f"completed={runtime.total_completed} "
                f"failed={runtime.total_failed} "
                f"measured={len(runtime.measurement_metrics)} "
                f"active={runtime.measurement_active}",
                flush=True,
            )

    cmd_handler = CommandHandler(
        runtime=runtime,
        workload=workload,
        args=args,
        text_gen=text_gen,
        rate_changed=rate_changed,
        workload_changed=workload_changed,
        stop_event=stop_event,
    )
    handle_command = cmd_handler.handle

    async def stdin_command_loop() -> None:
        print(
            "Interactive commands: help | rate <qps> | start | measure <n> | "
            "stop | status | workload [k=v ...] | save [path|name] | "
            "save-dir <path> | quit"
        )
        while not stop_event.is_set():
            raw = await asyncio.to_thread(input, "bench> ")
            resp = await handle_command(raw)
            if resp:
                print(resp)

    async def socket_command_handler(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            data = await reader.read(4096)
            cmd = data.decode("utf-8", errors="replace").strip()
            resp = await handle_command(cmd)
            writer.write((resp + "\n").encode("utf-8"))
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    # Seed the ready queue
    seed_count = 2
    seed_idxs = [_next_session_idx() for _ in range(seed_count)]
    seed_futs = [
        loop.run_in_executor(
            cpu_pool,
            _create_conv_in_worker,
            idx,
            spec,
            shared_system_text,
        )
        for idx in seed_idxs
    ]
    for conv in await asyncio.gather(*seed_futs):
        await ready_queue.put((conv, 0))

    control_socket = _control_socket_path()
    if os.path.exists(control_socket):
        os.unlink(control_socket)
    socket_server = await asyncio.start_unix_server(
        socket_command_handler,
        path=control_socket,
    )
    print(f"Control socket listening at: {control_socket}")

    stdin_control = getattr(args, "stdin_control", False)
    if not stdin_control:
        print("Use a second terminal with --client to send commands.")

    connector = aiohttp.TCPConnector(limit=0)
    async with aiohttp.ClientSession(connector=connector) as http_session:
        background_tasks = [
            asyncio.create_task(pacer(http_session)),
            asyncio.create_task(reporter()),
            asyncio.create_task(socket_server.serve_forever()),
            asyncio.create_task(prefill_queue()),
        ]
        stdin_task = (
            asyncio.create_task(stdin_command_loop()) if stdin_control else None
        )

        if stdin_task is not None:
            await stdin_task
        else:
            await stop_event.wait()

        stop_event.set()
        socket_server.close()
        await socket_server.wait_closed()
        await asyncio.gather(*background_tasks, return_exceptions=True)

        if running_tasks:
            print(f"Waiting for {len(running_tasks)} in-flight request task(s)...")
            await asyncio.gather(*list(running_tasks), return_exceptions=True)

    cpu_pool.shutdown(wait=False)
    if os.path.exists(control_socket):
        os.unlink(control_socket)


# ============================================================================
# Interactive client
# ============================================================================


async def _send_command_once(control_socket: str, cmd: str) -> str:
    reader, writer = await asyncio.open_unix_connection(control_socket)
    writer.write(cmd.encode("utf-8"))
    await writer.drain()
    if writer.can_write_eof():
        writer.write_eof()
    data = await reader.read()
    writer.close()
    await writer.wait_closed()
    return data.decode("utf-8", errors="replace").strip()


async def run_client(args: argparse.Namespace) -> None:
    control_socket = _control_socket_path()
    print(f"Connected to control socket: {control_socket}")
    print(
        "Type commands: help, rate <qps>, start, measure <n>, stop, status, "
        "save [path|name], save-dir <path>, quit"
    )
    session = None
    if PromptSession is not None and FileHistory is not None:
        history_path = str(Path("~/.interactive_rate_bench_history").expanduser())
        session = PromptSession(history=FileHistory(history_path))
    else:
        print(
            "prompt_toolkit not installed; using basic input(). "
            "Install with: pip install prompt_toolkit"
        )
    while True:
        if session is not None:
            raw = await asyncio.to_thread(session.prompt, "benchctl> ")
        else:
            raw = await asyncio.to_thread(input, "benchctl> ")
        cmd = raw.strip()
        if not cmd:
            continue
        try:
            resp = await _send_command_once(control_socket, cmd)
        except (FileNotFoundError, ConnectionRefusedError) as e:
            print(f"Failed to connect to server socket: {e}")
            return
        print(resp)
        if cmd.lower() in ("quit", "exit"):
            return


async def run_client_oneshot(args: argparse.Namespace) -> None:
    if not args.cmd:
        raise ValueError("Client mode with --cmd requires a command string.")
    control_socket = _control_socket_path()
    try:
        resp = await _send_command_once(control_socket, args.cmd)
    except (FileNotFoundError, ConnectionRefusedError) as e:
        raise RuntimeError(f"Failed to connect to server socket: {e}") from e
    print(resp)


# ============================================================================
# Entry points for cli.py
# ============================================================================


def run_interactive_server(args: argparse.Namespace) -> int:
    """Entry point for interactive server mode."""
    try:
        asyncio.run(run_interactive(args))
        return 0
    except Exception as e:
        logger.error("Interactive server failed: %s", e)
        return 1


def run_interactive_client(args: argparse.Namespace) -> int:
    """Entry point for interactive client mode."""
    try:
        if args.cmd:
            asyncio.run(run_client_oneshot(args))
        else:
            asyncio.run(run_client(args))
        return 0
    except Exception as e:
        logger.error("Interactive client failed: %s", e)
        return 1
