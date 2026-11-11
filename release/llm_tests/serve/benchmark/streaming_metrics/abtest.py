"""Same-engine OFF/ON comparison on a warmed metrics-ON deployment.

OFF restores production flag-OFF behavior in place: vLLM's native stream
generators and route handlers in each replica, and an HAProxy reload without
the observer filter, exporter or routing timer. Alternating OFF/ON runs on the
same engine processes removes deploy-to-deploy engine variance.

Usage: python abtest.py --out DIR --points 6:64:360:8 36:256:2048:32 --cycles 3
"""
import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import requests

PY = "/mnt/local_storage/sm/sm_env/bin/python"
MODEL = "Qwen/Qwen3.5-0.8B"


def toggle_hooks(self, enabled):
    """Runs inside a replica actor."""
    from fastapi.routing import APIRoute, request_response

    replica = self._replica_impl
    engine = replica._user_callable_wrapper._callable.engine
    app = replica._user_callable_asgi_app
    state = getattr(engine, "_ab_state", None)
    if state is None:
        hooks = []
        for serving, name in (
            (engine._oai_serving_completion, "completion_stream_generator"),
            (engine._oai_serving_chat, "chat_completion_stream_generator"),
        ):
            if serving is not None and name in vars(serving):
                hooks.append((serving, name, vars(serving)[name]))
        routes = [
            (route, route.app)
            for route in app.routes
            if isinstance(route, APIRoute)
            and route.path in ("/v1/completions", "/v1/chat/completions")
        ]
        state = engine._ab_state = (hooks, routes)
    hooks, routes = state
    assert len(hooks) == 2 and len(routes) == 2, (hooks, routes)
    for serving, name, wrapper in hooks:
        vars(serving).pop("chat_completion_stream_generator_raw", None)
        if enabled:
            setattr(serving, name, wrapper)
        else:
            vars(serving).pop(name, None)
    for route, wrapped in routes:
        route.app = wrapped if enabled else request_response(route.get_route_handler())
    return {"pid": os.getpid(), "enabled": enabled}


def install_copies(self, unused_fields=0):
    """Runs inside a replica actor (benchmark-only arm): vLLM's native
    generators and routes, but with the validator-free model copies the
    instrumented path uses and no token counts. ON minus this arm isolates the
    metrics' own cost."""
    import types
    from typing import List

    from ray.llm._internal.serve.observability.metrics import llm_metrics as lm
    from vllm.entrypoints.openai.chat_completion import serving as chat
    from vllm.entrypoints.openai.completion import serving as completion

    toggle_hooks(self, False)
    engine = self._replica_impl._user_callable_wrapper._callable.engine
    # Built once per replica, like ON's models, so repeated arm switches do not
    # accumulate model classes in one arm only. unused_fields > 0 is the
    # "placebo" arm: COPIES plus unset fields, a per-event validation cost
    # comparable to the count's, without any count or observer.
    cache = engine.__dict__.setdefault("_ab_copies", {})
    copies = cache.get(unused_fields)
    if copies is None:
        from typing import Any

        unused = {f"ray_unused_{i}": (Any, None) for i in range(unused_fields)}
        # The same closed (extra="ignore") choice as ON's counted model, so ON
        # minus COPIES is the count alone.
        extra = (
            "ignore"
            if lm._closed_choices(
                type(engine._oai_serving_completion).completion_stream_generator
            )
            else "allow"
        )
        c = lm._stream_model(
            completion.CompletionResponseStreamChoice, "C0", extra=extra, **unused
        )
        r = lm._stream_model(
            completion.CompletionStreamResponse, "R0", choices=(List[c], ...)
        )
        cc = lm._stream_model(chat.ChatCompletionResponseStreamChoice, "CC0")
        cr = lm._stream_model(
            chat.ChatCompletionStreamResponse, "CR0", choices=(List[cc], ...)
        )
        copies = cache[unused_fields] = (
            lm._clone(
                type(engine._oai_serving_completion).completion_stream_generator,
                CompletionResponseStreamChoice=c,
                CompletionStreamResponse=r,
            ),
            lm._clone(
                type(engine._oai_serving_chat).chat_completion_stream_generator_raw,
                ChatCompletionResponseStreamChoice=cc,
                ChatCompletionStreamResponse=cr,
            ),
        )
    serving = engine._oai_serving_completion
    serving.completion_stream_generator = types.MethodType(copies[0], serving)
    serving = engine._oai_serving_chat
    serving.chat_completion_stream_generator_raw = types.MethodType(copies[1], serving)
    return {
        "pid": os.getpid(),
        "copies": True,
        "copies_choice_extra": copies[0]
        .__globals__["CompletionResponseStreamChoice"]
        .model_config["extra"],
    }


def toggle_observer(self, enabled):
    """Runs inside the HAProxy proxy actor."""
    self._haproxy.cfg.stream_metrics_enabled = enabled
    return True


def replica_pid(self):
    return os.getpid()


def thread_sched(pids):
    """Per-thread scheduler stats of ``pids``: (comm, run_ns, runqueue_wait_ns,
    timeslices, nonvoluntary context switches)."""
    out = {}
    for pid in pids:
        try:
            tids = os.listdir(f"/proc/{pid}/task")
        except OSError:
            continue
        for tid in tids:
            base = f"/proc/{pid}/task/{tid}"
            try:
                run, wait, slices = map(int, open(f"{base}/schedstat").read().split())
                comm = open(f"{base}/comm").read().strip()
                nv = next(
                    int(line.split()[1])
                    for line in open(f"{base}/status")
                    if line.startswith("nonvoluntary_ctxt_switches")
                )
            except (OSError, StopIteration, ValueError):
                continue
            out[f"{pid}:{tid}"] = (comm, run, wait, slices, nv)
    return out


def sched_delta(before, after):
    rows = []
    for key, (comm, run, wait, slices, nv) in after.items():
        if key in before:
            b = before[key]
            rows.append(
                {
                    "thread": key,
                    "comm": comm,
                    "run_s": (run - b[1]) / 1e9,
                    "wait_s": (wait - b[2]) / 1e9,
                    "slices": slices - b[3],
                    "nvcsw": nv - b[4],
                }
            )
    rows.sort(key=lambda r: -r["run_s"])
    return rows


def haproxy_glibc_tunables():
    """GLIBC_TUNABLES of each live HAProxy process. glibc's parser overwrites the
    ':' separators in the process's environment with NUL, so the 'glibc.'
    entries that follow it are rejoined."""
    out = {}
    for pid in subprocess.run(
        ["pgrep", "-x", "haproxy"], capture_output=True, text=True
    ).stdout.split():
        try:
            entries = open(f"/proc/{pid}/environ").read().split("\0")
        except OSError:
            continue
        value = None
        for i, entry in enumerate(entries):
            if entry.startswith("GLIBC_TUNABLES="):
                parts = [entry.split("=", 1)[1]]
                for following in entries[i + 1 :]:
                    if not following.startswith("glibc."):
                        break
                    parts.append(following)
                value = ":".join(parts)
                break
        out[pid] = value
    return out


GLIBC_TUNED = (
    "glibc.malloc.trim_threshold=1073741824:glibc.malloc.mmap_threshold=33554432"
)


def set_haproxy_env(self, tunables):
    """Runs inside the HAProxy proxy actor: GLIBC_TUNABLES for HAProxy processes
    spawned from now on (benchmark-only allocator arms)."""
    if tunables:
        os.environ["GLIBC_TUNABLES"] = tunables
    else:
        os.environ.pop("GLIBC_TUNABLES", None)
    return True


def process_cpu(self):
    """Runs inside an actor: this process's user+system CPU seconds."""
    times = os.times()
    return times.user + times.system


def haproxy_cpu():
    """CPU seconds of the live HAProxy processes (all generations)."""
    hz = os.sysconf("SC_CLK_TCK")
    total = 0.0
    for pid in subprocess.run(
        ["pgrep", "-f", "ray_haproxy/bin/haproxy"], capture_output=True, text=True
    ).stdout.split():
        try:
            fields = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
            total += (int(fields[11]) + int(fields[12])) / hz
        except (FileNotFoundError, ProcessLookupError):
            pass
    return total


def haproxy_config(self):
    return Path(self._haproxy.config_file_path).read_text()


def probe(enabled):
    response = requests.post(
        "http://127.0.0.1:8000/v1/completions",
        json={
            "model": MODEL,
            "prompt": "hello",
            "max_tokens": 4,
            "stream": True,
            "ignore_eos": True,
        },
        stream=True,
        timeout=60,
    )
    response.raise_for_status()
    assert "x-ray-llm-metric-tags" not in response.headers
    body = response.content.decode()
    assert ('"ray_token_count"' in body) == enabled, body[:400]


def actors():
    import ray
    from ray.util.state import list_actors

    alive = list_actors(filters=[("state", "=", "ALIVE")], limit=1000)
    replicas = [
        a for a in alive if a.class_name.startswith("ServeReplica:sm:LLMServer")
    ]
    proxies = [a for a in alive if "HAProxyManager" in a.class_name]
    handles = [ray.get_actor(a.name, namespace="serve") for a in replicas]
    proxy = ray.get_actor(proxies[0].name, namespace="serve")
    return handles, proxy


def set_mode(
    enabled, replicas, proxy, out, dist=None, hooks=None, observer=None, tunables=""
):
    """enabled sets both sides; hooks/observer override one side for attribution."""
    import ray

    ray.get(proxy.__ray_call__.remote(set_haproxy_env, tunables))
    hooks = enabled if hooks is None else hooks
    observer = enabled if observer is None else observer
    if dist:
        subprocess.run(
            ["/mnt/local_storage/sm/use_variant.sh", dist],
            check=True,
            capture_output=True,
        )
    if hooks in ("copies", "placebo"):
        unused = 2 if hooks == "placebo" else 0
        settings = ray.get(
            [r.__ray_call__.remote(install_copies, unused) for r in replicas]
        )
        hooks = False
    else:
        settings = ray.get(
            [r.__ray_call__.remote(toggle_hooks, hooks) for r in replicas]
        )
    ray.get(proxy.__ray_call__.remote(toggle_observer, observer))
    ray.get(proxy._reload_haproxy.remote())
    config = ray.get(proxy.__ray_call__.remote(haproxy_config))
    assert ("filter ray_stream_metrics" in config) == observer
    time.sleep(2)
    for _ in range(4):
        probe(hooks)
    time.sleep(3)  # Let HAProxy export the probes before the run's first scrape.
    out.mkdir(parents=True, exist_ok=True)
    environs = haproxy_glibc_tunables()
    (out / "mode.json").write_text(
        json.dumps(
            {
                "hooks": hooks,
                "observer": observer,
                "dist": dist,
                "settings": settings,
                "tunables": tunables,
                "haproxy_glibc_tunables": environs,
            }
        )
    )
    (out / "haproxy.cfg").write_text(config)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True)
    parser.add_argument(
        "--points",
        nargs="+",
        default=["6:64:360:8", "18:128:1080:16", "36:256:2048:32"],
    )
    parser.add_argument("--cycles", type=int, default=3)
    parser.add_argument(
        "--first-cycle", type=int, default=0, help="Append cycles to an earlier run."
    )
    parser.add_argument("--backend", default="openai")
    parser.add_argument("--endpoint", default="/v1/completions")
    parser.add_argument(
        "--arms",
        nargs="+",
        default=["off:", "on:"],
        help="mode:dist per arm, e.g. off:dist_patched on:dist_patched on:dist_lean3",
    )
    args = parser.parse_args()
    import ray

    os.environ["RAY_RUNTIME_ENV_HOOK"] = "sm_noop_hook.env_hook"
    ray.init(address="auto", namespace="serve")
    replicas, proxy = actors()
    replica_pids = ray.get([r.__ray_call__.remote(replica_pid) for r in replicas])
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    try:
        for point in args.points:
            rate, conc, num, clients = point.split(":")
            arms = [a.split(":", 1) for a in args.arms]
            for cycle in range(args.first_cycle, args.first_cycle + args.cycles):
                # Rotate the arm order each cycle to cancel slow drifts.
                shift = cycle % len(arms)
                for mode, dist in arms[shift:] + arms[:shift]:
                    # on/off: both sides; py: Python counts only; hap: observer only;
                    # ong/offg/pyg: on/off/py with HAProxy spawned under GLIBC_TUNED.
                    base = {"ong": "on", "offg": "off", "pyg": "py"}.get(mode, mode)
                    tunables = GLIBC_TUNED if base != mode else ""
                    enabled = base == "on"
                    sides = {
                        "py": (True, False),
                        "hap": (False, True),
                        "copies": ("copies", False),
                        "placebo": ("placebo", False),
                    }.get(base, (enabled, enabled))
                    label = mode + (
                        f"-{dist}" if dist and len({d for _, d in arms}) > 1 else ""
                    )
                    tag = f"r{rate}_c{conc}_{label}_{cycle}"
                    run = out / tag
                    set_mode(
                        enabled,
                        replicas,
                        proxy,
                        run,
                        dist or None,
                        *sides,
                        tunables=tunables,
                    )
                    cmd = [
                        PY,
                        "bench.py",
                        "run",
                        "--out",
                        str(run),
                        "--rate",
                        rate,
                        "--conc",
                        conc,
                        "--num",
                        num,
                        "--clients",
                        clients,
                        "--seed",
                        "1000",
                        "--backend",
                        args.backend,
                        "--endpoint",
                        args.endpoint,
                    ]
                    print(time.strftime("%T"), "RUN", tag, flush=True)
                    import ray

                    frontend0 = sum(
                        ray.get([r.__ray_call__.remote(process_cpu) for r in replicas])
                    )
                    haproxy0 = haproxy_cpu()
                    hap_pids = subprocess.run(
                        ["pgrep", "-f", "ray_haproxy/bin/haproxy"],
                        capture_output=True,
                        text=True,
                    ).stdout.split()
                    rep0, hap0 = thread_sched(replica_pids), thread_sched(hap_pids)
                    with open(out / f"{tag}.txt", "w") as log:
                        subprocess.run(
                            cmd, stdout=log, stderr=subprocess.STDOUT, check=True
                        )
                    frontend1 = sum(
                        ray.get([r.__ray_call__.remote(process_cpu) for r in replicas])
                    )
                    haproxy1 = haproxy_cpu()
                    (run / "sched.json").write_text(
                        json.dumps(
                            {
                                "replicas": sched_delta(
                                    rep0, thread_sched(replica_pids)
                                )[:12],
                                "haproxy": sched_delta(hap0, thread_sched(hap_pids)),
                            }
                        )
                    )
                    (run / "component_cpu.json").write_text(
                        json.dumps(
                            {
                                "frontend_s": frontend1 - frontend0,
                                "haproxy_s": haproxy1 - haproxy0,
                            }
                        )
                    )
    finally:
        set_mode(
            True,
            replicas,
            proxy,
            out / "restored",
            args.arms[-1].split(":", 1)[1] or None,
        )
    print("AB DONE", flush=True)


if __name__ == "__main__":
    sys.exit(main())
