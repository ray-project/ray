import argparse
import logging
import time
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Tuple

from ray.serve._private.utils import generate_request_id

logger = logging.getLogger(__file__)
logging.basicConfig(level=logging.INFO)

MASTER_PORT = 5557


@dataclass
class LocustStage:
    duration_s: int
    users: int
    spawn_rate: float


@dataclass
class PerformanceStats:
    p50_latency: float
    p90_latency: float
    p99_latency: float
    rps: float
    max_latency: float = 0.0

@dataclass
class LocustTestResults:
    history: List[Dict]
    total_requests: int
    num_failures: int
    avg_latency: float
    p50_latency: float
    p90_latency: float
    p99_latency: float
    avg_rps: float
    stats_in_stages: List[PerformanceStats]
    max_latency: float = 0.0


@dataclass
class FailedRequest:
    request_id: str
    status_code: int
    exception: str
    response_time_ms: float
    start_time_s: float


class LocustClient:
    def __init__(
        self,
        host_url: str,
        token: str,
        data: Dict[str, Any] = None,
    ):
        from locust import FastHttpUser, constant, events, task
        from locust.contrib.fasthttp import FastResponse

        self.errors = []
        self.stats_in_stages: List[PerformanceStats] = []

        class EndpointUser(FastHttpUser):
            wait_time = constant(0)
            failed_requests = []
            host = host_url

            @task
            def test(self):
                request_id = generate_request_id()
                headers = (
                    {"Authorization": f"Bearer {token}", "X-Request-ID": request_id}
                    if token
                    else None
                )
                with self.client.get(
                    "", headers=headers, json=data, catch_response=True
                ) as r:
                    r.request_meta["context"]["request_id"] = request_id

            @events.request.add_listener
            def on_request(
                response: FastResponse,
                exception,
                context,
                start_time: float,
                response_time: float,
                **kwargs,
            ):
                if exception and response.status_code != 0:
                    request_id = context["request_id"]
                    print(
                        f"Request '{request_id}' failed with exception:\n"
                        f"{exception}\n{response.text}"
                    )

                    if response.status_code != 0:
                        response.encoding = "utf-8"
                        err = FailedRequest(
                            request_id=request_id,
                            status_code=response.status_code,
                            exception=response.text,
                            response_time_ms=response_time,
                            start_time_s=start_time,
                        )
                        self.errors.append(err)
                        print(
                            f"Request '{request_id}' failed with exception:\n"
                            f"{exception}\n{response.text}"
                        )

        self.user_class = EndpointUser


def on_stage_finished(master_runner, stats_in_stages):
    stats_entry_key = ("", "GET")
    stats_entry = master_runner.stats.entries.get(stats_entry_key)

    stats_in_stages.append(
        PerformanceStats(
            p50_latency=stats_entry.get_current_response_time_percentile(0.5),
            p90_latency=stats_entry.get_current_response_time_percentile(0.9),
            p99_latency=stats_entry.get_current_response_time_percentile(0.99),
            rps=stats_entry.current_rps,
            max_latency=stats_entry.max_response_time,
        )
    )


def run_locust_worker(
    master_address: str, host_url: str, token: str, data: Dict[str, Any]
):
    import locust
    from locust.env import Environment
    from locust.log import setup_logging

    setup_logging("INFO")
    client = LocustClient(host_url=host_url, token=token, data=data)
    env = Environment(user_classes=[client.user_class], events=locust.events)

    runner = env.create_worker_runner(
        master_host=master_address, master_port=MASTER_PORT
    )
    runner.greenlet.join()

    if client.errors:
        raise RuntimeError(f"There were {len(client.errors)} errors: {client.errors}")


def run_locust_master(
    host_url: str,
    token: str,
    expected_num_workers: int,
    stages: List[LocustStage],
    wait_for_workers_timeout_s: float,
):
    import gevent
    import locust
    from locust import LoadTestShape
    from locust.env import Environment
    from locust.stats import (
        get_error_report_summary,
        get_percentile_stats_summary,
        get_stats_summary,
        stats_history,
        stats_printer,
    )

    client = LocustClient(host_url, token)

    class StagesShape(LoadTestShape):
        curr_stage_ix = 0

        def tick(cls):
            run_time = cls.get_run_time()
            prefix_time = 0
            for i, stage in enumerate(stages):
                prefix_time += stage.duration_s

                if run_time < prefix_time:
                    if i != cls.curr_stage_ix:
                        on_stage_finished(master_runner, client.stats_in_stages)
                        cls.curr_stage_ix = i

                    current_stage = stages[cls.curr_stage_ix]
                    return current_stage.users, current_stage.spawn_rate

            # End of stage test
            on_stage_finished(master_runner, client.stats_in_stages)

    master_env = Environment(
        user_classes=[client.user_class],
        shape_class=StagesShape(),
        events=locust.events,
    )
    master_runner = master_env.create_master_runner("*", MASTER_PORT)

    start = time.time()
    while len(master_runner.clients.ready) < expected_num_workers:
        if time.time() - start > wait_for_workers_timeout_s:
            raise RuntimeError(
                f"Timed out waiting for {expected_num_workers} workers to "
                "connect to Locust master."
            )

        print(
            f"Waiting for workers to be ready, "
            f"{len(master_runner.clients.ready)} "
            f"of {expected_num_workers} ready."
        )
        time.sleep(1)

    # Periodically output current stats (each entry is aggregated
    # stats over the past 10 seconds, by default)
    gevent.spawn(stats_printer(master_env.stats))
    gevent.spawn(stats_history, master_runner)

    # Start test & wait for the shape test to finish
    master_runner.start_shape()
    master_runner.shape_greenlet.join()
    # Send quit signal to all locust workers
    master_runner.quit()

    # Print stats
    for line in get_stats_summary(master_runner.stats, current=False):
        print(line)
    # Print percentile stats
    for line in get_percentile_stats_summary(master_runner.stats):
        print(line)
    # Print error report
    if master_runner.stats.errors:
        for line in get_error_report_summary(master_runner.stats):
            print(line)

    stats_entry_key = ("", "GET")
    stats_entry = master_runner.stats.entries.get(stats_entry_key)
    results = LocustTestResults(
        history=master_runner.stats.history,
        total_requests=master_runner.stats.num_requests,
        num_failures=master_runner.stats.num_failures,
        avg_latency=stats_entry.avg_response_time,
        p50_latency=stats_entry.get_response_time_percentile(0.5),
        p90_latency=stats_entry.get_response_time_percentile(0.9),
        p99_latency=stats_entry.get_response_time_percentile(0.99),
        avg_rps=stats_entry.total_rps,
        stats_in_stages=client.stats_in_stages,
        max_latency=stats_entry.max_response_time,
    )
    return asdict(results)


def _make_endpoint_user_class(
    name: str,
    route: str,
    weight: int,
    host_url: str,
    token: str,
    payload: Any,
    user_class_name_prefix: str = "",
) -> type:
    """Create a locust FastHttpUser subclass for one endpoint."""
    from locust import FastHttpUser, between, task

    # Capture local vars for the closure
    _name = name
    _route = route
    _weight = weight
    _host_url = host_url
    _token = token
    _payload = payload

    _headers = {"Authorization": f"Bearer {_token}"} if _token else None

    class _User(FastHttpUser):
        host = _host_url
        wait_time = between(min_wait=0.001, max_wait=0.002)
        weight = _weight

        @task
        def predict(self):
            self.client.post(_route, json=_payload, name=_name, headers=_headers)

    prefix = f"{user_class_name_prefix}_" if user_class_name_prefix else ""
    _User.__name__ = prefix + name.replace("-", "_") + "_User"
    _User.__qualname__ = _User.__name__
    return _User


def _build_user_classes(
    endpoints: List[Tuple],
    host_url: str,
    token: str,
    payload: Any,
    user_class_name_prefix: str = "",
) -> List[type]:
    """Build locust user classes from endpoint configs."""
    return [
        _make_endpoint_user_class(
            n, r, w, host_url, token, payload, user_class_name_prefix
        )
        for n, r, w in endpoints
    ]


def _interpolate(run_time_sec: float, profile: List[Tuple[float, float]]) -> float:
    """Linear interpolation of (time, value) control points."""
    if run_time_sec <= 0:
        return float(profile[0][1])
    for i, (t, v) in enumerate(profile):
        if run_time_sec <= t:
            if i == 0:
                return float(v)
            t_prev, v_prev = profile[i - 1]
            frac = (run_time_sec - t_prev) / (t - t_prev)
            return v_prev + frac * (v - v_prev)
    return 0.0


def run_multi_endpoint_worker(
    master_address: str,
    host_url: str,
    token: str,
    warmup_endpoints: List[Tuple],
    ramp_endpoints: List[Tuple],
    payload: Any = None,
):
    """Run a locust worker with multi-endpoint user classes."""
    import locust
    from locust.env import Environment
    from locust.log import setup_logging

    setup_logging("INFO")
    warmup_classes = _build_user_classes(
        warmup_endpoints, host_url, token, payload, user_class_name_prefix="warmup"
    )
    ramp_classes = _build_user_classes(
        ramp_endpoints, host_url, token, payload, user_class_name_prefix="ramp"
    )
    all_classes = warmup_classes + ramp_classes

    env = Environment(user_classes=all_classes, events=locust.events)
    runner = env.create_worker_runner(
        master_host=master_address, master_port=MASTER_PORT
    )

    try:
        runner.greenlet.join()
    except Exception as e:
        logger.warning(f"Worker runner exited with exception during teardown: {e}")


def run_multi_endpoint_master(
    host_url: str,
    token: str,
    expected_num_workers: int,
    warmup_endpoints: List[Tuple],
    ramp_endpoints: List[Tuple],
    ramp_profile: List[Tuple[float, float]],
    warmup_sec: int,
    wait_for_workers_timeout_s: float,
    payload: Any = None,
    warmup_spawn_rate: int = 2,
    ramp_base_users: int = 9,
    ramp_spawn_rate: int = 20,
    stages: Optional[List[Tuple]] = None,
) -> Dict:
    """Run a locust master with multi-endpoint weighted load shape."""
    import gevent
    import locust
    from locust import LoadTestShape
    from locust.env import Environment
    from locust.stats import (
        get_error_report_summary,
        get_percentile_stats_summary,
        get_stats_summary,
        stats_history,
        stats_printer,
    )

    # sliding window for metrics; default is 10s
    locust.stats.CURRENT_RESPONSE_TIME_PERCENTILE_WINDOW = 20

    # default interval is 2s
    locust.stats.CONSOLE_STATS_INTERVAL_SEC = 5

    warmup_classes = _build_user_classes(
        warmup_endpoints, host_url, token, payload, user_class_name_prefix="warmup"
    )
    ramp_classes = _build_user_classes(
        ramp_endpoints, host_url, token, payload, user_class_name_prefix="ramp"
    )
    all_classes = warmup_classes + ramp_classes

    stage_stats = []
    prev_stage = [None]
    stage_start_requests = 0
    stage_start_time = 0.0

    def _get_stage(run_time):
        if not stages:
            return None
        for name, start_s, end_s in stages:
            if start_s <= run_time < end_s:
                return name
        return None

    def _capture_stage_stats(stage_name):
        nonlocal stage_start_requests, stage_start_time

        if stage_name is None:
            return

        now = time.time()
        total = master_runner.stats.total
        if total.num_requests == 0:
            return

        requests_in_stage = max(0, total.num_requests - stage_start_requests)
        duration = max(1e-6, now - stage_start_time)

        stage_stats.append(
            {
                "name": stage_name,
                "avg_rps": requests_in_stage / duration,
                "p99_latency": total.get_current_response_time_percentile(0.99),
                "max_latency": total.max_response_time,
                "avg_users": master_runner.user_count,
            }
        )
        stage_start_requests = total.num_requests
        stage_start_time = now

    class MultiEndpointLoadShape(LoadTestShape):
        def tick(cls):
            run_time = cls.get_run_time()

            # Track stage transitions
            current = _get_stage(run_time)
            if current != prev_stage[0]:
                if prev_stage[0] is not None:
                    _capture_stage_stats(prev_stage[0])
                prev_stage[0] = current

            if run_time < warmup_sec:
                return len(warmup_classes), warmup_spawn_rate, warmup_classes

            ramp_time = run_time - warmup_sec
            ramp_users = _interpolate(ramp_time, ramp_profile)
            total_users = ramp_base_users + int(round(ramp_users))

            # End of test: let Locust stop the run.
            if ramp_users <= 0 and ramp_time >= ramp_profile[-1][0]:
                if prev_stage[0] is not None:
                    _capture_stage_stats(prev_stage[0])
                    prev_stage[0] = None
                return None

            return total_users, ramp_spawn_rate, ramp_classes

    master_env = Environment(
        user_classes=all_classes,
        shape_class=MultiEndpointLoadShape(),
        events=locust.events,
    )
    master_runner = master_env.create_master_runner(
        master_bind_host="*", master_bind_port=MASTER_PORT
    )

    start = time.time()
    while len(master_runner.clients.ready) < expected_num_workers:
        if time.time() - start > wait_for_workers_timeout_s:
            raise RuntimeError(
                f"Timed out waiting for {expected_num_workers} workers to "
                f"connect. Got {len(master_runner.clients.ready)}."
            )
        print(
            f"Waiting for workers: "
            f"{len(master_runner.clients.ready)}/{expected_num_workers} ready."
        )
        time.sleep(1)

    gevent.spawn(stats_printer(master_env.stats))
    gevent.spawn(stats_history, master_runner)

    stage_start_requests = master_runner.stats.num_requests
    stage_start_time = time.time()

    master_runner.start_shape()
    master_runner.shape_greenlet.join()

    # Send quit signal to all locust workers
    master_runner.quit()

    # Print stats
    for line in get_stats_summary(master_runner.stats, current=False):
        print(line)
    for line in get_percentile_stats_summary(master_runner.stats):
        print(line)
    if master_runner.stats.errors:
        for line in get_error_report_summary(master_runner.stats):
            print(line)

    # Collect per-endpoint stats
    endpoint_names = {
        n for eps in [warmup_endpoints, ramp_endpoints] for n, _, _ in eps
    }
    per_endpoint = {}
    for ep_name in endpoint_names:
        entry = master_runner.stats.entries.get((ep_name, "POST"))
        if entry and entry.num_requests > 0:
            per_endpoint[ep_name] = {
                "request_count": entry.num_requests,
                "failure_count": entry.num_failures,
                "p50_latency": entry.get_response_time_percentile(0.5),
                "p90_latency": entry.get_response_time_percentile(0.9),
                "p99_latency": entry.get_response_time_percentile(0.99),
                "rps": entry.total_rps,
            }

    stages_dict = {}
    for s in stage_stats:
        stages_dict[s["name"]] = {
            "avg_rps": s["avg_rps"],
            "p99_latency": s["p99_latency"],
            "max_latency": s["max_latency"],
            "avg_users": s["avg_users"],
        }

    total = master_runner.stats.total
    results = {
        "total_requests": master_runner.stats.num_requests,
        "num_failures": master_runner.stats.num_failures,
        "p50_latency": total.get_response_time_percentile(0.5),
        "p90_latency": total.get_response_time_percentile(0.9),
        "p99_latency": total.get_response_time_percentile(0.99),
        "max_latency": total.max_response_time,
        "avg_latency": total.avg_response_time,
        "avg_rps": total.total_rps,
        "per_endpoint": per_endpoint,
        "stages": stages_dict,
    }

    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker-type", type=str, required=True)
    parser.add_argument("--host-url", type=str, required=True)
    parser.add_argument("--token", type=str, required=True)
    parser.add_argument("--master-address", type=str, required=False)
    parser.add_argument("--expected-num-workers", type=int, required=False)
    parser.add_argument("--stages", type=str, required=False)
    parser.add_argument("--wait-for-workers-timeout-s", type=float, required=False)
    args = parser.parse_args()
    host_url = args.host_url
    token = args.token
    if args.worker_type == "master":
        results = run_locust_master(
            host_url,
            token,
            args.expected_num_workers,
            args.stages,
            args.wait_for_workers_timeout_s,
        )
    else:
        results = run_locust_worker(args.master_address, host_url, token, args.data)

    print(results)


if __name__ == "__main__":
    main()
