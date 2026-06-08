import argparse
import logging
import time
from dataclasses import asdict, dataclass
from typing import Any, Dict, List

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


def _fine_bucket_response_time(response_time):
    """Finer-grained replacement for locust's ``bucket_response_time``.

    Locust rounds response times to ~2 significant figures (1ms below 100ms,
    10ms below 1s, ...) to bound the percentile histogram for distributed mode.
    For sub-10ms Serve latencies that 1ms floor dominates the signal, so use
    0.1ms resolution below 100ms; stays coarse above to keep keys bounded.
    """
    if response_time < 100:
        return round(response_time, 1)
    elif response_time < 1000:
        return round(response_time)
    else:
        return int(round(response_time, -1))


def _install_fine_response_time_bucketing():
    """Swap in the finer bucketer. Must run in every process that logs response
    times (each worker) and the master."""
    import locust.stats

    locust.stats.bucket_response_time = _fine_bucket_response_time


def _percentile_from_histogram(response_times, num_requests, percentile):
    """Percentile over a ``{rounded_response_time: count}`` histogram, so we can
    derive per-stage percentiles by differencing cumulative snapshots."""
    if not num_requests or not response_times:
        return 0.0
    target = num_requests * percentile
    acc = 0
    for rt in sorted(response_times):
        acc += response_times[rt]
        if acc >= target:
            return rt
    return max(response_times)


def _response_time_snapshot(stats_entry):
    """Cumulative response-time histogram + request count for stage diffing."""
    return {
        "response_times": dict(stats_entry.response_times),
        "num_requests": stats_entry.num_requests,
    }


def on_stage_finished(master_runner, stats_in_stages, stage_duration_s, prev_snapshot):
    """Record per-stage stats by differencing cumulative snapshots.

    Reports the stage's *average* RPS (requests in the stage / duration) and
    *full-stage* percentiles (over the stage's own histogram), instead of
    locust's trailing-~10s ``current_rps`` / current-percentile snapshot, which
    is far noisier run-to-run. Returns the snapshot to seed the next stage.
    """
    stats_entry = master_runner.stats.entries.get(("", "GET"))
    snapshot = _response_time_snapshot(stats_entry)

    stage_hist = {}
    for rt, count in snapshot["response_times"].items():
        delta = count - prev_snapshot["response_times"].get(rt, 0)
        if delta > 0:
            stage_hist[rt] = delta
    stage_requests = snapshot["num_requests"] - prev_snapshot["num_requests"]

    stats_in_stages.append(
        PerformanceStats(
            p50_latency=_percentile_from_histogram(stage_hist, stage_requests, 0.5),
            p90_latency=_percentile_from_histogram(stage_hist, stage_requests, 0.9),
            p99_latency=_percentile_from_histogram(stage_hist, stage_requests, 0.99),
            rps=stage_requests / stage_duration_s if stage_duration_s else 0.0,
        )
    )
    return snapshot


def run_locust_worker(
    master_address: str, host_url: str, token: str, data: Dict[str, Any]
):
    import locust
    from locust.env import Environment
    from locust.log import setup_logging

    setup_logging("INFO")
    # Workers log response times, so the finer bucketer must be installed here.
    _install_fine_response_time_bucketing()
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

    _install_fine_response_time_bucketing()
    client = LocustClient(host_url, token)

    class StagesShape(LoadTestShape):
        curr_stage_ix = 0
        # Cumulative response-time snapshot at the start of the current stage;
        # on_stage_finished diffs against it to get per-stage stats.
        prev_snapshot = {"response_times": {}, "num_requests": 0}

        def tick(cls):
            run_time = cls.get_run_time()
            prefix_time = 0
            for i, stage in enumerate(stages):
                prefix_time += stage.duration_s

                if run_time < prefix_time:
                    if i != cls.curr_stage_ix:
                        cls.prev_snapshot = on_stage_finished(
                            master_runner,
                            client.stats_in_stages,
                            stages[cls.curr_stage_ix].duration_s,
                            cls.prev_snapshot,
                        )
                        cls.curr_stage_ix = i

                    current_stage = stages[cls.curr_stage_ix]
                    return current_stage.users, current_stage.spawn_rate

            # End of stage test
            on_stage_finished(
                master_runner,
                client.stats_in_stages,
                stages[cls.curr_stage_ix].duration_s,
                cls.prev_snapshot,
            )

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
    )
    return asdict(results)


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
