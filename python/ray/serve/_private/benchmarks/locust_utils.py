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


def on_stage_finished(master_runner, stats_in_stages):
    stats_entry_key = ("", "GET")
    stats_entry = master_runner.stats.entries.get(stats_entry_key)

    stats_in_stages.append(
        PerformanceStats(
            p50_latency=stats_entry.get_current_response_time_percentile(0.5),
            p90_latency=stats_entry.get_current_response_time_percentile(0.9),
            p99_latency=stats_entry.get_current_response_time_percentile(0.99),
            rps=stats_entry.current_rps,
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
