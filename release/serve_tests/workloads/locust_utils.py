from dataclasses import asdict, dataclass
from itertools import chain
import json
import logging
import time
from tqdm import tqdm
from typing import Any, Dict, List

import ray
from ray.serve._private.utils import generate_request_id


logger = logging.getLogger(__file__)
logging.basicConfig(level=logging.INFO)


@dataclass
class LocustStage:
    duration_s: int
    users: int
    spawn_rate: float


@dataclass
class LocustLoadTestConfig:
    num_workers: int
    host_url: str
    auth_token: str
    data: Any
    stages: List[LocustStage]
    wait_for_workers_timeout_s: float = 600


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
    response_time_s: float
    start_time_s: float


class LocustClient:
    def __init__(
        self,
        host_url: str,
        token: str,
        data: Dict[str, Any] = None,
    ):
        from locust import task, constant, events, FastHttpUser
        from locust.contrib.fasthttp import FastResponse

        self.errors = []

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
                if exception:
                    request_id = context["request_id"]
                    response.encoding = "utf-8"
                    err = FailedRequest(
                        request_id=request_id,
                        status_code=response.status_code,
                        exception=response.text,
                        response_time_s=response_time,
                        start_time_s=start_time,
                    )
                    self.errors.append(err)
                    print(
                        f"Request '{request_id}' failed with exception: {response.text}"
                    )

        self.user_class = EndpointUser


@ray.remote(num_cpus=1)
class LocustWorker(LocustClient):
    def __init__(
        self,
        host_url: str,
        token: str,
        master_address: str,
        data: Dict[str, Any] = None,
    ):
        # NOTE(zcin): We need to lazily import locust because the driver
        # script won't connect to ray properly otherwise.
        import locust
        from locust.env import Environment
        from locust.log import setup_logging

        super().__init__(host_url=host_url, token=token, data=data)
        setup_logging("INFO")
        self.env = Environment(user_classes=[self.user_class], events=locust.events)
        self.master_address = master_address

    def run(self) -> List[Dict]:
        runner = self.env.create_worker_runner(
            master_host=self.master_address, master_port=5557
        )
        runner.greenlet.join()
        return self.errors


@ray.remote(num_cpus=1)
class LocustMaster(LocustClient):
    def __init__(
        self,
        host_url: str,
        token: str,
        expected_num_workers: int,
        stages: List[LocustStage],
        wait_for_workers_timeout_s: float,
    ):
        # NOTE(zcin): We need to lazily import locust because the driver
        # script won't connect to ray properly otherwise.
        import locust
        from locust import LoadTestShape
        from locust.env import Environment
        from locust.log import setup_logging

        super().__init__(host_url=host_url, token=token)
        setup_logging("INFO")

        self.stats_in_stages: List[PerformanceStats] = []

        class StagesShape(LoadTestShape):
            curr_stage_ix = 0

            def tick(cls):
                run_time = cls.get_run_time()
                prefix_time = 0
                for i, stage in enumerate(stages):
                    prefix_time += stage.duration_s

                    if run_time < prefix_time:
                        if i != cls.curr_stage_ix:
                            self.on_stage_finished()
                            cls.curr_stage_ix = i

                        current_stage = stages[cls.curr_stage_ix]
                        return current_stage.users, current_stage.spawn_rate

                # End of stage test
                self.on_stage_finished()

        self.master_env = Environment(
            user_classes=[self.user_class],
            shape_class=StagesShape(),
            events=locust.events,
        )
        self.expected_num_workers = expected_num_workers
        self.wait_for_workers_timeout_s = wait_for_workers_timeout_s
        self.master_runner = None

    def on_stage_finished(self):
        stats_entry_key = ("", "GET")
        stats_entry = self.master_runner.stats.entries.get(stats_entry_key)

        self.stats_in_stages.append(
            PerformanceStats(
                p50_latency=stats_entry.get_current_response_time_percentile(0.5),
                p90_latency=stats_entry.get_current_response_time_percentile(0.9),
                p99_latency=stats_entry.get_current_response_time_percentile(0.99),
                rps=stats_entry.current_rps,
            )
        )

    def run(self):
        import gevent
        from locust.stats import (
            get_stats_summary,
            get_percentile_stats_summary,
            get_error_report_summary,
            stats_history,
            stats_printer,
        )

        self.master_runner = self.master_env.create_master_runner("*", 5557)

        start = time.time()
        while len(self.master_runner.clients.ready) < self.expected_num_workers:
            if time.time() - start > self.wait_for_workers_timeout_s:
                raise RuntimeError(
                    f"Timed out waiting for {self.expected_num_workers} workers to "
                    "connect to Locust master."
                )

            print(
                f"Waiting for workers to be ready, "
                f"{len(self.master_runner.clients.ready)} "
                f"of {self.expected_num_workers} ready."
            )
            time.sleep(1)

        # Periodically output current stats (each entry is aggregated
        # stats over the past 10 seconds, by default)
        gevent.spawn(stats_printer(self.master_env.stats))
        gevent.spawn(stats_history, self.master_runner)

        # Start test & wait for the shape test to finish
        self.master_runner.start_shape()
        self.master_runner.shape_greenlet.join()
        # Send quit signal to all locust workers
        self.master_runner.quit()

        # Print stats
        for line in get_stats_summary(self.master_runner.stats, current=False):
            print(line)
        # Print percentile stats
        for line in get_percentile_stats_summary(self.master_runner.stats):
            print(line)
        # Print error report
        if self.master_runner.stats.errors:
            for line in get_error_report_summary(self.master_runner.stats):
                print(line)

        stats_entry_key = ("", "GET")
        stats_entry = self.master_runner.stats.entries.get(stats_entry_key)
        return LocustTestResults(
            history=self.master_runner.stats.history,
            total_requests=self.master_runner.stats.num_requests,
            num_failures=self.master_runner.stats.num_failures,
            avg_latency=stats_entry.avg_response_time,
            p50_latency=stats_entry.get_response_time_percentile(0.5),
            p90_latency=stats_entry.get_response_time_percentile(0.9),
            p99_latency=stats_entry.get_response_time_percentile(0.99),
            avg_rps=stats_entry.total_rps,
            stats_in_stages=self.stats_in_stages,
        )


def run_locust_load_test(config: LocustLoadTestConfig) -> LocustTestResults:
    """Runs a Locust load test against a service.

    Returns:
        Performance results (e.g. throughput and latency) from the test.
    Raises:
        RuntimeError if any requests failed during the load test.
    """

    logger.info(f"Spawning {config.num_workers} Locust worker Ray tasks.")
    master_address = ray.util.get_node_ip_address()
    worker_refs = []

    # Start Locust workers
    for _ in tqdm(range(config.num_workers)):
        locust_worker = LocustWorker.remote(
            host_url=config.host_url,
            token=config.auth_token,
            master_address=master_address,
            data=config.data,
        )
        worker_refs.append(locust_worker.run.remote())

    # Start Locust master
    master_worker = LocustMaster.remote(
        host_url=config.host_url,
        token=config.auth_token,
        expected_num_workers=config.num_workers,
        stages=config.stages,
        wait_for_workers_timeout_s=config.wait_for_workers_timeout_s,
    )
    master_ref = master_worker.run.remote()

    # Collect results and metrics
    stats: LocustTestResults = ray.get(master_ref)
    errors = sorted(chain(*ray.get(worker_refs)), key=lambda e: e.start_time_s)

    # If there were any requests that failed, raise error.
    if stats.num_failures > 0:
        errors_json = [asdict(err) for err in errors]
        raise RuntimeError(
            f"There were failed requests: {json.dumps(errors_json, indent=4)}"
        )

    return stats
