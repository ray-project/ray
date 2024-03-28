import time
from typing import Any, Dict, List

import ray


class LocustClient:
    def __init__(
        self,
        host_url: str,
        token: str,
        stages: List[Dict] = None,
        data: Dict[str, Any] = None,
    ):
        from locust import task, constant, events, FastHttpUser, LoadTestShape
        from locust.contrib.fasthttp import FastResponse

        stages = stages or []
        self.errors = []

        class EndpointUser(FastHttpUser):
            wait_time = constant(0)
            failed_requests = []
            host = host_url

            @task
            def test(self):
                headers = {"Authorization": f"Bearer {token}"} if token else None
                with self.client.get(
                    "", headers=headers, json=data, catch_response=True
                ) as r:
                    if r.status_code == 200:
                        r.request_meta["context"]["request_id"] = r.headers[
                            "x-request-id"
                        ]

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
                    err_data = {
                        "request_id": request_id,
                        "status_code": response.status_code,
                        "exception": response.text,
                        "response_time": response_time,
                        "start_time": start_time,
                    }
                    self.errors.append(err_data)
                    print(
                        f"Request '{request_id}' failed with exception: {response.text}"
                    )

        class StagesShape(LoadTestShape):
            def tick(self):
                run_time = self.get_run_time()
                for stage in stages:
                    if run_time < stage["duration"]:
                        tick_data = (stage["users"], stage["spawn_rate"])
                        return tick_data
                return None

        self.user_class = EndpointUser
        self.shape_class = StagesShape


@ray.remote(num_cpus=1)
class LocustWorker(LocustClient):
    def __init__(
        self,
        host_url: str,
        token: str,
        master_address: str,
        data: Dict[str, Any] = None,
    ):
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
        stages: List[Dict],
    ):
        from locust.env import Environment
        from locust.log import setup_logging
        import locust

        super().__init__(host_url=host_url, token=token, stages=stages)
        setup_logging("INFO")

        self.master_env = Environment(
            user_classes=[self.user_class],
            shape_class=self.shape_class(),
            events=locust.events,
        )
        self.expected_num_workers = expected_num_workers

    def run(self):
        import gevent
        from locust.stats import (
            get_stats_summary,
            get_percentile_stats_summary,
            get_error_report_summary,
            stats_history,
            stats_printer,
        )

        master_runner = self.master_env.create_master_runner("*", 5557)

        while len(master_runner.clients.ready) < self.expected_num_workers:
            print(
                f"Waiting for workers to be ready, {len(master_runner.clients.ready)} "
                f"of {self.expected_num_workers} ready."
            )
            time.sleep(1)

        # Stats stuff
        gevent.spawn(stats_printer(self.master_env.stats))
        gevent.spawn(stats_history, master_runner)

        # Start test
        master_runner.start_shape()
        master_runner.shape_greenlet.join()
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
        return {
            "history": master_runner.stats.history,
            "total_requests": master_runner.stats.num_requests,
            "num_failures": master_runner.stats.num_failures,
            "avg_latency": stats_entry.avg_response_time,
            "p50_latency": stats_entry.get_current_response_time_percentile(0.5),
            "p90_latency": stats_entry.get_current_response_time_percentile(0.9),
            "p99_latency": stats_entry.get_current_response_time_percentile(0.99),
            "avg_rps": stats_entry.total_rps,
        }
