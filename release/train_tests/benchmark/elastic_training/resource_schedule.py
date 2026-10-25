from dataclasses import dataclass
from enum import Enum
import logging
import random
import time
from typing import List
import uuid

import psutil
import ray
from ray.data._internal.cluster_autoscaler import (
    ResourceRequestPriority,
    get_or_create_autoscaling_coordinator,
)
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from ray.util.state import list_actors


logger = logging.getLogger(__name__)


@ray.remote(num_cpus=0)
def kill_process(pid):
    proc = psutil.Process(pid)
    proc.kill()


class MockResourceRequestPriority(Enum):
    OVERRIDE = ResourceRequestPriority.HIGH.value + 1


@dataclass
class ResourceAvailabilityEvent:
    time_s: int
    resource_units: int


class ResourceAvailabilityUpdater:
    def __init__(self, starting_resource_units: int = 0, resource_key: str = "GPU"):
        self._starting_resource_units = starting_resource_units
        self._resource_key = resource_key

    def execute_schedule(self, schedule: List[ResourceAvailabilityEvent]):
        pass

    def shutdown(self):
        pass


class MockResourceAvailabilityUpdater(ResourceAvailabilityUpdater):
    def __init__(self, starting_resource_units: int = 0, resource_key: str = "GPU"):
        super().__init__(starting_resource_units, resource_key)

        self._coord = get_or_create_autoscaling_coordinator()
        self._clear_all_requests()

        logging.basicConfig(level=logging.INFO)
        logger.info(
            "Initializing resource availability: '%s': %s",
            resource_key,
            starting_resource_units,
        )

        self._total_resource_units = int(ray.cluster_resources()[resource_key])
        self._dummy_requester_ids = [
            self._get_requester_id()
            for _ in range(self._total_resource_units - starting_resource_units)
        ]
        self._request(self._dummy_requester_ids)

    def _request(self, requester_ids):
        futs = []
        for requester_id in requester_ids:
            fut = self._coord.request_resources.remote(
                requester_id=requester_id,
                resources=[{self._resource_key: 1.0}],
                expire_after_s=10000,
                priority=MockResourceRequestPriority.OVERRIDE,
            )
            futs.append(fut)
        ray.get(futs)

    def _cancel(self, requester_ids):
        futs = []
        for requester_id in requester_ids:
            fut = self._coord.cancel_request.remote(requester_id=requester_id)
            futs.append(fut)
        ray.get(futs)

    def _clear_all_requests(self):
        def clear_all_requests(coord_self):
            coord_self._ongoing_reqs = {}

        ray.get(self._coord.__ray_call__.remote(clear_all_requests))

    def _get_requester_id(self):
        return f"dummy_{uuid.uuid4().hex[:6]}"

    def _kill_random_train_worker(self):
        actors = list_actors(
            filters=[("class_name", "=", "RayTrainWorker"), ("state", "=", "ALIVE")]
        )
        if not actors:
            return

        actor_to_kill = random.choice(actors)
        logger.info("Killing random train worker: %s", actor_to_kill)
        strategy = NodeAffinitySchedulingStrategy(
            node_id=actor_to_kill.node_id, soft=False
        )
        ray.get(
            kill_process.options(scheduling_strategy=strategy).remote(actor_to_kill.pid)
        )

    def execute_schedule(self, schedule: List[ResourceAvailabilityEvent]):
        schedule_str = " -> ".join(
            f"({event.time_s:.0f}s, {self._resource_key}: {event.resource_units})"
            for event in schedule
        )
        logger.info("Executing availability schedule: %s", schedule_str)

        start_time = time.time()

        for event in schedule:
            curr_time_s = time.time() - start_time
            time.sleep(max(0, event.time_s - curr_time_s))

            logger.info("Executing scheduled event: %s", event)

            curr_withheld = len(self._dummy_requester_ids)
            curr_available = self._total_resource_units - curr_withheld
            if curr_available == event.resource_units:
                logger.info(
                    "No change in availability: %s -> %s",
                    curr_available,
                    event.resource_units,
                )
                continue
            if curr_available > event.resource_units:
                num_units_to_withhold = curr_available - event.resource_units
                new_requesters = [
                    self._get_requester_id() for _ in range(num_units_to_withhold)
                ]
                logger.info(
                    "Reducing availability from %s to %s",
                    curr_available,
                    event.resource_units,
                )

                # If reducing resources, kill a worker process to trigger recovery.
                self._kill_random_train_worker()
                self._request(new_requesters)
                self._dummy_requester_ids += new_requesters
            else:
                num_to_cancel = event.resource_units - curr_available
                self._dummy_requester_ids, ids_to_cancel = (
                    self._dummy_requester_ids[num_to_cancel:],
                    self._dummy_requester_ids[:num_to_cancel],
                )
                logger.info(
                    "Increasing availability from %s to %s",
                    curr_available,
                    event.resource_units,
                )
                self._cancel(ids_to_cancel)

    def shutdown(self):
        self._cancel(self._dummy_requester_ids)


def generate_schedule(
    resource_availability_options: list,
    duration_s: int = 60,
    interval_s: int = 5,
    seed: int = 42,
) -> List[ResourceAvailabilityEvent]:
    random.seed(seed)
    num_updates = duration_s // interval_s

    curr_idx = random.choice(range(len(resource_availability_options)))
    schedule = [
        ResourceAvailabilityEvent(
            time_s=0, resource_units=resource_availability_options[curr_idx]
        )
    ]

    for i in range(1, num_updates):
        # Weights are chosen to bias schedules towards the max workers.
        weights = None
        if curr_idx == 0:
            choices = [0, 1]
        elif curr_idx == len(resource_availability_options) - 1:
            choices = [-1, 0]
            weights = [20, 80]
        else:
            choices = [-1, 0, 1]
            weights = [25, 25, 50]

        random_update = random.choices(choices, weights=weights)[0]
        curr_idx += random_update
        schedule.append(
            ResourceAvailabilityEvent(
                time_s=i * interval_s,
                resource_units=resource_availability_options[curr_idx],
            )
        )

    return schedule
