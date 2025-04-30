import subprocess
from ray import serve

import os
import json
import asyncio
import enum
import logging
import math
import random
import time
from collections import defaultdict, deque
from typing import (
    AsyncGenerator,
    Callable,
    DefaultDict,
    Deque,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Any
)

from ray.actor import ActorHandle
from ray.exceptions import ActorDiedError, ActorUnavailableError
from ray.serve._private.common import (
    DeploymentHandleSource,
    DeploymentID,
    ReplicaID,
    ReplicaQueueLengthInfo,
    RequestMetadata,
    RunningReplicaInfo,
)
from ray.serve._private.constants import (
    RAY_SERVE_MAX_QUEUE_LENGTH_RESPONSE_DEADLINE_S,
    RAY_SERVE_MULTIPLEXED_MODEL_ID_MATCHING_TIMEOUT_S,
    RAY_SERVE_QUEUE_LENGTH_RESPONSE_DEADLINE_S,
    SERVE_LOGGER_NAME,
)
from ray.serve._private.replica_scheduler.common import (
    PendingRequest,
    ReplicaQueueLengthCache,
)
from ray.serve._private.replica_scheduler.replica_scheduler import ReplicaScheduler
from ray.serve._private.replica_scheduler.replica_wrapper import RunningReplica
from ray.util import metrics

logger = logging.getLogger(SERVE_LOGGER_NAME)


class LocalityScope(str, enum.Enum):
    NODE = "NODE"
    AVAILABILITY_ZONE = "AVAILABILITY_ZONE"


class PrefixAwareReplicaScheduler(ReplicaScheduler):
    """Chooses a replica for each request using the "power of two choices" procedure.

    Requests are scheduled in FIFO order.

    When a request comes in, two candidate replicas are chosen randomly. Each replica
    is sent a control message to fetch its queue length.

    The replica responds with two items: (queue_len, accepted). Only replicas that
    accept the request are considered; between those, the one with the lower queue
    length is chosen.

    In the case when neither replica accepts the request (e.g., their queues are full),
    the procedure is repeated with backoff. This backoff repeats indefinitely until a
    replica is chosen, so the caller should use timeouts and cancellation to avoid
    hangs.

    Each request being scheduled may spawn an independent task that runs the scheduling
    procedure concurrently. This task will not necessarily satisfy the request that
    started it (in order to maintain the FIFO order). The total number of tasks is
    capped at (2 * num_replicas).
    """

    # The sequence of backoff timeouts to use when all replicas' queues are full.
    # The last item in the list is the max timeout and will be used repeatedly.
    backoff_sequence_s = [0, 0.05, 0.1, 0.15, 0.2, 0.5, 1.0]

    # Deadline for replicas to respond with their queue length. If the response isn't
    # received within this deadline, the replica will not be considered.
    # If this deadline is repeatedly missed, it will be exponentially increased up to
    # the maximum configured here.
    queue_len_response_deadline_s = RAY_SERVE_QUEUE_LENGTH_RESPONSE_DEADLINE_S
    max_queue_len_response_deadline_s = RAY_SERVE_MAX_QUEUE_LENGTH_RESPONSE_DEADLINE_S

    # Hard limit on the maximum number of scheduling tasks to run. Having too many of
    # these tasks can cause stability issue due to too much load on the local process
    # and many too requests in flight to fetch replicas' queue lengths.
    max_num_scheduling_tasks_cap = 50

    def __init__(
        self,
        deployment_id: DeploymentID,
        handle_source: DeploymentHandleSource,
        prefer_local_node_routing: bool = False,
        prefer_local_az_routing: bool = False,
        self_node_id: Optional[str] = None,
        self_actor_id: Optional[str] = None,
        self_actor_handle: Optional[ActorHandle] = None,
        self_availability_zone: Optional[str] = None,
        use_replica_queue_len_cache: bool = False,
        get_curr_time_s: Optional[Callable[[], float]] = None,
        create_replica_wrapper_func: Optional[
            Callable[[RunningReplicaInfo], RunningReplica]
        ] = None,
        # scheduler_params: Optional[Dict[str, Any]] = None,
    ):
        # Dictionary to track load distribution over time
        self._load_distribution: Dict[float, Dict[str, int]] = {}
        self._benchmark_start_time = 0.0
        self._zero_load_count = 0
        self._load_tracking_task = None
        self._num_requests_seen = 0
        self._timing_output_file = f"/home/ray/default/work/_testing/results/deployment_overhead/{time.strftime('%H-%M-%S')}_id_{random.randint(0, 1000000)}.json"

        # Variables to track prefix match rates / replica
        self._prefix_match_rates: Dict[str, List[float]] = {}
        
        # Extracting tree_deployment from scheduler_params
        self._tree_deployment = serve.get_deployment_handle("TreeDeployment", app_name="llm_app")
        if self._tree_deployment is None:
            raise ValueError("Tree deployment was not found")
        self.balance_abs_threshold = 10
        self.balance_rel_threshold = 0.5

        self._deployment_id = deployment_id
        self._handle_source = handle_source
        self._prefer_local_node_routing = prefer_local_node_routing
        self._prefer_local_az_routing = prefer_local_az_routing
        self._self_node_id = self_node_id
        self._self_actor_id = self_actor_id # Why is this needed?
        self._self_actor_handle = self_actor_handle
        self._self_availability_zone = self_availability_zone
        self._use_replica_queue_len_cache = use_replica_queue_len_cache
        self._create_replica_wrapper_func = create_replica_wrapper_func

        # Current replicas available to be scheduled.
        # Updated via `update_replicas`.
        self._replica_id_set: Set[ReplicaID] = set()
        self._replicas: Dict[ReplicaID, RunningReplica] = {}
        self._replica_queue_len_cache = ReplicaQueueLengthCache(
            get_curr_time_s=get_curr_time_s,
        )

        # NOTE(edoakes): Python 3.10 removed the `loop` parameter to `asyncio.Event`.
        # Now, the `asyncio.Event` will call `get_running_loop` in its constructor to
        # determine the loop to attach to. This class can be constructed for the handle
        # from a different loop than it uses for scheduling, so we need to construct it
        # lazily to avoid an error due to the event being attached to the wrong loop.
        self._lazily_constructed_replicas_updated_event: Optional[asyncio.Event] = None
        self._lazily_fetched_loop: Optional[asyncio.AbstractEventLoop] = None

        # Colocated replicas (e.g. wrt node, AZ)
        self._colocated_replica_ids: DefaultDict[
            LocalityScope, Set[ReplicaID]
        ] = defaultdict(set)
        self._multiplexed_model_id_to_replica_ids: DefaultDict[
            str, Set[ReplicaID]
        ] = defaultdict(set)

        # When there is no match for a multiplexed model id, we will try to fallback
        # to all replicas immediately. This set is used to make sure we only fallback
        # once for concurrent requests for the same model id.
        # Whenever there is a match, we will remove the the model id from this set.
        self._multiplexed_model_id_fallback_match: Set[str] = set()

        # Tasks running the scheduling loop. The size of this set may vary over time
        # as new tasks will be scheduled when a request comes in or new replicas are
        # added, but it will not exceed self.max_num_scheduling_tasks.
        self._scheduling_tasks: Set[asyncio.Task] = set()

        # We keep two separate queues of pending requests:
        # - self._pending_requests_to_fulfill is a queue that will be used to fulfill
        # requests in FIFO order by scheduling tasks once they've acquired a replica.
        # To avoid long tail latencies due to backoff, the scheduling task started by
        # a given request may not be the one to fulfill it.
        # - self._pending_requests_to_schedule is a queue that is used for tasks to
        # best-effort grab the metadata of requests waiting to be fulfilled. This is
        # currently used for scheduling tasks to know which multiplexed model IDs they
        # should be trying to get replicas for.
        self._pending_requests_to_fulfill: Deque[PendingRequest] = deque()
        self._pending_requests_to_schedule: Deque[PendingRequest] = deque()

        # Prepare scheduler metrics.
        self.num_scheduling_tasks_gauge = metrics.Gauge(
            "serve_num_scheduling_tasks",
            description="The number of request scheduling tasks in the router.",
            tag_keys=("app", "deployment", "actor_id"),
        ).set_default_tags(
            {
                "app": self._deployment_id.app_name,
                "deployment": self._deployment_id.name,
                "actor_id": self_actor_id if self_actor_id else "",
            }
        )
        self.num_scheduling_tasks_gauge.set(0)

        self.num_scheduling_tasks_in_backoff = 0
        self.num_scheduling_tasks_in_backoff_gauge = metrics.Gauge(
            "serve_num_scheduling_tasks_in_backoff",
            description=(
                "The number of request scheduling tasks in the router "
                "that are undergoing backoff."
            ),
            tag_keys=("app", "deployment", "actor_id"),
        ).set_default_tags(
            {
                "app": self._deployment_id.app_name,
                "deployment": self._deployment_id.name,
                "actor_id": self_actor_id if self_actor_id else "",
            }
        )
        self.num_scheduling_tasks_in_backoff_gauge.set(
            self.num_scheduling_tasks_in_backoff
        )
        self._scheduling_assignments_file_path = f"/home/ray/default/work/_testing/results/scheduling_logs/prefix_aware_{int(time.time())}_id_{random.randint(0, 1000000)}.csv"
        self._scheduling_queue_overhead_file_path = f"/home/ray/default/work/_testing/results/scheduling_logs/prefix_aware_queue_overhead_{int(time.time())}_id_{random.randint(0, 1000000)}.csv"
        with open(self._scheduling_assignments_file_path, "w") as f:
            f.write("scheduling_decision,original_request_id,matched_replica_request_id\n")  # Clear the log file at start
        with open(self._scheduling_queue_overhead_file_path, "w") as f:
            f.write("num_searched,search_duration\n")  # Clear the log file at start

    @property
    def _event_loop(self) -> asyncio.AbstractEventLoop:
        if self._lazily_fetched_loop is None:
            self._lazily_fetched_loop = asyncio.get_running_loop()

        return self._lazily_fetched_loop

    @property
    def _replicas_updated_event(self) -> asyncio.Event:
        """Lazily construct `asyncio.Event`.

        See comment for self._lazily_constructed_replicas_updated_event.
        """
        if self._lazily_constructed_replicas_updated_event is None:
            self._lazily_constructed_replicas_updated_event = asyncio.Event()

        return self._lazily_constructed_replicas_updated_event

    @property
    def num_pending_requests(self) -> int:
        """Current number of requests pending assignment."""
        return len(self._pending_requests_to_fulfill)

    @property
    def curr_num_scheduling_tasks(self) -> int:
        """Current number of scheduling tasks running."""
        return len(self._scheduling_tasks)

    @property
    def max_num_scheduling_tasks(self) -> int:
        """Max number of scheduling tasks to run at any time."""
        return min(self.max_num_scheduling_tasks_cap, 2 * len(self._replicas))

    @property
    def target_num_scheduling_tasks(self) -> int:
        """Target number of scheduling tasks to be running based on pending requests.

        This will never exceed `self.max_num_scheduling_tasks`.
        """
        return min(self.num_pending_requests, self.max_num_scheduling_tasks)

    @property
    def curr_replicas(self) -> Dict[ReplicaID, RunningReplica]:
        return self._replicas

    @property
    def app_name(self) -> str:
        return self._deployment_id.app_name

    @property
    def replica_queue_len_cache(self) -> ReplicaQueueLengthCache:
        return self._replica_queue_len_cache

    async def _track_load_distribution(self):
        """Track load + vLLM metrics by WorkerId every 0.1s, save to JSON at end."""
        try:
            self._benchmark_start_time = time.time()
            print("Beginning to track load distribution immediately")

            self._vllm_metrics_over_time = {}
            import requests
            session = requests.Session()
            while True:
                await asyncio.sleep(0.1)
                current_time = time.time()
                elapsed = round(current_time - self._benchmark_start_time, 2)
                total_load = 0

                # === Load distribution ===
                result = await self._probe_queue_lens(self._replicas.values(), 0)
                result_dict = {r.replica_id: q for r, q in result}
                current_load = {}

                for replica_id, replica in self._replicas.items():
                    queue_len = result_dict[replica_id] or 0
                    current_load[replica_id.unique_id] = queue_len
                    total_load += queue_len

                self._load_distribution[elapsed] = current_load

                # === vLLM metrics via curl ===
                try:
                    response = session.get("http://localhost:5001/metrics")
                    output = response.text
                    # output = subprocess.check_output(["curl", "-s", "http://localhost:5001/metrics"]).decode("utf-8")
                    lines = output.strip().split("\n")
                    current_vllm_metrics = {}

                    for line in lines:
                        if line.startswith("#") or "vllm" not in line:
                            continue

                        parts = line.split()
                        if len(parts) != 2:
                            continue

                        metric_line, value = parts
                        try:
                            value = float(value)
                        except ValueError:
                            continue

                        # Parse metric name and labels
                        if "{" in metric_line:
                            name, label_str = metric_line.split("{", 1)
                            # if name == "ray_vllm:num_requests_running":
                            #     total_load += value
                            label_str = label_str.rstrip("}")
                            labels = dict(item.split("=") for item in label_str.split(","))
                            labels = {k: v.strip('"') for k, v in labels.items()}
                        else:
                            name = metric_line
                            labels = {}

                        # Extract WorkerId
                        worker_id = labels.get("WorkerId", "unknown")

                        # Keep only important label keys
                        important_keys = {"le", "model_name"}
                        filtered_labels = {k: v for k, v in labels.items() if k in important_keys}

                        # Append important label suffix to metric name
                        if filtered_labels:
                            label_suffix = ",".join(f"{k}={v}" for k, v in sorted(filtered_labels.items()))
                            metric_key = f"{name}{{{label_suffix}}}"
                        else:
                            metric_key = name

                        if worker_id not in current_vllm_metrics:
                            current_vllm_metrics[worker_id] = {}
                        current_vllm_metrics[worker_id][metric_key] = value

                    self._vllm_metrics_over_time[elapsed] = current_vllm_metrics

                except Exception as e:
                    print(f"[WARN] Failed to curl or parse /metrics: {e}")
                    
                # === End condition ===
                # if self._num_requests_seen > 90:
                if self._num_requests_seen > 10 and total_load == 0:
                    self._zero_load_count += 1
                    if self._zero_load_count >= 2:
                        print("Benchmark ended, writing data to disk")

                        # Dump load distribution
                        results_dir = "/home/ray/default/work/ray/_prefix_aware_playground/inside_ray/results/load_distributions"
                        os.makedirs(results_dir, exist_ok=True)
                        with open(os.path.join(results_dir, f"prefix_aware_{int(time.time())}_id_{random.randint(0, 1000000)}.json"), "w") as f:
                            json.dump(self._load_distribution, f, indent=2)

                        # Dump prefix match rates
                        prefix_match_dir = "/home/ray/default/work/ray/_prefix_aware_playground/inside_ray/results/prefix_match_rates"
                        os.makedirs(prefix_match_dir, exist_ok=True)
                        with open(os.path.join(prefix_match_dir, f"prefix_aware_{int(time.time())}_id_{random.randint(0, 1000000)}.json"), "w") as f:
                            json.dump(self._prefix_match_rates, f, indent=2)

                        # Dump vLLM metrics
                        metrics_dir = "/home/ray/default/work/ray/_prefix_aware_playground/inside_ray/results/vllm_metrics"
                        os.makedirs(metrics_dir, exist_ok=True)
                        with open(os.path.join(metrics_dir, f"prefix_aware_{int(time.time())}_id_{random.randint(0, 1000000)}.json"), "w") as f:
                            json.dump(self._vllm_metrics_over_time, f, indent=2)

                        break
                else:
                    self._zero_load_count = 0
        except Exception as e:
            print(f"Error in load tracking: {e}")
        finally:
            self._load_tracking_task = None

    def create_replica_wrapper(
        self, replica_info: RunningReplicaInfo
    ) -> RunningReplica:
        return self._create_replica_wrapper_func(replica_info)

    def on_replica_actor_died(self, replica_id: ReplicaID):
        """Drop replica from replica set so it's not considered for future requests."""
        self._replicas.pop(replica_id, None)
        self._replica_id_set.discard(replica_id)
        for id_set in self._colocated_replica_ids.values():
            id_set.discard(replica_id)

    def on_replica_actor_unavailable(self, replica_id: ReplicaID):
        """Invalidate cache entry so active probing is required for the next request."""
        self._replica_queue_len_cache.invalidate_key(replica_id)

    def on_new_queue_len_info(
        self, replica_id: ReplicaID, queue_len_info: ReplicaQueueLengthInfo
    ):
        """Update queue length cache with new info received from replica."""
        if self._use_replica_queue_len_cache:
            self._replica_queue_len_cache.update(
                replica_id, queue_len_info.num_ongoing_requests
            )

    def update_replicas(self, replicas: List[RunningReplica]):
        """Update the set of available replicas to be considered for scheduling.

        When the set of replicas changes, we may spawn additional scheduling tasks
        if there are pending requests.
        """
        new_replicas = {}
        new_replica_id_set = set()
        new_colocated_replica_ids = defaultdict(set)
        new_multiplexed_model_id_to_replica_ids = defaultdict(set)

        for r in replicas:
            # If on the proxy, replica needs to call back into the proxy with
            # `receive_asgi_messages` which can be blocked when GCS is down.
            # To prevent that from happening, push proxy handle eagerly
            if (
                self._handle_source == DeploymentHandleSource.PROXY
                and r.replica_id not in self._replicas
            ):
                r.push_proxy_handle(self._self_actor_handle)

            new_replicas[r.replica_id] = r
            new_replica_id_set.add(r.replica_id)
            if self._self_node_id is not None and r.node_id == self._self_node_id:
                new_colocated_replica_ids[LocalityScope.NODE].add(r.replica_id)
            if (
                self._self_availability_zone is not None
                and r.availability_zone == self._self_availability_zone
            ):
                new_colocated_replica_ids[LocalityScope.AVAILABILITY_ZONE].add(
                    r.replica_id
                )
            for model_id in r.multiplexed_model_ids:
                new_multiplexed_model_id_to_replica_ids[model_id].add(r.replica_id)

        if self._replica_id_set != new_replica_id_set:
            replica_id_set_strs = {r.unique_id for r in new_replica_id_set}
            logger.info(
                f"Got updated replicas for {self._deployment_id}: "
                f"{replica_id_set_strs}.",
                extra={"log_to_stderr": False},
            )

        # Get list of new replicas
        new_ids = new_replica_id_set - self._replica_id_set
        replicas_to_ping = [new_replicas.get(id) for id in new_ids]

        self._replicas = new_replicas
        self._replica_id_set = new_replica_id_set
        self._colocated_replica_ids = new_colocated_replica_ids
        self._multiplexed_model_id_to_replica_ids = (
            new_multiplexed_model_id_to_replica_ids
        )
        self._replica_queue_len_cache.remove_inactive_replicas(
            active_replica_ids=new_replica_id_set
        )
        # Populate cache for new replicas
        self._event_loop.create_task(self._probe_queue_lens(replicas_to_ping, 0))
        self._replicas_updated_event.set()
        self.maybe_start_scheduling_tasks()

        # # Start load tracking task if it's not already running and we have replicas
        # if self._load_tracking_task is None and len(self._replicas) > 0:
        #     self._load_tracking_task = self._event_loop.create_task(self._track_load_distribution())

    def _get_replica_ids_with_fewest_multiplexed_models(self) -> Set[str]:
        """Get the set of replicas that have the fewest multiplexed models loaded."""
        candidates = set()
        sorted_replicas = sorted(
            self._replicas.values(), key=lambda x: len(x.multiplexed_model_ids)
        )
        least_num_multiplexed_model_ids = math.inf
        for replica in sorted_replicas:
            if len(replica.multiplexed_model_ids) <= least_num_multiplexed_model_ids:
                candidates.add(replica.replica_id)
                least_num_multiplexed_model_ids = len(replica.multiplexed_model_ids)
            else:
                break

        return candidates

    async def choose_two_replicas_with_backoff(
        self,
        request_metadata: Optional[RequestMetadata] = None,
    ) -> AsyncGenerator[List[RunningReplicaInfo], None]:
        """Generator that repeatedly chooses (at most) two random available replicas.

        In the first iteration, only replicas colocated on the same node as this router
        will be considered. If those are occupied, the full set of replicas will be
        considered on subsequent iterations.

        For multiplexing, this will first attempt to choose replicas that have the
        requested model ID for a configured timeout. If no replicas with the matching
        model ID are available after that timeout, it will fall back to the regular
        procedure.

        After each iteration, there will be an increasing backoff sleep time (dictated
        by `self.backoff_sequence_s`). The caller should exit the generator to reset the
        backoff sleep time.
        """

        try:
            backoff_index = 0
            entered_backoff = False

            tried_same_az = False
            tried_same_node = False

            multiplexed_start_matching_time = None
            multiplexed_matching_timeout = random.uniform(
                RAY_SERVE_MULTIPLEXED_MODEL_ID_MATCHING_TIMEOUT_S,
                RAY_SERVE_MULTIPLEXED_MODEL_ID_MATCHING_TIMEOUT_S * 2,
            )
            tried_fewest_multiplexed_models = False

            while True:
                # If no replicas are available, wait until `update_replicas` is called.
                while len(self._replicas) == 0:
                    logger.info(
                        "No replicas are currently available for "
                        f"{self._deployment_id}.",
                        extra={"log_to_stderr": False},
                    )
                    self._replicas_updated_event.clear()
                    await self._replicas_updated_event.wait()
                    logger.info(
                        f"New replicas are available for {self._deployment_id}, "
                        "attempting to schedule queued requests.",
                        extra={"log_to_stderr": False},
                    )

                if multiplexed_start_matching_time is None:
                    multiplexed_start_matching_time = time.time()

                candidate_replica_ids = None
                if (
                    request_metadata is not None
                    and request_metadata.multiplexed_model_id
                ):
                    # Get candidates for multiplexed model ID.
                    if (
                        time.time() - multiplexed_start_matching_time
                        < multiplexed_matching_timeout
                    ):
                        candidate_replica_ids = (
                            self._multiplexed_model_id_to_replica_ids.get(
                                request_metadata.multiplexed_model_id, None
                            )
                        )
                        if (
                            not candidate_replica_ids
                            and request_metadata.multiplexed_model_id
                            not in self._multiplexed_model_id_fallback_match
                        ):
                            # When there is no match for a multiplexed model id,
                            # first try to fall back to replicas with the fewest models.
                            candidate_replica_ids = (
                                self._get_replica_ids_with_fewest_multiplexed_models()
                            )
                            self._multiplexed_model_id_fallback_match.add(
                                request_metadata.multiplexed_model_id
                            )
                        elif candidate_replica_ids:
                            self._multiplexed_model_id_fallback_match.discard(
                                request_metadata.multiplexed_model_id
                            )
                    elif not tried_fewest_multiplexed_models:
                        # After the `multiplexed_matching_timeout` is up, first try
                        # routing to replicas that have the fewest models loaded.
                        # We only try this once to avoid deterministically retrying on
                        # the same replicas repeatedly.
                        candidate_replica_ids = (
                            self._get_replica_ids_with_fewest_multiplexed_models()
                        )
                        tried_fewest_multiplexed_models = True
                    else:
                        # If the timeout is up and we've already tried the candidates
                        # with the fewest models loaded, fall back to all replicas.
                        candidate_replica_ids = self._replica_id_set
                    should_backoff = True
                elif (
                    self._prefer_local_node_routing
                    and not tried_same_node
                    and len(self._colocated_replica_ids[LocalityScope.NODE]) > 0
                ):
                    # Attempt to schedule requests to replicas on the
                    # same node at most once
                    candidate_replica_ids = self._colocated_replica_ids[
                        LocalityScope.NODE
                    ]
                    tried_same_node = True
                    should_backoff = False
                elif (
                    self._prefer_local_az_routing
                    and not tried_same_az
                    and len(
                        self._colocated_replica_ids[LocalityScope.AVAILABILITY_ZONE]
                    )
                    > 0
                ):
                    # Attempt to schedule requests to replicas in the same
                    # AZ at most once
                    candidate_replica_ids = self._colocated_replica_ids[
                        LocalityScope.AVAILABILITY_ZONE
                    ]
                    tried_same_az = True
                    should_backoff = False
                else:
                    # On subsequent iterations or when there are no replicas on the same
                    # node or AZ, consider all available replicas.
                    candidate_replica_ids = self._replica_id_set
                    should_backoff = True

                if candidate_replica_ids:
                    chosen_ids = random.sample(
                        list(candidate_replica_ids),
                        # k=min(2, len(candidate_replica_ids))
                        k=len(candidate_replica_ids)
                    )
                    yield [self._replicas[chosen_id] for chosen_id in chosen_ids]

                # We have a slight unintended behavior when enabled locality routing
                # for both node and AZ. The intention is to try same node first,
                # then try same AZ if node fails, then try everything else until a
                # replica is found. These sequence should only help to reduce the
                # latency of the request. No backoff and sleep should be applied, until
                # we have fall into the case trying on all available replicas.
                if not should_backoff:
                    continue

                if not entered_backoff:
                    entered_backoff = True
                    self.num_scheduling_tasks_in_backoff += 1
                    self.num_scheduling_tasks_in_backoff_gauge.set(
                        self.num_scheduling_tasks_in_backoff
                    )

                await asyncio.sleep(self.backoff_sequence_s[backoff_index])
                backoff_index = min(backoff_index + 1, len(self.backoff_sequence_s) - 1)
        finally:
            if entered_backoff:
                self.num_scheduling_tasks_in_backoff -= 1
                self.num_scheduling_tasks_in_backoff_gauge.set(
                    self.num_scheduling_tasks_in_backoff
                )

    async def _probe_queue_lens(
        self,
        replicas: List[RunningReplica],
        backoff_index: int,
    ) -> List[Tuple[RunningReplica, Optional[int]]]:
        """Actively probe the queue length from each of the replicas.

        Sends an RPC to each replica to fetch its queue length, with a response deadline
        that increases exponentially in backoff.

        Returns a list of queue lengths in the same order as the replicas passed in.
        Replicas whose RPCs fail or don't respond within the deadline will have a queue
        length of `None`. Replicas that return a `RayActorError` will be removed from
        future consideration for requests.

        This method also updates the local cache of replica queue lengths according to
        the responses.
        """
        result: List[Tuple[RunningReplica, int]] = []
        if len(replicas) == 0:
            return result

        # Ensure the max deadline is always >= the initial deadline.
        max_queue_len_response_deadline_s = max(
            self.queue_len_response_deadline_s,
            self.max_queue_len_response_deadline_s,
        )

        try:
            queue_len_response_deadline_s = min(
                self.queue_len_response_deadline_s * (2**backoff_index),
                max_queue_len_response_deadline_s,
            )
        except OverflowError:
            # self.queue_len_response_deadline_s * (2**backoff_index)
            # can overflow if backoff_index gets sufficiently large (e.g.
            # 1024 when queue_len_response_deadline_s is 0.1).
            queue_len_response_deadline_s = max_queue_len_response_deadline_s

        get_queue_len_tasks = []
        for r in replicas:
            t = self._event_loop.create_task(
                r.get_queue_len(deadline_s=queue_len_response_deadline_s)
            )
            t.replica = r
            get_queue_len_tasks.append(t)

        done, pending = await asyncio.wait(
            get_queue_len_tasks,
            timeout=queue_len_response_deadline_s,
            return_when=asyncio.ALL_COMPLETED,
        )
        for t in pending:
            replica = t.replica
            result.append((replica, None))
            t.cancel()
            logger.warning(
                f"Failed to get queue length from {replica.replica_id} "
                f"within {queue_len_response_deadline_s}s. If this happens repeatedly "
                "it's likely caused by high network latency in the cluster. You can "
                "configure the deadline using the "
                "`RAY_SERVE_QUEUE_LENGTH_RESPONSE_DEADLINE_S` environment variable."
            )

        for t in done:
            replica = t.replica
            if t.exception() is not None:
                result.append((replica, None))
                msg = (
                    "Failed to fetch queue length for "
                    f"{replica.replica_id}: '{t.exception()}'"
                )
                # If we get an ActorDiedError, the replica actor has died. This
                # is not recoverable (the controller will start a new replica in its
                # place), so we should no longer consider it for requests.
                # We do not catch RayActorError here because that error can be
                # raised even when a replica is temporarily unavailable.
                # See https://github.com/ray-project/ray/issues/44185 for details.
                if isinstance(t.exception(), ActorDiedError):
                    self.on_replica_actor_died(replica.replica_id)
                    msg += " This replica will no longer be considered for requests."
                # Replica is temporarily unavailable because of network issues, or
                # replica has died but GCS is down so ActorUnavailableError will
                # be raised until the GCS recovers. For the time being, invalidate
                # the cache entry so that we don't try to send requests to this
                # replica without actively probing.
                elif isinstance(t.exception(), ActorUnavailableError):
                    self.on_replica_actor_unavailable(replica.replica_id)
                    msg = (
                        "Failed to fetch queue length for "
                        f"{replica.replica_id}. Replica is temporarily "
                        "unavailable."
                    )

                logger.warning(msg)
            else:
                queue_len = t.result()
                result.append((replica, queue_len))
                self._replica_queue_len_cache.update(replica.replica_id, queue_len)

        assert len(result) == len(replicas)
        return result

    def _get_input_text(self, pending_request: PendingRequest) -> str:
        chat_completion_request = pending_request.args[0]
        # print(f"[prefix_aware_scheduler.py: _get_input_text] Chat completion request: {chat_completion_request}")
        if hasattr(chat_completion_request, "messages"):
            messages = chat_completion_request.messages
            return "".join(msg.get("content", "") for msg in messages if "content" in msg)
        elif hasattr(chat_completion_request, "prompt"):
            return chat_completion_request.prompt
        else:
            raise ValueError("Invalid chat completion request")

    async def select_from_candidate_replicas(
        self,
        candidates: List[RunningReplica],
        backoff_index: int,
        pending_request: Optional[PendingRequest],
    ) -> Optional[RunningReplica]:
        """Chooses the best replica from the list of candidates.

        If none of the replicas can be scheduled, returns `None`.

        The queue length for each replica is first looked up in the local cache. If not
        present in the cache, the replica will be actively probed and the cache updated.

        Among replicas that respond within the deadline and don't have full queues, the
        one with the lowest queue length is chosen.
        """
        # # Artificial random delay to test scheduling mismatch
        # await asyncio.sleep(random.uniform(0.0, 0.1))
        # # End artificial random delay

        # BEGIN POW 2 LOGIC
        lowest_queue_len = math.inf
        highest_queue_len = 0
        chosen_replica_id: Optional[str] = None
        not_in_cache: List[RunningReplica] = []
        if self._use_replica_queue_len_cache:
            # Populate available queue lens from the cache.
            for r in candidates:
                queue_len = self._replica_queue_len_cache.get(r.replica_id)
                # Include replicas whose queues are full as not in the cache so we will
                # actively probe them. Otherwise we may end up in "deadlock" until their
                # cache entries expire.
                if queue_len is None or queue_len >= r.max_ongoing_requests:
                    not_in_cache.append(r)
                else:
                    highest_queue_len = max(highest_queue_len, queue_len)
                    if queue_len < lowest_queue_len:
                        lowest_queue_len = queue_len
                        chosen_replica_id = r.replica_id
        else:
            not_in_cache = candidates

        # If there is a valid replica to schedule based on the information in the
        # cache, schedule it. Else fall back to actively probing.
        if chosen_replica_id is None:
            for r, queue_len in await self._probe_queue_lens(
                not_in_cache,
                backoff_index,
            ):
                if queue_len is None:
                    # None is returned if we failed to get the queue len.
                    continue
                highest_queue_len = max(highest_queue_len, queue_len)
                if queue_len < r.max_ongoing_requests and queue_len < lowest_queue_len:
                    lowest_queue_len = queue_len
                    chosen_replica_id = r.replica_id
        elif len(not_in_cache) > 0:
            # If there are replicas without a valid cache entry, probe them in the
            # background to populate the cache.
            self._event_loop.create_task(
                self._probe_queue_lens(not_in_cache, backoff_index)
            )
        # END POW 2 LOGIC

        # BEGIN PREFIX AWARE LOGIC
        request_id = self._num_requests_seen + 1
        timing_info = {"request_id": request_id}
        # Determine if the load is imbalanced.
        if pending_request is not None:
            input_text = self._get_input_text(pending_request)
            is_imbalanced = highest_queue_len - lowest_queue_len > 10
            # if is_imbalanced:
            if False:
                pass
            else:
                timing_info["before_calling_prefix_match"] = time.time()
                matched_text, tenant_id, match_timing = await self._tree_deployment.prefix_match.remote(input_text)
                timing_info.update(match_timing)
                timing_info["after_calling_prefix_match"] = time.time()
                match_rate = len(matched_text) / len(input_text) if input_text else 0
                if match_rate < 0.1:
                    # chosen_replica = self._tree_deployment.get_smallest_tenant.remote()
                    pass
                else:
                    for replica in candidates:
                        if tenant_id == replica.replica_id:
                            chosen_replica_id = replica.replica_id
                
                # If no good match was found or match rate was too low
                if chosen_replica_id is None or chosen_replica_id == "empty":
                    pass
        # END PREFIX AWARE LOGIC

        # # BEGIN PREFIX MATCH RATE TRACKING
        # if pending_request is not None:
        #     input_text = self._get_input_text(pending_request)
        #     matched_text = await self._tree_deployment.prefix_match_tenant.remote(input_text, chosen_replica_id)
        #     # matched_text = self._tree_deployment.prefix_match_tenant(input_text, chosen_replica_id)
        #     if chosen_replica_id.unique_id not in self._prefix_match_rates:
        #         self._prefix_match_rates[chosen_replica_id.unique_id] = []
        #     if matched_text is not None:
        #         self._prefix_match_rates[chosen_replica_id.unique_id].append(len(matched_text) / len(input_text))
        #     else:
        #         self._prefix_match_rates[chosen_replica_id.unique_id].append(0.0)
        # # END PREFIX MATCH RATE TRACKING

        # BEGIN UPDATE TREE
        if pending_request is not None:
            input_text = self._get_input_text(pending_request)
            timing_info["before_calling_insert"] = time.time()
            success, insert_timing = await self._tree_deployment.insert.remote(input_text, tenant=chosen_replica_id)
            timing_info.update(insert_timing)
            timing_info["after_calling_insert"] = time.time()
        # END UPDATE TREE
        
        # # write to file takes very little time (< 1 ms)
        # with open(self._timing_output_file, "a") as f:
        #     f.write(json.dumps(timing_info) + "\n")
        self._num_requests_seen += 1
        return self._replicas.get(chosen_replica_id, None)

    def _get_pending_request_matching_metadata(
        self,
        request_metadata: Optional[RequestMetadata] = None,
    ) -> Optional[PendingRequest]:
        selected_pr = None

        # First, try to match based on multiplexed model ID:
        if request_metadata is not None and request_metadata.multiplexed_model_id:
            for pr in self._pending_requests_to_fulfill:
                if (
                    not pr.future.done()
                    and pr.metadata.multiplexed_model_id
                    == request_metadata.multiplexed_model_id
                ):
                    selected_pr = pr
                    with open(self._scheduling_assignments_file_path, "a") as f:
                        f.write(f"multiplexed_model_id,{pr.metadata.internal_request_id},{request_metadata.internal_request_id}\n")

                    break
        # # Second, try to match based on internal request ID:
        # num_searched = 0
        # search_start = time.time()
        # if selected_pr is None and request_metadata is not None and request_metadata.internal_request_id:
        #     for pr in self._pending_requests_to_fulfill:
        #         num_searched += 1
        #         if (
        #             not pr.future.done()
        #             and pr.metadata.internal_request_id
        #             == request_metadata.internal_request_id
        #         ):
        #             with open(self._scheduling_assignments_file_path, "a") as f:
        #                 f.write(f"internal_request_id,{pr.metadata.internal_request_id},{request_metadata.internal_request_id}\n")
        #             selected_pr = pr
        #             break

        # search_duration = time.time() - search_start
        # with open(self._scheduling_queue_overhead_file_path, "a") as f:
        #     f.write(f"{num_searched},{search_duration}\n")

        return selected_pr

    def fulfill_next_pending_request(
        self,
        replica: RunningReplica,
        request_metadata: Optional[RequestMetadata] = None,
    ):
        """Assign the replica to the next pending request in FIFO order.

        If a pending request has been cancelled, it will be popped from the queue
        and not assigned.
        """
        # First try to match a pending request based on the request metadata (currently
        # this only looks at the multiplexed model ID).
        matched_pending_request = self._get_pending_request_matching_metadata(
            request_metadata
        )
        if matched_pending_request is not None:
            matched_pending_request.future.set_result(replica)
            self._pending_requests_to_fulfill.remove(matched_pending_request)
            return

        # If no pending request matches the request metadata, fulfill the next in the
        # queue in FIFO order, passing over futures that have been cancelled.
        while len(self._pending_requests_to_fulfill) > 0:
            pr = self._pending_requests_to_fulfill.popleft()
            if not pr.future.done():
                with open(self._scheduling_assignments_file_path, "a") as f:
                    f.write(f"FIFO,{pr.metadata.internal_request_id},{request_metadata.internal_request_id}\n")
                pr.future.set_result(replica)
                break

    # Modify this to pass pending_request too
    def _get_next_pending_request_metadata_to_schedule(
        self,
    ) -> Optional[Tuple[RequestMetadata, PendingRequest]]:
        while len(self._pending_requests_to_schedule) > 0:
            pr = self._pending_requests_to_schedule.popleft()
            if not pr.future.done():
                return pr.metadata, pr

        return None

    async def fulfill_pending_requests(self):
        """Repeatedly tries to fulfill a pending request with an available replica.

        This is expected to be run inside a task in self._scheduling tasks.

        When a replica is found, this method will exit if the number of scheduling tasks
        has exceeded the target number. Else it will loop again to schedule another
        replica.
        """
        # print(f"[prefix_aware_scheduler.py: fulfill_pending_requests] called")
        try:
            while len(self._scheduling_tasks) <= self.target_num_scheduling_tasks:
                start_time = time.time()
                backoff_index = 0
                result = self._get_next_pending_request_metadata_to_schedule()
                if result is None:
                    request_metadata = None
                    pending_request = None
                else:
                    request_metadata, pending_request = result
                # print(f"[prefix_aware_scheduler.py: fulfill_pending_requests] Request metadata: {request_metadata}")
                # print(f"[prefix_aware_scheduler.py: fulfill_pending_requests] Pending request: {pending_request}")
                async for candidates in self.choose_two_replicas_with_backoff(
                    request_metadata
                ):
                    # print(f"[prefix_aware_scheduler.py: fulfill_pending_requests] Candidates: {candidates}")
                    # Clear out pending requests at the front of the
                    # queue that have been cancelled, then reevaluate
                    # if we need to continue this scheduling task.
                    while (
                        len(self._pending_requests_to_fulfill) > 0
                        and self._pending_requests_to_fulfill[0].future.done()
                    ):
                        self._pending_requests_to_fulfill.popleft()

                    if len(self._scheduling_tasks) > self.target_num_scheduling_tasks:
                        break

                    replica = await self.select_from_candidate_replicas(
                        candidates, backoff_index, pending_request
                    )
                    if replica is not None:
                        self.fulfill_next_pending_request(replica, request_metadata)
                        break
                    else:
                        print(f"[prefix_aware_scheduler.py: fulfill_pending_requests] No replica found for request {request_metadata.internal_request_id}")

                    backoff_index += 1
                    if backoff_index >= 50 and backoff_index % 50 == 0:
                        scheduling_time_elapsed = time.time() - start_time
                        warning_log = (
                            "Failed to schedule request after "
                            f"{backoff_index} attempts over "
                            f"{scheduling_time_elapsed:.2f}s. Retrying."
                        )
                        if request_metadata is not None:
                            warning_log += (
                                f" Request ID: {request_metadata.request_id}."
                            )
                            if request_metadata.multiplexed_model_id:
                                warning_log += (
                                    " Multiplexed model ID: "
                                    f"{request_metadata.multiplexed_model_id}."
                                )
                        logger.warning(warning_log)

        except Exception:
            logger.exception("Unexpected error in fulfill_pending_requests.")
        finally:
            self._scheduling_tasks.remove(asyncio.current_task(loop=self._event_loop))
            self.num_scheduling_tasks_gauge.set(self.curr_num_scheduling_tasks)

    def maybe_start_scheduling_tasks(self):
        """Start scheduling tasks to fulfill pending requests if necessary.

        Starts tasks so that there is at least one task per pending request
        (respecting the max number of scheduling tasks).

        In the common case, this will start a single task when a new request comes
        in for scheduling. However, in cases where the number of available replicas
        is updated or a task exits unexpectedly, we may need to start multiple.
        """
        # print(f"[prefix_aware_scheduler.py: maybe_start_scheduling_tasks] maybe_start_scheduling_tasks() called")
        tasks_to_start = (
            self.target_num_scheduling_tasks - self.curr_num_scheduling_tasks
        )
        for _ in range(tasks_to_start):
            self._scheduling_tasks.add(
                self._event_loop.create_task(self.fulfill_pending_requests())
            )
        if tasks_to_start > 0:
            self.num_scheduling_tasks_gauge.set(self.curr_num_scheduling_tasks)

    async def choose_replica_for_request(
        self, pending_request: PendingRequest, *, is_retry: bool = False
    ) -> RunningReplica:
        """Chooses a replica to send the provided request to.

        By default, requests are scheduled in FIFO order, so this places a future on the
        back of an internal queue that will be popped when a replica is available.

        If `emplace_front` is passed, the request will be placed at the front of the
        queue.

        Upon cancellation (by the caller), the future is cancelled and will be passed
        over when a replica becomes available.
        """
        try:
            if not is_retry:
                self._pending_requests_to_fulfill.append(pending_request)
                self._pending_requests_to_schedule.append(pending_request)
            else:
                pending_request.reset_future()
                index = 0
                for pr in self._pending_requests_to_fulfill:
                    if pending_request.created_at < pr.created_at:
                        break

                    index += 1

                self._pending_requests_to_fulfill.insert(index, pending_request)

                index = 0
                for pr in self._pending_requests_to_schedule:
                    if pending_request.created_at < pr.created_at:
                        break

                    index += 1

                self._pending_requests_to_schedule.insert(index, pending_request)
            self.maybe_start_scheduling_tasks()
            replica = await pending_request.future
        except asyncio.CancelledError as e:
            pending_request.future.cancel()

            raise e from None

        return replica
