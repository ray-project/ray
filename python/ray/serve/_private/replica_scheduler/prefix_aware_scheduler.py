from ray._common.utils import get_or_create_event_loop
import asyncio
import json
import logging
import os
import random
import time
from typing import (
    List,
    Optional,
    Set,
)

import requests

from ray.llm._internal.serve.configs.openai_api_models import (
    ChatCompletionRequest,
    CompletionRequest,
)
from ray.llm._internal.serve.replica_scheduler.prefix_aware.prefix_tree import (
    PrefixTreeActor
)
from ray.serve._private.common import ReplicaID
from ray.serve._private.constants import (
    SERVE_LOGGER_NAME,
)
from ray.serve._private.replica_result import ReplicaResult
from ray.serve._private.replica_scheduler import PowerOfTwoChoicesReplicaScheduler
from ray.serve._private.replica_scheduler.common import (
    PendingRequest,
)
from ray.serve._private.replica_scheduler.replica_wrapper import (
    RunningReplica,
)

logger = logging.getLogger(SERVE_LOGGER_NAME)


class PrefixAwareReplicaScheduler(PowerOfTwoChoicesReplicaScheduler):
    """Extends the PowerOfTwoChoicesReplicaScheduler with prefix-matching capabilities.

    This scheduler optimizes replica selection by considering input text prefixes:

    1. Mixes between three strategies to balance prefix cache hit rate and load balancing:
       - When load is balanced (queue length difference < threshold), it selects replicas
         with the highest prefix match rate for the input text
       - When load is balanced but match rate is below 10%, it falls back to the smallest tenants
       - When load is imbalanced, it uses the default Power of Two selection

    2. Maintains a prefix tree to track which replicas have processed similar inputs:
       - Inserts prompt text into the prefix tree after scheduling
       - Uses this history to inform future scheduling decisions

    This approach improves performance by routing related requests to the same replicas,
    increasing cache locality and reducing overhead for language model inference.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._active_tree_actor = PrefixTreeActor.options(
            name="ActivePrefixTreeActor", get_if_exists=True
        ).remote()
        # self._updating_tree_actor = PrefixTreeActor.options(
        #     name="UpdatingPrefixTreeActor", get_if_exists=True
        # ).remote()
        # self._dual_tree_actor = DualPrefixTreeActor.options(
        #     name="DualPrefixTreeActor", get_if_exists=True
        # ).remote()
        self._use_vllm_prompt_processor = False
        if self._use_vllm_prompt_processor:
            from ray import serve

            self._vllm_engine_deployment = serve.get_deployment_handle(
                "vllm_engine_deployment", app_name="default"
            )
        self._imbalanced_threshold = 10

        # === Metrics tracking ===
        self._do_track_metrics = True
        self._track_metrics_task = None
        self._vllm_metrics_path = "/home/ray/default/work/_testing/results/vllm_metrics"
        self._char_count_over_time_path = (
            "/home/ray/default/work/_testing/results/char_count_over_time"
        )
        self._vllm_metrics_over_time = {}
        self._char_count_over_time = {}
        self._benchmark_start_time = 0.0
        self._num_requests_seen = 0
        self._zero_load_count = 0

        # === Eviction policy ===
        self._use_swapped_tree = False
        self._max_char_count = 100_000  # example threshold
        self._eviction_interval_secs = 10
        self._eviction_task = self._event_loop.create_task(
        # self._eviction_task = get_or_create_event_loop().create_task(
            self._eviction_loop()
        )

    # This doesn't seem to copy ray actors correctly
    # async def copy_tree_to_tree(self, copy_from, copy_to):
        # copyable_attrs = ["root", "tenant_to_char_count", "tenant_to_lru_tail"]
        # for attr in copyable_attrs:
        #     value = await copy_from.getattr.remote(attr)
        #     print(f"[[PrefixAwareReplicaScheduler.copy_tree_to_tree]] Setting {attr} to {value}")
        #     await copy_to.setattr.remote(attr, value)

    async def _eviction_loop(self):
        while True:
            print(f"[[PrefixAwareReplicaScheduler._eviction_loop]] Eviction loop started")
            try:
                print(f"[[PrefixAwareReplicaScheduler._eviction_loop]] Sleeping for {self._eviction_interval_secs} seconds")
                await asyncio.sleep(self._eviction_interval_secs)
                # continue
                if self._use_swapped_tree:
                    pass
                    # print(f"[[PrefixAwareReplicaScheduler._eviction_loop (swapped=True)]] Swapped-tree eviction")
                    # # === Swapped-tree eviction ===

                    # # 1. Copy active tree to updating tree
                    # await self._dual_tree_actor.copy_active_to_updating.remote()
                    # print(f"[[PrefixAwareReplicaScheduler._eviction_loop (swapped=True)]] Copied active tree to updating tree")
                    # # 2. Run eviction in updating tree
                    # tenant_to_char_count = (await self._dual_tree_actor.getattr.remote("updating_tree")).getattr("tenant_to_char_count")
                    # print(f"[[PrefixAwareReplicaScheduler._eviction_loop (swapped=True)]] Tenant to char count: {tenant_to_char_count}")
                    # for tenant, char_count in tenant_to_char_count.items():
                    #     print(f"[[PrefixAwareReplicaScheduler._eviction_loop (swapped=True)]] Tenant: {tenant}, Char count: {char_count}")
                    #     if char_count > self._max_char_count:
                    #         excess = char_count - self._max_char_count
                    #         print(f"[[PrefixAwareReplicaScheduler._eviction_loop (swapped=True)]] Evicting tenant: {tenant}, Excess: {excess}")
                    #         chars_removed = (await self._dual_tree_actor.getattr.remote("updating_tree")).evict_tenant_by_lru(tenant, excess)
                    #         print(f"[[PrefixAwareReplicaScheduler._eviction_loop (swapped=True)]] Evicted tenant: {tenant}, Excess: {excess}, Chars removed: {chars_removed}")
                    # # 3. Copy updating tree to active tree
                    # await self._dual_tree_actor.copy_updating_to_active.remote()
                    # print(f"[[PrefixAwareReplicaScheduler._eviction_loop (swapped=True)]] Copied updating tree to active tree")
                else:
                    # === In-place eviction ===
                    print(f"[[PrefixAwareReplicaScheduler._eviction_loop (swapped=False)]] In-place eviction")
                    tenant_char_count = await self._active_tree_actor.getattr.remote("tenant_to_char_count")
                    print(f"[[PrefixAwareReplicaScheduler._eviction_loop (swapped=False)]] Tenant to char count: {tenant_char_count}")
                    if tenant_char_count:
                        for tenant, char_count in tenant_char_count.items():
                            print(f"[[PrefixAwareReplicaScheduler._eviction_loop (swapped=False)]] Tenant: {tenant}, Char count: {char_count}")
                            if char_count > self._max_char_count:
                                excess = char_count - self._max_char_count
                                print(f"[[PrefixAwareReplicaScheduler._eviction_loop (swapped=False)]] Evicting tenant: {tenant}, Excess: {excess}")
                                chars_removed = await self._active_tree_actor.evict_tenant_by_lru.remote(tenant, excess)
                                print(f"[[PrefixAwareReplicaScheduler._eviction_loop (swapped=False)]] Evicted tenant: {tenant}, Excess: {excess}, Chars removed: {chars_removed}")

            except Exception as e:
                logger.exception(f"[Eviction Loop] error: {e}")

    async def _track_metrics(self):
        """Track vLLM metrics by WorkerId every 0.1s, save to JSON at end."""
        try:
            self._benchmark_start_time = time.time()
            print("Beginning to track metrics immediately")
            session = requests.Session()
            while True:
                await asyncio.sleep(1)
                current_time = time.time()
                print(
                    f"[[PrefixAwareReplicaScheduler._track_metrics]] current_time: {current_time}"
                )
                elapsed = round(current_time - self._benchmark_start_time, 2)
                total_load = 0

                # === vLLM metrics via curl ===
                try:
                    response = session.get("http://localhost:5001/metrics")
                    output = response.text
                    print(
                        f"[[PrefixAwareReplicaScheduler._track_metrics]] output: {output}"
                    )
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
                            print(
                                f"[[PrefixAwareReplicaScheduler._track_metrics]] name: {name}, label_str: {label_str}"
                            )
                            if name == "ray_vllm:num_requests_running":
                                print(
                                    f"[[PrefixAwareReplicaScheduler._track_metrics]] ray_vllm:num_requests_running value: {value}"
                                )
                                total_load += value
                            label_str = label_str.rstrip("}")
                            labels = dict(
                                item.split("=") for item in label_str.split(",")
                            )
                            labels = {k: v.strip('"') for k, v in labels.items()}
                        else:
                            name = metric_line
                            labels = {}

                        # Extract WorkerId
                        worker_id = labels.get("WorkerId", "unknown")

                        # Keep only important label keys
                        important_keys = {"le", "model_name"}
                        filtered_labels = {
                            k: v for k, v in labels.items() if k in important_keys
                        }

                        # Append important label suffix to metric name
                        if filtered_labels:
                            label_suffix = ",".join(
                                f"{k}={v}" for k, v in sorted(filtered_labels.items())
                            )
                            metric_key = f"{name}{{{label_suffix}}}"
                        else:
                            metric_key = name

                        if worker_id not in current_vllm_metrics:
                            current_vllm_metrics[worker_id] = {}
                        current_vllm_metrics[worker_id][metric_key] = value

                    self._vllm_metrics_over_time[elapsed] = current_vllm_metrics

                except Exception as e:
                    print(f"[WARN] Failed to curl or parse /metrics: {e}")

                # === Character count over time ===
                tenant_char_count = await self._active_tree_actor.getattr.remote(
                    "tenant_to_char_count"
                )
                from collections import defaultdict

                current_char_count = defaultdict(int)
                if tenant_char_count is not None:
                    for tenant, char_count in tenant_char_count.items():
                        current_char_count[tenant] = char_count
                self._char_count_over_time[elapsed] = current_char_count

                # === End condition ===
                print(
                    f"[[PrefixAwareReplicaScheduler._track_metrics]] self._num_requests_seen: {self._num_requests_seen}, total_load: {total_load}"
                )
                if self._num_requests_seen > 10 and total_load == 0:
                    self._zero_load_count += 1
                    if self._zero_load_count >= 2:
                        print("Benchmark ended, writing data to disk")
                        # Dump vLLM metrics
                        os.makedirs(self._vllm_metrics_path, exist_ok=True)
                        os.makedirs(self._char_count_over_time_path, exist_ok=True)
                        with open(
                            os.path.join(
                                self._vllm_metrics_path,
                                f"prefix_aware_{int(time.time())}_id_{random.randint(0, 1000000)}.json",
                            ),
                            "w",
                        ) as f:
                            json.dump(self._vllm_metrics_over_time, f, indent=2)
                        with open(
                            os.path.join(
                                self._char_count_over_time_path,
                                f"prefix_aware_{int(time.time())}_id_{random.randint(0, 1000000)}.json",
                            ),
                            "w",
                        ) as f:
                            json.dump(self._char_count_over_time, f, indent=2)
                        break
                else:
                    self._zero_load_count = 0
        except Exception as e:
            print(f"Error in metrics tracking: {e}")
        finally:
            self._track_metrics_task = None

    async def _extract_text_from_request(self, pending_request: PendingRequest) -> str:
        request = pending_request.args[0]
        if isinstance(request, CompletionRequest):
            prompt = request.prompt
        elif isinstance(request, ChatCompletionRequest):
            prompt = request.messages
        else:
            raise ValueError(
                "request is not a CompletionRequest or ChatCompletionRequest"
            )

        if self._use_vllm_prompt_processor:
            from ray.llm._internal.serve.configs.server_models import (
                GenerationRequest,
                Prompt,
            )

            wrapped_prompt = Prompt(prompt=prompt)
            vllm_request: GenerationRequest = (
                await self._vllm_engine_deployment.prepare_request.remote(
                    request_id="N/A",
                    prompt=wrapped_prompt,
                    stream=False,
                    disk_lora_model=None,
                )
            )
            prompt_text: str = vllm_request.prompt
            return prompt_text
        else:
            if isinstance(prompt, list):
                concatenated_messages = "".join(
                    msg.get("content", "") for msg in prompt if "content" in msg
                )
                return concatenated_messages
            else:
                return prompt

    async def _prefix_match_best_replicas(
        self,
        pending_request: Optional[PendingRequest],
        candidate_replica_ids: Set[ReplicaID],
    ) -> List[ReplicaID]:
        """
        Returns a set of candidate replicas, of which the one with the smallest replica queue will be chosen.
        0. Default: same as pow 2 scheduler, return 2 replicas at random.
        1. If load is balanced, choose replica(s) with highest prefix match rate. If highest hit rate is below 10% or no match found, use replicas with smallest KV cache usage.
        2. If load is imbalanced, use default.
        """
        # Convert candidate replica IDs to strings for prefix matching.
        candidate_replica_ids_strings = [
            replica_id.to_full_id_str() for replica_id in candidate_replica_ids
        ]
        print(
            f"[[PrefixAwareReplicaScheduler._prefix_match_best_replicas]] candidate_replica_ids_strings: {candidate_replica_ids_strings}"
        )

        chosen_replica_ids_strings = []

        if (
            pending_request is not None
            and pending_request.args is not None
            and len(pending_request.args) > 0
        ):
            input_text = await self._extract_text_from_request(pending_request)
            if input_text is not None:
                # Check for imbalanced load.
                highest_queue_len = 0
                lowest_queue_len = float("inf")
                if self._use_replica_queue_len_cache:
                    # Populate available queue lens from the cache.
                    r: ReplicaID
                    for r in candidate_replica_ids:
                        queue_len = self._replica_queue_len_cache.get(r)
                        if queue_len is None:
                            continue
                        else:
                            highest_queue_len = max(highest_queue_len, queue_len)
                            lowest_queue_len = min(lowest_queue_len, queue_len)
                is_imbalanced = (
                    highest_queue_len - lowest_queue_len > self._imbalanced_threshold
                )
                if not is_imbalanced:
                    (
                        matched_text,
                        matched_tenant_ids,
                    ) = await self._active_tree_actor.prefix_match.remote(
                        input_text, candidate_replica_ids_strings
                    )
                    match_rate = len(matched_text) / len(input_text)
                    if match_rate < 0.1:
                        smallest_tenants = (
                            await self._active_tree_actor.get_smallest_tenants.remote()
                        )
                        if smallest_tenants is not None and len(smallest_tenants) > 0:
                            chosen_replica_ids_strings = smallest_tenants
                    else:
                        if (
                            matched_tenant_ids is not None
                            and len(matched_tenant_ids) > 0
                        ):
                            chosen_replica_ids_strings = matched_tenant_ids
        chosen_replica_ids = [
            ReplicaID.from_full_id_str(replica_id_string)
            for replica_id_string in chosen_replica_ids_strings
        ]
        return chosen_replica_ids

    def on_replica_actor_died(self, replica_id: ReplicaID):
        print(
            f"[[PrefixAwareReplicaScheduler.on_replica_actor_died]] replica_id: {replica_id}"
        )
        """Drop replica from replica set so it's not considered for future requests."""
        super().on_replica_actor_died(replica_id)
        self._active_tree_actor.remove_tenant.remote(
            replica_id.to_full_id_str()
        )
        print(
            f"[[PrefixAwareReplicaScheduler.on_replica_actor_died]] Removed {replica_id} from prefix tree"
        )

    def update_replicas(self, replicas: List[RunningReplica]):
        """Update the set of available replicas to be considered for scheduling.

        When the set of replicas changes, we may spawn additional scheduling tasks
        if there are pending requests.
        """
        print(f"[[PrefixAwareReplicaScheduler.update_replicas]] replicas: {replicas}")
        # 1) remember what was there before…
        old_ids = set(self._replica_id_set)
        print(f"[[PrefixAwareReplicaScheduler.update_replicas]] old_ids: {old_ids}")

        # 2) run the default logic (updates self._replicas, self._replica_id_set, etc)
        super().update_replicas(replicas)

        # 3) figure out who was added / removed
        new_ids = set(self._replica_id_set)
        print(f"[[PrefixAwareReplicaScheduler.update_replicas]] new_ids: {new_ids}")
        added = new_ids - old_ids
        removed = old_ids - new_ids

        # 4) tell the prefix‐tree about the changes
        for rid in added:
            print(
                f"[[PrefixAwareReplicaScheduler.update_replicas]] Adding {rid} to prefix tree"
            )
            # rid is a ReplicaID; we store them in the tree as strings
            self._active_tree_actor._add_tenant.remote(rid.to_full_id_str())

        for rid in removed:
            print(
                f"[[PrefixAwareReplicaScheduler.update_replicas]] Removing {rid} from prefix tree"
            )
            self._active_tree_actor.remove_tenant.remote(rid.to_full_id_str())

        # Start metrics tracking task if it's not already running and we have replicas
        if self._do_track_metrics and self._track_metrics_task is None and len(self._replicas) > 0:
            # self._track_metrics_task = get_or_create_event_loop().create_task(
            self._track_metrics_task = self._event_loop.create_task(
                self._track_metrics()
            )


    async def choose_replicas(
        self,
        replicas_ranks: List[List[RunningReplica]],
        pending_request: Optional[PendingRequest] = None,
    ) -> List[List[RunningReplica]]:
        self._num_requests_seen += 1
        """One iteration of the power of two choices procedure that chooses
         (at most) two random available replicas.

        For multiplexing, this will first attempt to choose replicas that have the
        requested model ID for a configured timeout. If no replicas with the matching
        model ID are available after that timeout, it will fall back to the regular
        procedure.
        """
        print(
            f"[[PrefixAwareReplicaScheduler.choose_replicas]] pending_request: {pending_request}"
        )
        # Get fallback replicas from PowerOfTwoChoicesReplicaScheduler
        fallback_replicas = await super().choose_replicas(
            replicas_ranks=replicas_ranks,
            pending_request=pending_request,
        )

        if pending_request is None or not fallback_replicas:
            return fallback_replicas

        if pending_request.metadata.multiplexed_model_id:
            # Get candidates for multiplexed model ID.
            candidate_replica_ids = self.apply_multiplex_scheduling(
                pending_request=pending_request,
            )
        else:
            # Get candidates for locality preference.
            candidate_replica_ids = self.apply_locality_scheduling(
                pending_request=pending_request,
            )

        if not candidate_replica_ids:
            return fallback_replicas

        chosen_ids = await self._prefix_match_best_replicas(
            pending_request, candidate_replica_ids
        )

        if chosen_ids:
            return [[self._replicas[chosen_id] for chosen_id in chosen_ids]]

        return fallback_replicas

    async def on_request_scheduled(
        self,
        pending_request: PendingRequest,
        replica_id: ReplicaID,
        result: ReplicaResult,
    ):
        """Called when a request is scheduled to a replica.

        This is used as a callback to update the state of the scheduler after
        a response is generated.
        """
        # Right now this only inserts the prompt into the prefix tree, not the response (streaming response makes things complicated)
        if (
            pending_request is not None
            and pending_request.args is not None
            and len(pending_request.args) > 0
        ):
            input_text = await self._extract_text_from_request(pending_request)
            if input_text is not None:
                self._active_tree_actor.insert.remote(
                    input_text, replica_id.to_full_id_str(), time.time()
                )
