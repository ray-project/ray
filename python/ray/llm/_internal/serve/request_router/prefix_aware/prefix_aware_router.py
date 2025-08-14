# These imports are used for metrics tracking, will remove for PR
import logging
import time
from typing import (
    Any,
    List,
    Optional,
)

import ray
from ray.actor import ActorHandle
from ray.llm._internal.serve.request_router.prefix_aware.prefix_tree import (
    PrefixTreeActor,
)
from ray.serve._private.common import ReplicaID
from ray.serve._private.constants import (
    SERVE_LOGGER_NAME,
)
from ray.serve._private.replica_result import ReplicaResult
from ray.serve._private.request_router import (
    PowerOfTwoChoicesRequestRouter,
)
from ray.serve._private.request_router.common import (
    PendingRequest,
)
from ray.serve._private.request_router.replica_wrapper import (
    RunningReplica,
)
from ray.serve._private.request_router.request_router import (
    LocalityMixin,
    MultiplexMixin,
    RequestRouter,
)

logger = logging.getLogger(SERVE_LOGGER_NAME)


class PrefixCacheAffinityRouter(LocalityMixin, MultiplexMixin, RequestRouter):
    """Extends the PowerOfTwoChoicesRequestRouter with prefix-matching capabilities.

    This request router optimizes replica selection by considering input text prefixes:

    1. Mixes between three strategies to balance prefix cache hit rate and load balancing:
       - When load is balanced (queue length difference < threshold), it selects replicas
         with the highest prefix match rate for the input text
       - When load is balanced but match rate is below 10%, it falls back to the smallest tenants
       - When load is imbalanced, it uses the default Power of Two selection

    2. Maintains a prefix tree to track which replicas have processed similar inputs:
       - Inserts prompt text into the prefix tree after routing
       - Uses this history to inform future routing decisions

    This approach improves performance by routing related requests to the same replicas,
    increasing cache locality and reducing overhead for language model inference.
    """

    def initialize_state(
        self,
        imbalanced_threshold: Optional[int] = 10,
        match_rate_threshold: Optional[float] = 0.1,
        do_eviction: Optional[bool] = False,
        eviction_threshold_chars: Optional[int] = 400_000,
        eviction_target_chars: Optional[int] = 360_000,
        eviction_interval_secs: Optional[int] = 10,
        tree_actor: Optional[ActorHandle] = None,
    ):
        """Initialize the prefix-aware routing state and configuration.

        Args:
            imbalanced_threshold: Threshold for queue length difference to consider
                load balanced. When the difference between replica queue lengths is
                less than this value, prefix-aware routing is used.
            match_rate_threshold: Minimum prefix match rate (0.0-1.0) required to
                use prefix-aware routing. If match rate is below this threshold,
                falls back to smallest tenant selection.
            do_eviction: Whether to enable automatic eviction of old prefix tree
                entries to manage memory usage.
            eviction_threshold_chars: Maximum number of characters in the prefix
                tree before eviction is triggered.
            eviction_target_chars: Target number of characters to reduce the
                prefix tree to during eviction.
            eviction_interval_secs: Interval in seconds between eviction checks
                when eviction is enabled.
            tree_actor: The actor to use for the prefix tree in a test environment.
                If None, a detached actor will be created/retrieved.
        """
        # === Prefix-aware routing logic hyperparameters ===
        self._imbalanced_threshold = imbalanced_threshold
        self._match_rate_threshold = match_rate_threshold

        # === Eviction policy ===
        self._do_eviction = do_eviction
        self._eviction_loop_running = False
        self._eviction_threshold_chars = eviction_threshold_chars
        # Default eviction_target_chars to eviction_threshold_chars if not specified
        self._eviction_target_chars = (
            eviction_target_chars
            if eviction_target_chars is not None
            else eviction_threshold_chars
        )
        self._eviction_interval_secs = eviction_interval_secs

        if tree_actor is None:
            # Use a detached actor to avoid issues with actor lifetime since this is shared between routers
            self._tree_actor = PrefixTreeActor.options(
                name="LlmPrefixTreeActor", get_if_exists=True, lifetime="detached"
            ).remote()
        else:
            self._tree_actor = tree_actor

    def _extract_text_from_request(self, pending_request: PendingRequest) -> str:
        """Extracts the text content from a pending request for prefix matching.

        Searches through request arguments for either 'messages' or 'prompt' attributes,
        then normalizes the content to a single string representation that can be used
        for prefix tree operations.

        Args:
            pending_request: The request to extract text from

        Returns:
            A string containing the prompt text or concatenated message contents

        Raises:
            ValueError: If no prompt or messages attribute is found in the request
        """
        prompt = None
        for arg in pending_request.args:
            valid_input_types = ["messages", "prompt"]
            for valid_input_type in valid_input_types:
                if hasattr(arg, valid_input_type):
                    prompt = (
                        arg.prompt if valid_input_type == "prompt" else arg.messages
                    )
                    break
            if prompt is not None:
                break
        if prompt is None:
            raise ValueError(
                "No request with message or prompt attribute found in pending_request.args"
            )

        return self._normalize_prompt_to_string(prompt)

    def _coerce_to_text(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value
        if isinstance(value, list):
            return "".join(self._coerce_to_text(item) for item in value)
        if isinstance(value, dict):
            text_value = value.get("text")
            if isinstance(text_value, str):
                return text_value
            if "content" in value:
                return self._coerce_to_text(value["content"])

        return ""

    def _normalize_prompt_to_string(self, prompt: Any) -> str:
        """Normalize prompt/messages a single string of characters.
        This is not exhaustive (e.g. thinking parts, multimodal are not supported).
        TODO(seiji): find a more maintainable way to normalize the prompt/messages.

        Supported:
        - string → return as-is
        - list of strings → concat
        - list of message dicts with 'content' as string → concat
        - list of message dicts with 'content' as list of dicts → concat the 'text' fields from those parts
        """
        if isinstance(prompt, str):
            return prompt

        if isinstance(prompt, list):
            return "".join(
                self._coerce_to_text(
                    message.get("content") if isinstance(message, dict) else message
                )
                for message in prompt
            )

        return ""

    async def _prefix_match_best_replicas(
        self,
        pending_request: Optional[PendingRequest],
        candidate_replicas: List[RunningReplica],
    ) -> List[RunningReplica]:
        """
        Returns a set of candidate replicas, of which the one with the smallest replica queue will be chosen.
        0. Default: same as pow 2 request router, return 2 replicas at random.
        1. If load is balanced, choose replica(s) with highest prefix match rate. If highest hit rate is below 10% or no match found, use replicas with smallest KV cache usage.
        2. If load is imbalanced, use default.
        """
        chosen_replica_id_strings = []
        if (
            pending_request is not None
            and pending_request.args is not None
            and len(pending_request.args) > 0
        ):
            input_text = self._extract_text_from_request(pending_request)
            if input_text is not None:
                # Start Sphinx tag: __begin_load_balance_component__
                # Check for imbalanced load.
                highest_queue_len = 0
                lowest_queue_len = float("inf")
                not_in_cache: List[ReplicaID] = []
                if self._use_replica_queue_len_cache:
                    # Populate available queue lens from the cache.
                    for r in candidate_replicas:
                        queue_len = self._replica_queue_len_cache.get(r.replica_id)
                        if queue_len is None or queue_len >= r.max_ongoing_requests:
                            not_in_cache.append(r)
                        else:
                            highest_queue_len = max(highest_queue_len, queue_len)
                            lowest_queue_len = min(lowest_queue_len, queue_len)
                else:
                    not_in_cache = candidate_replicas
                if len(not_in_cache) > 0:
                    for r, queue_len in await self._probe_queue_lens(
                        not_in_cache,
                        0,
                    ):
                        if queue_len is None:
                            continue
                        highest_queue_len = max(highest_queue_len, queue_len)
                        lowest_queue_len = min(lowest_queue_len, queue_len)

                is_imbalanced = (
                    highest_queue_len - lowest_queue_len > self._imbalanced_threshold
                )
                # End Sphinx tag: __end_load_balance_component__
                # Start Sphinx tag: __begin_prefix_match_component__
                if not is_imbalanced:
                    # Convert candidate replica IDs to strings for prefix matching.
                    candidate_replica_ids_strings = [
                        r.replica_id.to_full_id_str() for r in candidate_replicas
                    ]
                    (matched_text, matched_tenant_id_strings,) = ray.get(
                        self._tree_actor.prefix_match.remote(
                            input_text, candidate_replica_ids_strings
                        )
                    )
                    match_rate = len(matched_text) / len(input_text)
                    if match_rate < self._match_rate_threshold:
                        smallest_tenants_id_strings = ray.get(
                            self._tree_actor.get_smallest_tenants.remote()
                        )
                        if (
                            smallest_tenants_id_strings is not None
                            and len(smallest_tenants_id_strings) > 0
                        ):
                            chosen_replica_id_strings = smallest_tenants_id_strings
                    else:
                        if (
                            matched_tenant_id_strings is not None
                            and len(matched_tenant_id_strings) > 0
                        ):
                            chosen_replica_id_strings = matched_tenant_id_strings
                # End Sphinx tag: __end_prefix_match_component__
        return [
            [
                self._replicas[ReplicaID.from_full_id_str(chosen_id_string)]
                for chosen_id_string in chosen_replica_id_strings
            ]
        ]

    # Start Sphinx tag: __begin_on_replica_actor_died__
    def on_replica_actor_died(self, replica_id: ReplicaID):
        """Drop replica from replica set so it's not considered for future requests."""
        super().on_replica_actor_died(replica_id)
        ray.get(self._tree_actor.remove_tenants.remote([replica_id.to_full_id_str()]))

    # End Sphinx tag: __end_on_replica_actor_died__

    def update_replicas(self, replicas: List[RunningReplica]):
        """Update the set of available replicas to be considered for routing.

        When the set of replicas changes, we may spawn additional routing tasks
        if there are pending requests.
        """
        # 1) Record the old replica IDs
        old_ids = set(self._replica_id_set)

        # 2) Run the default update_replicas logic
        super().update_replicas(replicas)

        # 3) Figure out which replicas were added / removed
        new_ids = set(self._replica_id_set)
        added = new_ids - old_ids
        removed = old_ids - new_ids

        # 4) Update the prefix tree with the changes
        if added:
            added_strings = [rid.to_full_id_str() for rid in added]
            ray.get(self._tree_actor.add_tenants.remote(added_strings, time.time()))

        if removed:
            removed_strings = [rid.to_full_id_str() for rid in removed]
            ray.get(self._tree_actor.remove_tenants.remote(removed_strings))

        # === Start tasks (if enabled and not already running) ===
        if self._do_eviction and not self._eviction_loop_running:
            ray.get(
                self._tree_actor.start_eviction_loop.remote(
                    self._eviction_threshold_chars,
                    self._eviction_target_chars,
                    self._eviction_interval_secs,
                )
            )
            self._eviction_loop_running = True

    async def choose_replicas(
        self,
        candidate_replicas: List[RunningReplica],
        pending_request: Optional[PendingRequest] = None,
    ) -> List[RunningReplica]:
        """One iteration of the power of two choices procedure that chooses
         (at most) two random available replicas.

        For multiplexing, this will first attempt to choose replicas that have the
        requested model ID for a configured timeout. If no replicas with the matching
        model ID are available after that timeout, it will fall back to the regular
        procedure.
        """
        # Start Sphinx tag: __begin_pow2_router_base__
        # Get fallback replicas from PowerOfTwoChoicesRequestRouter
        fallback_replicas = await PowerOfTwoChoicesRequestRouter.choose_replicas(
            self,
            candidate_replicas=candidate_replicas,
            pending_request=pending_request,
        )
        if pending_request is None or not fallback_replicas:
            return fallback_replicas
        # End Sphinx tag: __end_pow2_router_base__

        if (
            pending_request is not None
            and pending_request.metadata.multiplexed_model_id
        ):
            # Get candidates for multiplexed model ID.
            candidate_replica_ids = self.apply_multiplex_routing(
                pending_request=pending_request,
            )
        else:
            # Get candidates for locality preference.
            candidate_replica_ids = self.apply_locality_routing(
                pending_request=pending_request,
            )
        if not candidate_replica_ids:
            return fallback_replicas

        # Convert candidate replica IDs to RunningReplica objects.
        replica_id_to_replica_map = {
            replica.replica_id: replica for replica in candidate_replicas
        }
        candidate_replicas = [
            replica_id_to_replica_map[candidate_replica_id]
            for candidate_replica_id in candidate_replica_ids
        ]
        chosen_replicas = await self._prefix_match_best_replicas(
            pending_request, candidate_replicas
        )
        if chosen_replicas[0]:
            return chosen_replicas

        return fallback_replicas

    # Start Sphinx tag: __begin_on_request_routed__
    def on_request_routed(
        self,
        pending_request: PendingRequest,
        replica_id: ReplicaID,
        result: ReplicaResult,
    ):
        """Called when a request is routed to a replica.

        This is used as a callback to update the state of the request router
        after a response is generated.
        """
        # Right now this only inserts the prompt into the prefix tree, not the response (streaming response makes things complicated)
        if (
            pending_request is not None
            and pending_request.args is not None
            and len(pending_request.args) > 0
        ):
            input_text = self._extract_text_from_request(pending_request)
            if input_text is not None:
                # Insert into prefix tree
                ray.get(
                    self._tree_actor.insert.remote(
                        input_text, replica_id.to_full_id_str(), time.time()
                    )
                )

    # End Sphinx tag: __end_on_request_routed__
