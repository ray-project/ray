import logging
import time
from typing import (
    List,
    Optional,
    Set,
)

from ray.llm._internal.serve.configs.openai_api_models import (
    ChatCompletionRequest,
    CompletionRequest,
)
from ray.llm._internal.serve.replica_scheduler.prefix_aware.prefix_tree import (
    PrefixTreeActor,
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
        self._tree_actor = PrefixTreeActor.options(
            name="PrefixTreeActor", get_if_exists=True
        ).remote()
        self._use_vllm_prompt_processor = False
        if self._use_vllm_prompt_processor:
            from ray import serve
            self._vllm_engine_deployment = serve.get_deployment_handle(
                "vllm_engine_deployment", app_name="default"
            )
        self.IMBALANCED_THRESHOLD = 10

    def on_replica_actor_died(self, replica_id: ReplicaID):
        print(f"[[PrefixAwareReplicaScheduler.on_replica_actor_died]] replica_id: {replica_id}")
        """Drop replica from replica set so it's not considered for future requests."""
        super().on_replica_actor_died(replica_id)
        chars_removed =self._tree_actor.remove_tenant.remote(replica_id.to_full_id_str())
        print(f"[[PrefixAwareReplicaScheduler.on_replica_actor_died]] Removed {replica_id} from prefix tree, chars_removed: {chars_removed}")

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
        added   = new_ids - old_ids
        removed = old_ids - new_ids

        # 4) tell the prefix‐tree about the changes
        for rid in added:
            print(f"[[PrefixAwareReplicaScheduler.update_replicas]] Adding {rid} to prefix tree")
            # rid is a ReplicaID; we store them in the tree as strings
            self._tree_actor._add_tenant.remote(rid.to_full_id_str())

        for rid in removed:
            print(f"[[PrefixAwareReplicaScheduler.update_replicas]] Removing {rid} from prefix tree")
            self._tree_actor.remove_tenant.remote(rid.to_full_id_str())


    async def _extract_text_from_request(self, pending_request: PendingRequest) -> str:
        request = pending_request.args[0]
        if isinstance(request, CompletionRequest):
            prompt = request.prompt
        elif isinstance(request, ChatCompletionRequest):
            prompt = request.messages
        else:
            raise ValueError("request is not a CompletionRequest or ChatCompletionRequest")

        if self._use_vllm_prompt_processor:
            from ray.llm._internal.serve.configs.server_models import Prompt, GenerationRequest
            wrapped_prompt = Prompt(prompt=prompt)
            vllm_request: GenerationRequest = await self._vllm_engine_deployment.prepare_request.remote(
                request_id="N/A", prompt=wrapped_prompt, stream=False, disk_lora_model=None
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

    async def choose_replicas(
        self,
        replicas_ranks: List[List[RunningReplica]],
        pending_request: Optional[PendingRequest] = None,
    ) -> List[List[RunningReplica]]:
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

        chosen_ids = await self.prefix_match_best_replicas(
            pending_request, candidate_replica_ids
        )

        if chosen_ids:
            return [[self._replicas[chosen_id] for chosen_id in chosen_ids]]

        return fallback_replicas

    async def prefix_match_best_replicas(
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
        print(f"[[PrefixAwareReplicaScheduler.prefix_match_best_replicas]] candidate_replica_ids_strings: {candidate_replica_ids_strings}")

        # # Ensure each candidate replica is an active tenant in the prefix tree.
        # for replica_id_string in candidate_replica_ids_strings:
        #     await self._tree_actor._add_tenant.remote(replica_id_string)

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
                    highest_queue_len - lowest_queue_len > self.IMBALANCED_THRESHOLD
                )
                if not is_imbalanced:
                    (
                        matched_text,
                        matched_tenant_ids,
                    ) = await self._tree_actor.prefix_match.remote(
                        input_text, candidate_replica_ids_strings
                    )
                    match_rate = len(matched_text) / len(input_text)
                    if match_rate < 0.1:
                        smallest_tenants = (
                            await self._tree_actor.get_smallest_tenants.remote()
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
                await self._tree_actor.insert.remote(
                    input_text, replica_id.to_full_id_str(), time.time()
                )
