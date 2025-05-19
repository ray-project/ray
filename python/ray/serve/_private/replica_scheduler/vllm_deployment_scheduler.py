import asyncio
import logging
import random
import time
from typing import (
    AsyncGenerator,
    List,
    Optional,
)

from ray import serve
from ray.serve._private.common import (
    RequestMetadata,
    RunningReplicaInfo,
)
from ray.serve._private.constants import (
    RAY_SERVE_MULTIPLEXED_MODEL_ID_MATCHING_TIMEOUT_S,
    SERVE_LOGGER_NAME,
)
from ray.serve._private.replica_scheduler.common import PendingRequest
from ray.serve._private.replica_scheduler.replica_scheduler import (
    LocalityScope,
    ReplicaScheduler,
)

logger = logging.getLogger(SERVE_LOGGER_NAME)


class VLLMDeploymentScheduler(ReplicaScheduler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._use_vllm_prompt_processor = True
        if self._use_vllm_prompt_processor:

            self._vllm_engine_deployment = serve.get_deployment_handle(
                "vllm_engine_deployment", app_name="default"
            )

    # This is an example function that uses the vllm engine deployment to apply the chat template.
    async def _extract_text_from_request(self, pending_request: PendingRequest) -> str:
        from ray.llm._internal.serve.configs.openai_api_models import (
            ChatCompletionRequest,
            CompletionRequest,
        )

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

    async def choose_replicas_with_backoff(
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
            tried_first_multiplexed_models = False

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
                        ) or tried_first_multiplexed_models:
                            # When there is no match for a multiplexed model id
                            # or when the replica(s) with the matching model id is busy,
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
                        tried_first_multiplexed_models = True
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
                        k=min(2, len(candidate_replica_ids)),
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
