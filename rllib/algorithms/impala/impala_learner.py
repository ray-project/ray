from queue import Queue, Empty
import threading
from typing import Dict, List, Optional

import tree  # pip install dm_tree

import ray
from ray.rllib.algorithms.impala.impala import (
    ImpalaConfig,
    LEARNER_RESULTS_CURR_ENTROPY_COEFF_KEY,
)
from ray.rllib.core.columns import Columns
from ray.rllib.core.learner.learner import Learner
from ray.rllib.connectors.learner import AddOneTsToEpisodesAndTruncate
from ray.rllib.policy.sample_batch import MultiAgentBatch, SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.lambda_defaultdict import LambdaDefaultDict
from ray.rllib.utils.metrics import (
    ALL_MODULES,
    NUM_AGENT_STEPS_TRAINED,
    NUM_ENV_STEPS_TRAINED,
)
from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.postprocessing.episodes import (
    add_one_ts_to_episodes_and_truncate,
    remove_last_ts_from_data,
    remove_last_ts_from_episodes_and_restore_truncateds,
)
from ray.rllib.utils.postprocessing.value_predictions import extract_bootstrapped_values
from ray.rllib.utils.postprocessing.zero_padding import unpad_data_if_necessary
from ray.rllib.utils.schedules.scheduler import Scheduler
from ray.rllib.utils.typing import EpisodeType, ModuleID, ResultDict

torch, _ = try_import_torch()

GPU_LOADER_QUEUE_WAIT_TIMER = "gpu_loader_queue_wait_timer"
GPU_LOADER_LOAD_TO_GPU_TIMER = "gpu_loader_load_to_gpu_timer"
LEARNER_THREAD_IN_QUEUE_WAIT_TIMER = "learner_thread_in_queue_wait_timer"
LEARNER_THREAD_UPDATE_TIMER = "learner_thread_update_timer"


class ImpalaLearner(Learner):
    @override(Learner)
    def build(self) -> None:
        super().build()

        # Dict mapping module IDs to the respective entropy Scheduler instance.
        self.entropy_coeff_schedulers_per_module: Dict[
            ModuleID, Scheduler
        ] = LambdaDefaultDict(
            lambda module_id: Scheduler(
                fixed_value_or_schedule=(
                    self.config.get_config_for_module(module_id).entropy_coeff
                ),
                framework=self.framework,
                device=self._device,
            )
        )

        # Extend all episodes by one artificual timestep to allow the value function net
        # to compute the bootstrap values (and add a mask to the batch to know, which
        # slots to mask out).
        if self.config.add_default_connectors_to_learner_pipeline:
            self._learner_connector.prepend(AddOneTsToEpisodesAndTruncate())

        # Create and start the GPU-loader thread. It picks up train-ready batches from
        # the "GPU-loader queue" and loads them to the GPU, then places the GPU batches
        # on the "update queue" for the actual RLModule forward pass and loss
        # computations.
        self._gpu_loader_in_queue = Queue()
        self._learner_thread_in_queue = Queue()
        self._learner_thread_out_queue = Queue()

        # Create and start the GPU loader thread.
        self._gpu_loader_thread = _GPULoaderThread(
            in_queue=self._gpu_loader_in_queue,
            out_queue=self._learner_thread_in_queue,
            device=self._device,
            metrics_logger=self.metrics,
        )
        self._gpu_loader_thread.start()

        # Create and start the Learner thread.
        self._learner_thread = _LearnerThread(
            update_method=self._update_from_batch_or_episodes,
            in_queue=self._learner_thread_in_queue,
            out_queue=self._learner_thread_out_queue,
            metrics_logger=self.metrics,
        )
        self._learner_thread.start()

    def update_from_episodes(
        self,
        episodes: List[EpisodeType],
        *,
        # TODO (sven): Deprecate these in favor of config attributes for only those
        #  algos that actually need (and know how) to do minibatching.
        minibatch_size: Optional[int] = None,
        num_iters: int = 1,
        min_total_mini_batches: int = 0,
        reduce_fn=None,  # Deprecated args.
    ) -> ResultDict:
        # Resolve batch/episodes being ray object refs (instead of
        # actual batch/episodes objects).
        episodes = ray.get(episodes)
        episodes = tree.flatten(episodes)

        # Call the learner connector pipeline.
        batch = self._learner_connector(
            rl_module=self.module,
            data={},
            episodes=episodes,
            shared_data={},
        )
        # Convert to a batch (on the CPU).
        # TODO (sven): Try to not require MultiAgentBatch anymore.
        batch = MultiAgentBatch(
            {
                module_id: SampleBatch(module_data)
                for module_id, module_data in batch.items()
            },
            env_steps=sum(len(e) for e in episodes),
        )

        # Queue the CPU batch to the GPU-loader thread.
        self._gpu_loader_in_queue.put(batch)

        # Return all queued result dicts thus far (after reducing over them).
        results = {}
        try:
            while True:
                results = self._learner_thread_out_queue.get(block=False)
        except Empty:
            return results

    #@override(Learner)
    #def _update_from_batch_or_episodes(
    #    self,
    #    *,
    #    batch=None,
    #    episodes=None,
    #    reduce_fn=_reduce_mean_results,
    #    minibatch_size=None,
    #    num_iters=1,
    #):
    #    # First perform GAE computation on the entirety of the given train data (all
    #    # episodes).
    #    if self.config.uses_new_env_runners:
    #        # Resolve batch/episodes being ray object refs (instead of actual
    #        # batch/episodes objects).
    #        episodes = ray.get(episodes)
    #        episodes = tree.flatten(episodes)
    #        batch, episodes = self._compute_v_trace_from_episodes(episodes=episodes)

    #    # Now that GAE (advantages and value targets) have been added to the train
    #    # batch, we can proceed normally (calling super method) with the update step.
    #    return super()._update_from_batch_or_episodes(
    #        batch=batch,
    #        episodes=episodes,
    #        reduce_fn=reduce_fn,
    #        minibatch_size=minibatch_size,
    #        num_iters=num_iters,
    #    )

    def _NOT_NEEDED_ANYMORE_compute_v_trace_from_episodes(
        self,
        *,
        episodes,
    ):
        batch = {}
        sa_episodes_list = list(
            self._learner_connector.single_agent_episode_iterator(
                episodes, agents_that_stepped_only=False
            )
        )

        # Make all episodes one ts longer in order to just have a single batch
        # (and distributed forward pass) for both vf predictions AND the bootstrap
        # vf computations.
        orig_truncateds_of_sa_episodes = add_one_ts_to_episodes_and_truncate(
            sa_episodes_list
        )

        # Call the learner connector (on the artificially elongated episodes)
        # in order to get the batch to pass through the module for vf (and
        # bootstrapped vf) computations.
        batch_for_vf = self._learner_connector(
            rl_module=self.module,
            data={},
            episodes=episodes,
            shared_data={},
        )
        # TODO (sven): Try to not require MultiAgentBatch anymore.
        batch_for_vf = MultiAgentBatch(
            {mid: SampleBatch(v) for mid, v in batch_for_vf.items()},
            env_steps=sum(len(e) for e in episodes),
        )
        # Perform the value model's forward pass.
        vf_preds = convert_to_numpy(self._compute_values(batch_for_vf))

        for module_id, module_vf_preds in vf_preds.items():
            # Collect new (single-agent) episode lengths.
            episode_lens_plus_1 = [
                len(e)
                for e in sa_episodes_list
                if e.module_id is None or e.module_id == module_id
            ]
            orig_episode_lens = [e - 1 for e in episode_lens_plus_1]

            # Remove all zero-padding again, if applicable, for the upcoming
            # GAE computations.
            module_vf_preds = unpad_data_if_necessary(
                episode_lens_plus_1, module_vf_preds
            )
            # Generate the bootstrap value column (with only one entry per batch row).
            module_values_bootstrapped = extract_bootstrapped_values(
                vf_preds=module_vf_preds,
                episode_lengths=orig_episode_lens,
                T=self.config.get_rollout_fragment_length(),
            )
            # Remove the extra timesteps again from vf_preds and value targets. Now that
            # the GAE computation is done, we don't need this last timestep anymore in
            # any of our data.
            module_vf_preds = remove_last_ts_from_data(
                episode_lens_plus_1, module_vf_preds
            )

            # Restructure VF_PREDS and VALUES_BOOTSTRAPPED in a way that the Learner
            # connector can properly re-batch these new fields.
            batch_pos = 0
            for eps in sa_episodes_list:
                if eps.module_id is not None and eps.module_id != module_id:
                    continue
                len_ = len(eps) - 1
                self._learner_connector.add_n_batch_items(
                    batch=batch,
                    column=Columns.VF_PREDS,
                    items_to_add=module_vf_preds[batch_pos : batch_pos + len_],
                    num_items=len_,
                    single_agent_episode=eps,
                )
                batch_pos += len_

            batch[module_id] = {
                Columns.VALUES_BOOTSTRAPPED: module_values_bootstrapped,
            }

            # Register agent timesteps (per module) for our metrics.
            self.register_metric(
                module_id=module_id,
                key=NUM_AGENT_STEPS_TRAINED,
                value=sum(orig_episode_lens),
            )

        # Remove the extra (artificial) timesteps again at the end of all episodes.
        remove_last_ts_from_episodes_and_restore_truncateds(
            episodes,
            orig_truncateds_of_sa_episodes,
        )

        # Register env timesteps for our metrics.
        self.register_metrics(
            module_id=ALL_MODULES,
            metrics_dict={
                NUM_AGENT_STEPS_TRAINED: sum(e.agent_steps() for e in episodes),
                NUM_ENV_STEPS_TRAINED: sum(e.env_steps() for e in episodes),
            },
        )

        return batch, episodes

    @override(Learner)
    def remove_module(self, module_id: str):
        super().remove_module(module_id)
        self.entropy_coeff_schedulers_per_module.pop(module_id)

    @override(Learner)
    def additional_update_for_module(
        self, *, module_id: ModuleID, config: ImpalaConfig, timestep: int
    ) -> None:
        super().additional_update_for_module(
            module_id=module_id, config=config, timestep=timestep
        )

        # Update entropy coefficient via our Scheduler.
        new_entropy_coeff = self.entropy_coeff_schedulers_per_module[module_id].update(
            timestep=timestep
        )
        self.metrics.log_value(
            (module_id, LEARNER_RESULTS_CURR_ENTROPY_COEFF_KEY),
            new_entropy_coeff,
            window=1,
        )

    #@OverrideToImplementCustomLogic
    #def _compute_values(
    #    self,
    #    batch_for_vf: MultiAgentBatch,
    #) -> Union[TensorType, Dict[str, Any]]:
    #    """Computes the value function predictions for the RLModule(s) being optimized.

    #    This method must be overridden by multiagent-specific algorithm learners to
    #    specify the specific value computation logic. If the algorithm is single agent
    #    (or independent multi-agent), there should be no need to override this method.

    #    Args:
    #        batch_for_vf: The multi-agent batch to be used for value function
    #            predictions.

    #    Returns:
    #        A dictionary mapping module IDs to individual value function prediction
    #        tensors.
    #    """
    #    return {
    #        module_id: self.module[module_id]
    #        .unwrapped()
    #        ._compute_values(module_batch, self._device)
    #        for module_id, module_batch in batch_for_vf.policy_batches.items()
    #        if self.should_module_be_updated(module_id, module_batch)
    #    }


class _GPULoaderThread(threading.Thread):
    def __init__(
        self,
        *,
        in_queue: Queue,
        out_queue: Queue,
        device: torch.device,
        metrics_logger: MetricsLogger,
    ):
        super().__init__()
        self.daemon = True

        self._in_queue = in_queue
        self._out_queue = out_queue
        self._device = device
        self.metrics = metrics_logger

    def run(self) -> None:
        while True:
            self._step()

    def _step(self) -> None:
        # Get a new batch from the data (inqueue).
        with self.metrics.log_time((ALL_MODULES, GPU_LOADER_QUEUE_WAIT_TIMER)):
            ma_batch = self._in_queue.get()

        # Load the batch onto the GPU device.
        with self.metrics.log_time((ALL_MODULES, GPU_LOADER_LOAD_TO_GPU_TIMER)):
            ma_batch_on_gpu = ma_batch.to_device(self._device)
            self._out_queue.put(ma_batch_on_gpu)


class _LearnerThread(threading.Thread):
    def __init__(self, *, update_method, in_queue, out_queue, metrics_logger):
        super().__init__()
        self.daemon = True
        self.metrics = metrics_logger
        self.stopped = False

        self._update_method = update_method
        self._in_queue = in_queue
        self._out_queue = out_queue

    def run(self) -> None:
        while not self.stopped:
            self.step()

    def step(self):
        # Get a new batch from the GPU-data (inqueue).
        with self.metrics.log_time((ALL_MODULES, LEARNER_THREAD_IN_QUEUE_WAIT_TIMER)):
            ma_batch_on_gpu = self._in_queue.get()

        # Call the update method on the batch.
        with self.metrics.log_time((ALL_MODULES, LEARNER_THREAD_UPDATE_TIMER)):
            # TODO (sven): For multi-agent AND SGD iter > 1, we need to make sure
            #  this thread has the information about the min minibatches necessary
            #  (due to different agents taking different steps in the env, e.g.
            #  MA-CartPole).
            results = self._update_method(batch=ma_batch_on_gpu)
            self._out_queue.put(results)
