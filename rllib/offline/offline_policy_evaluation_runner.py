import gymnasium as gym
import math
import numpy
import ray

from enum import Enum
from typing import (
    Collection,
    Dict,
    Iterable,
    List,
    Optional,
    TYPE_CHECKING,
    Union,
)

from ray.data.iterator import DataIterator
from ray.rllib.connectors.env_to_module import EnvToModulePipeline
from ray.rllib.core import (
    ALL_MODULES,
    DEFAULT_AGENT_ID,
    DEFAULT_MODULE_ID,
    COMPONENT_ENV_TO_MODULE_CONNECTOR,
    COMPONENT_RL_MODULE,
)
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.offline.offline_prelearner import OfflinePreLearner, SCHEMA
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.checkpoints import Checkpointable
from ray.rllib.utils.framework import get_device, try_import_torch
from ray.rllib.utils.metrics import (
    DATASET_NUM_ITERS_EVALUATED,
    DATASET_NUM_ITERS_EVALUATED_LIFETIME,
    EPISODE_LEN_MAX,
    EPISODE_LEN_MEAN,
    EPISODE_LEN_MIN,
    EPISODE_RETURN_MAX,
    EPISODE_RETURN_MEAN,
    EPISODE_RETURN_MIN,
    MODULE_SAMPLE_BATCH_SIZE_MEAN,
    NUM_ENV_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
    NUM_MODULE_STEPS_SAMPLED,
    NUM_MODULE_STEPS_SAMPLED_LIFETIME,
    OFFLINE_SAMPLING_TIMER,
    WEIGHTS_SEQ_NO,
)
from ray.rllib.utils.minibatch_utils import MiniBatchRayDataIterator
from ray.rllib.utils.runners.runner import Runner
from ray.rllib.utils.torch_utils import convert_to_torch_tensor
from ray.rllib.utils.typing import (
    DeviceType,
    EpisodeID,
    StateDict,
    TensorType,
)

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

torch, _ = try_import_torch()

TOTAL_EVAL_LOSS_KEY = "total_eval_loss"


# TODO (simon): Implement more ...
class OfflinePolicyEvaluationTypes(str, Enum):
    """Defines the offline policy evaluation types.

    IS: Importance Sampling.
    PDIS: Per-Decision Importance Sampling. In contrast to IS this method
        weighs each reward and not the return as a whole. As a result it
        usually exhibits lower variance.
    """

    IS = "is"
    PDIS = "pdis"


class OfflinePolicyPreEvaluator(OfflinePreLearner):
    def __call__(self, batch: Dict[str, numpy.ndarray]) -> Dict[str, numpy.ndarray]:
        # If we directly read in episodes we just convert to list.
        if self.input_read_episodes:
            # Import `msgpack` for decoding.
            import msgpack
            import msgpack_numpy as mnp

            # Read the episodes and decode them.
            episodes: List[SingleAgentEpisode] = [
                SingleAgentEpisode.from_state(
                    msgpack.unpackb(state, object_hook=mnp.decode)
                )
                for state in batch["item"]
            ]
            # Ensure that all episodes are done and no duplicates are in the batch.
            episodes = self._validate_episodes(episodes)
            # Add the episodes to the buffer.
            self.episode_buffer.add(episodes)
            # TODO (simon): Refactor into a single code block for both cases.
            episodes = self.episode_buffer.sample(
                num_items=self.config.train_batch_size_per_learner,
                batch_length_T=self.config.model_config.get("max_seq_len", 0)
                if self._module.is_stateful()
                else None,
                n_step=self.config.get("n_step", 1) or 1,
                # TODO (simon): This can be removed as soon as DreamerV3 has been
                # cleaned up, i.e. can use episode samples for training.
                sample_episodes=True,
                to_numpy=True,
            )
        # Else, if we have old stack `SampleBatch`es.
        elif self.input_read_sample_batches:
            episodes: List[
                SingleAgentEpisode
            ] = OfflinePreLearner._map_sample_batch_to_episode(
                self._is_multi_agent,
                batch,
                to_numpy=True,
                schema=SCHEMA | self.config.input_read_schema,
                input_compress_columns=self.config.input_compress_columns,
            )[
                "episodes"
            ]
            # Ensure that all episodes are done and no duplicates are in the batch.
            episodes = self._validate_episodes(episodes)
            # Add the episodes to the buffer.
            self.episode_buffer.add(episodes)
            # Sample steps from the buffer.
            episodes = self.episode_buffer.sample(
                num_items=self.config.train_batch_size_per_learner,
                batch_length_T=self.config.model_config.get("max_seq_len", 0)
                if self._module.is_stateful()
                else None,
                n_step=self.config.get("n_step", 1) or 1,
                # TODO (simon): This can be removed as soon as DreamerV3 has been
                # cleaned up, i.e. can use episode samples for training.
                sample_episodes=True,
                to_numpy=True,
            )
        # Otherwise we map the batch to episodes.
        else:
            episodes: List[SingleAgentEpisode] = self._map_to_episodes(
                self._is_multi_agent,
                batch,
                schema=SCHEMA | self.config.input_read_schema,
                to_numpy=False,
                input_compress_columns=self.config.input_compress_columns,
                observation_space=self.observation_space,
                action_space=self.action_space,
            )["episodes"]

        episode_dicts = []
        for episode in episodes:
            # Note, we expect users to provide terminated episodes in `SingleAgentEpisode`
            # or `SampleBatch` format. Otherwise computation of episode returns will be
            # biased.
            episode_dict = {}
            episode_dict[Columns.OBS] = episode.get_observations(slice(0, len(episode)))
            episode_dict[Columns.ACTIONS] = episode.get_actions()
            episode_dict[Columns.REWARDS] = episode.get_rewards()
            episode_dict[Columns.ACTION_LOGP] = episode.get_extra_model_outputs(
                key=Columns.ACTION_LOGP
            )
            episode_dicts.append(episode_dict)

        return {"episodes": episode_dicts}


class OfflinePolicyEvaluationRunner(Runner, Checkpointable):
    def __init__(
        self,
        config: "AlgorithmConfig",
        module_spec: Optional[MultiRLModuleSpec] = None,
        **kwargs,
    ):

        # This needs to be defined before we call the `Runner.__init__`
        # b/c the latter calls the `make_module` and then needs the spec.
        # TODO (simon): Check, if we make this a generic attribute.
        self.__module_spec: MultiRLModuleSpec = module_spec
        self.__dataset_iterator = None
        self.__batch_iterator = None

        Runner.__init__(self, config=config, **kwargs)
        Checkpointable.__init__(self)

        # This has to be defined after we have a `self.config`.
        self.__spaces = kwargs.get("spaces")
        self.__env_to_module = self.config.build_env_to_module_connector(
            spaces=self._spaces, device=self._device
        )
        self.__offline_evaluation_type = OfflinePolicyEvaluationTypes(
            self.config["offline_evaluation_type"]
        )

    def run(
        self,
        explore: bool = False,
        train: bool = True,
        **kwargs,
    ) -> None:

        if self.__dataset_iterator is None:
            raise ValueError(
                f"{self} doesn't have a data iterator. Can't call `run` on "
                "`OfflinePolicyEvaluationRunner`."
            )

        if not self._batch_iterator:
            self.__batch_iterator = self._create_batch_iterator(
                **self.config.iter_batches_kwargs
            )

        # Log current weight seq no.
        self.metrics.log_value(
            key=WEIGHTS_SEQ_NO,
            value=self._weights_seq_no,
            window=1,
        )

        with self.metrics.log_time(OFFLINE_SAMPLING_TIMER):
            if explore is None:
                explore = self.config.explore

            # Evaluate on offline data.
            return self._evaluate(
                explore=explore,
                train=train,
            )

    def _create_batch_iterator(self, **kwargs) -> Iterable:

        # Import the torch utils here b/c Ray Air imports `torch`` directly.
        from ray.air._internal.torch_utils import (
            convert_ndarray_batch_to_torch_tensor_batch,
        )

        # Define the collate function that converts the flattened dictionary
        # to a `MultiAgentBatch` with Tensors.
        def _collate_fn(
            _batch: Dict[str, numpy.ndarray]
        ) -> Dict[EpisodeID, Dict[str, numpy.ndarray]]:

            return _batch["episodes"]

        # Define the finalize function that makes the host-to-device transfer.
        def _finalize_fn(
            _batch: Dict[EpisodeID, Dict[str, numpy.ndarray]]
        ) -> Dict[EpisodeID, Dict[str, TensorType]]:

            return [
                convert_ndarray_batch_to_torch_tensor_batch(
                    episode, device=self._device, dtypes=torch.float32
                )
                for episode in _batch
            ]

        # Return a minibatch iterator.
        return MiniBatchRayDataIterator(
            iterator=self._dataset_iterator,
            collate_fn=_collate_fn,
            finalize_fn=_finalize_fn,
            minibatch_size=self.config.offline_eval_batch_size_per_runner,
            num_iters=self.config.dataset_num_iters_per_eval_runner,
            **kwargs,
        )

    def _evaluate(
        self,
        explore: bool,
        train: bool,
    ) -> None:

        self.metrics.activate_tensor_mode()

        num_env_steps = 0
        for iteration, tensor_minibatch in enumerate(self._batch_iterator):
            for episode in tensor_minibatch:
                action_dist_cls = self.module[
                    DEFAULT_MODULE_ID
                ].get_inference_action_dist_cls()
                # TODO (simon): It needs here the `EnvToModule` pipeline.
                action_logits = self.module[DEFAULT_MODULE_ID].forward_inference(
                    episode
                )[Columns.ACTION_DIST_INPUTS]
                # TODO (simon): It might need here the ModuleToEnv pipeline until the
                # `GetActions` piece.
                action_dist = action_dist_cls.from_logits(action_logits)
                actions = action_dist.sample()
                action_logp = action_dist.logp(actions)
                # If we have action log-probs use them.
                if Columns.ACTION_LOGP in episode:
                    behavior_action_logp = episode[Columns.ACTION_LOGP]
                # Otherwise approximate them via the current action distribution.
                else:
                    behavior_action_logp = action_dist.logp(episode[Columns.ACTIONS])

                # Compute the weights.
                if self.__offline_evaluation_type == OfflinePolicyEvaluationTypes.IS:
                    weight = torch.prod(
                        torch.exp(action_logp) / torch.exp(behavior_action_logp)
                    )
                    # Note, we use the (un)-discounted return to compare with the `EnvRunner`
                    # returns.
                    episode_return = episode[Columns.REWARDS].sum()
                    offline_return = (weight * episode_return).item()
                elif (
                    self.__offline_evaluation_type == OfflinePolicyEvaluationTypes.PDIS
                ):
                    weights = torch.exp(action_logp) / torch.exp(behavior_action_logp)
                    offline_return = torch.dot(weights, episode[Columns.REWARDS]).item()

                episode_len = episode[Columns.REWARDS].shape[0] + 1
                num_env_steps += episode_len

                self._log_episode_metrics(episode_len, offline_return)

            self._log_batch_metrics(len(tensor_minibatch), num_env_steps)

        # Record the number of batches pulled from the dataset.
        self.metrics.log_value(
            (ALL_MODULES, DATASET_NUM_ITERS_EVALUATED),
            iteration + 1,
            reduce="sum",
            clear_on_reduce=True,
        )
        self.metrics.log_value(
            (ALL_MODULES, DATASET_NUM_ITERS_EVALUATED_LIFETIME),
            iteration + 1,
            reduce="sum",
        )

        self.metrics.deactivate_tensor_mode()

        return self.metrics.reduce()

    @override(Checkpointable)
    def get_ctor_args_and_kwargs(self):
        return (
            (),  # *args
            {"config": self.config},  # **kwargs
        )

    @override(Checkpointable)
    def get_state(
        self,
        components: Optional[Union[str, Collection[str]]] = None,
        *,
        not_components: Optional[Union[str, Collection[str]]] = None,
        **kwargs,
    ) -> StateDict:
        state = {
            NUM_ENV_STEPS_SAMPLED_LIFETIME: (
                self.metrics.peek(NUM_ENV_STEPS_SAMPLED_LIFETIME, default=0)
            ),
        }

        if self._check_component(COMPONENT_RL_MODULE, components, not_components):
            state[COMPONENT_RL_MODULE] = self.module.get_state(
                components=self._get_subcomponents(COMPONENT_RL_MODULE, components),
                not_components=self._get_subcomponents(
                    COMPONENT_RL_MODULE, not_components
                ),
                **kwargs,
            )
            state[WEIGHTS_SEQ_NO] = self._weights_seq_no
        if self._check_component(
            COMPONENT_ENV_TO_MODULE_CONNECTOR, components, not_components
        ):
            state[COMPONENT_ENV_TO_MODULE_CONNECTOR] = self._env_to_module.get_state()

        return state

    def _convert_to_tensor(self, struct) -> TensorType:
        """Converts structs to a framework-specific tensor."""
        return convert_to_torch_tensor(struct)

    def stop(self) -> None:
        """Releases all resources used by this EnvRunner.

        For example, when using a gym.Env in this EnvRunner, you should make sure
        that its `close()` method is called.
        """
        pass

    def __del__(self) -> None:
        """If this Actor is deleted, clears all resources used by it."""
        pass

    @override(Runner)
    def assert_healthy(self):
        """Checks that self.__init__() has been completed properly.

        Ensures that the instances has a `MultiRLModule` and an
        environment defined.

        Raises:
            AssertionError: If the EnvRunner Actor has NOT been properly initialized.
        """
        # Make sure, we have built our RLModule properly and assigned a dataset iterator.
        assert self._dataset_iterator and hasattr(self, "module")

    @override(Runner)
    def get_metrics(self):
        return self.metrics.reduce()

    def _convert_batch_type(
        self,
        batch: MultiAgentBatch,
        to_device: bool = True,
        pin_memory: bool = False,
        use_stream: bool = False,
    ) -> MultiAgentBatch:
        batch = convert_to_torch_tensor(
            batch.policy_batches,
            device=self._device if to_device else None,
            pin_memory=pin_memory,
            use_stream=use_stream,
        )
        # TODO (sven): This computation of `env_steps` is not accurate!
        length = max(len(b) for b in batch.values())
        batch = MultiAgentBatch(batch, env_steps=length)
        return batch

    @override(Checkpointable)
    def set_state(self, state: StateDict) -> None:
        if COMPONENT_ENV_TO_MODULE_CONNECTOR in state:
            self._env_to_module.set_state(state[COMPONENT_ENV_TO_MODULE_CONNECTOR])

        # Update the RLModule state.
        if COMPONENT_RL_MODULE in state:
            # A missing value for WEIGHTS_SEQ_NO or a value of 0 means: Force the
            # update.
            weights_seq_no = state.get(WEIGHTS_SEQ_NO, 0)

            # Only update the weigths, if this is the first synchronization or
            # if the weights of this `EnvRunner` lacks behind the actual ones.
            if weights_seq_no == 0 or self._weights_seq_no < weights_seq_no:
                rl_module_state = state[COMPONENT_RL_MODULE]
                if isinstance(rl_module_state, ray.ObjectRef):
                    rl_module_state = ray.get(rl_module_state)
                self.module.set_state(rl_module_state)

            # Update our weights_seq_no, if the new one is > 0.
            if weights_seq_no > 0:
                self._weights_seq_no = weights_seq_no

        # Update our lifetime counters.
        # TODO (simon): Create extra metrics.
        if NUM_ENV_STEPS_SAMPLED_LIFETIME in state:
            self.metrics.set_value(
                key=NUM_ENV_STEPS_SAMPLED_LIFETIME,
                value=state[NUM_ENV_STEPS_SAMPLED_LIFETIME],
                reduce="sum",
                with_throughput=True,
            )

    def _log_episode_metrics(self, episode_len: int, episode_return: float) -> None:
        """Logs episode metrics for each episode."""

        # Log general episode metrics.
        # Use the configured window, but factor in the parallelism of the
        # `OfflinePolicyEvaluationRunners`. As a result, we only log the last
        # `window / num_env_runners` steps here, b/c everything gets
        # parallel-merged in the Algorithm process.
        win = max(
            1,
            int(
                math.ceil(
                    self.config.metrics_num_episodes_for_smoothing
                    / (self.config.num_offline_eval_runners or 1)
                )
            ),
        )
        self.metrics.log_value(EPISODE_LEN_MEAN, episode_len, window=win)
        self.metrics.log_value(EPISODE_RETURN_MEAN, episode_return, window=win)
        # Per-agent returns.
        self.metrics.log_value(
            ("agent_episode_return_mean", DEFAULT_AGENT_ID), episode_return, window=win
        )
        # Per-RLModule returns.
        self.metrics.log_value(
            ("module_episode_return_mean", DEFAULT_MODULE_ID),
            episode_return,
            window=win,
        )

        # For some metrics, log min/max as well.
        self.metrics.log_value(EPISODE_LEN_MIN, episode_len, reduce="min", window=win)
        self.metrics.log_value(
            EPISODE_RETURN_MIN, episode_return, reduce="min", window=win
        )
        self.metrics.log_value(EPISODE_LEN_MAX, episode_len, reduce="max", window=win)
        self.metrics.log_value(
            EPISODE_RETURN_MAX, episode_return, reduce="max", window=win
        )

    def _log_batch_metrics(self, batch_size: int, num_env_steps: int):
        """Logs batch metrics for each mini batch."""

        # Log weights seq no for this batch.
        self.metrics.log_value(
            (DEFAULT_MODULE_ID, WEIGHTS_SEQ_NO),
            self._weights_seq_no,
            window=1,
        )

        # Log average batch size (for each module).
        self.metrics.log_value(
            key=(DEFAULT_MODULE_ID, MODULE_SAMPLE_BATCH_SIZE_MEAN),
            value=batch_size,
        )
        # Log module steps (for each module).
        self.metrics.log_value(
            key=(DEFAULT_MODULE_ID, NUM_MODULE_STEPS_SAMPLED),
            value=num_env_steps,
            reduce="sum",
            clear_on_reduce=True,
        )
        self.metrics.log_value(
            key=(DEFAULT_MODULE_ID, NUM_MODULE_STEPS_SAMPLED_LIFETIME),
            value=num_env_steps,
            reduce="sum",
        )
        # Log module steps (sum of all modules).
        self.metrics.log_value(
            key=(ALL_MODULES, NUM_MODULE_STEPS_SAMPLED),
            value=num_env_steps,
            reduce="sum",
            clear_on_reduce=True,
        )
        self.metrics.log_value(
            key=(ALL_MODULES, NUM_MODULE_STEPS_SAMPLED_LIFETIME),
            value=num_env_steps,
            reduce="sum",
        )
        # Log env steps (all modules).
        self.metrics.log_value(
            key=(ALL_MODULES, NUM_ENV_STEPS_SAMPLED),
            value=num_env_steps,
            reduce="sum",
            clear_on_reduce=True,
        )
        self.metrics.log_value(
            key=(ALL_MODULES, NUM_ENV_STEPS_SAMPLED_LIFETIME),
            value=num_env_steps,
            reduce="sum",
            with_throughput=True,
        )

    @override(Runner)
    def set_device(self):
        try:
            self.__device = get_device(
                self.config,
                0
                if not self.worker_index
                else self.config.num_gpus_per_offline_eval_runner,
            )
        except NotImplementedError:
            self.__device = None

    @override(Runner)
    def make_module(self):
        try:
            from ray.rllib.env import INPUT_ENV_SPACES

            if not self._module_spec:
                self.__module_spec = self.config.get_multi_rl_module_spec(
                    # Note, usually we have no environemnt in case of offline evaluation.
                    env=self.config.env,
                    spaces={
                        INPUT_ENV_SPACES: (
                            self.config.observation_space,
                            self.config.action_space,
                        )
                    },
                    inference_only=self.config.offline_eval_rl_module_inference_only,
                )
            # Build the module from its spec.
            self.module = self._module_spec.build()
            # TODO (simon): Implement GPU inference.
            # Move the RLModule to our device.
            # TODO (sven): In order to make this framework-agnostic, we should maybe
            #  make the MultiRLModule.build() method accept a device OR create an
            #  additional `(Multi)RLModule.to()` override.

            self.module.foreach_module(
                lambda mid, mod: (
                    mod.to(self._device) if isinstance(mod, torch.nn.Module) else mod
                )
            )

        # If `AlgorithmConfig.get_multi_rl_module_spec()` is not implemented, this env runner
        # will not have an RLModule, but might still be usable with random actions.
        except NotImplementedError:
            self.module = None

    @property
    def _dataset_iterator(self) -> DataIterator:
        """Returns the dataset iterator."""
        return self.__dataset_iterator

    def set_dataset_iterator(self, iterator):
        """Sets the dataset iterator."""
        self.__dataset_iterator = iterator

    @property
    def _batch_iterator(self) -> MiniBatchRayDataIterator:
        return self.__batch_iterator

    @property
    def _device(self) -> DeviceType:
        return self.__device

    @property
    def _module_spec(self) -> MultiRLModuleSpec:
        """Returns the `MultiRLModuleSpec` of this `Runner`."""
        return self.__module_spec

    @property
    def _spaces(self) -> Dict[str, gym.spaces.Space]:
        """Returns the spaces of thsi `Runner`."""
        return self.__spaces

    @property
    def _env_to_module(self) -> EnvToModulePipeline:
        """Returns the env-to-module pipeline of this `Runner`."""
        return self.__env_to_module

    @property
    def _offline_evaluation_type(self) -> Enum:
        """Returns the offline evaluation type of this `Runner`."""
        return self.__offline_evaluation_type
