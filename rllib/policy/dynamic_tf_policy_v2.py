from collections import OrderedDict
import gymnasium as gym
import logging
import re
import tree  # pip install dm_tree
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple, Type, Union

from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.tf_action_dist import TFActionDistribution
from ray.rllib.policy.dynamic_tf_policy import TFMultiGPUTowerStack
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.tf_policy import TFPolicy
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.utils import force_list
from ray.rllib.utils.annotations import (
    DeveloperAPI,
    OverrideToImplementCustomLogic,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
    is_overridden,
    override,
)
from ray.rllib.utils.debug import summarize
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.metrics import (
    DIFF_NUM_GRAD_UPDATES_VS_SAMPLER_POLICY,
    NUM_GRAD_UPDATES_LIFETIME,
)
from ray.rllib.utils.metrics.learner_info import LEARNER_STATS_KEY
from ray.rllib.utils.spaces.space_utils import get_dummy_batch_for_space
from ray.rllib.utils.tf_utils import get_placeholder
from ray.rllib.utils.typing import (
    AlgorithmConfigDict,
    LocalOptimizer,
    ModelGradients,
    TensorType,
)
from ray.util.debug import log_once

if TYPE_CHECKING:
    from ray.rllib.evaluation import Episode

tf1, tf, tfv = try_import_tf()

logger = logging.getLogger(__name__)


@DeveloperAPI
class DynamicTFPolicyV2(TFPolicy):
    """A TFPolicy that auto-defines placeholders dynamically at runtime.

    This class is intended to be used and extended by sub-classing.
    """

    @DeveloperAPI
    def __init__(
        self,
        obs_space: gym.spaces.Space,
        action_space: gym.spaces.Space,
        config: AlgorithmConfigDict,
        *,
        existing_inputs: Optional[Dict[str, "tf1.placeholder"]] = None,
        existing_model: Optional[ModelV2] = None,
    ):
        self.observation_space = obs_space
        self.action_space = action_space
        self.config = config
        self.framework = "tf"
        self._seq_lens = None
        self._is_tower = existing_inputs is not None

        self.validate_spaces(obs_space, action_space, config)

        self.dist_class = self._init_dist_class()
        # Setup self.model.
        if existing_model and isinstance(existing_model, list):
            self.model = existing_model[0]
            # TODO: (sven) hack, but works for `target_[q_]?model`.
            for i in range(1, len(existing_model)):
                setattr(self, existing_model[i][0], existing_model[i][1])
        else:
            self.model = self.make_model()
        # Auto-update model's inference view requirements, if recurrent.
        self._update_model_view_requirements_from_init_state()

        self._init_state_inputs(existing_inputs)
        self._init_view_requirements()
        timestep, explore = self._init_input_dict_and_dummy_batch(existing_inputs)
        (
            sampled_action,
            sampled_action_logp,
            dist_inputs,
            self._policy_extra_action_fetches,
        ) = self._init_action_fetches(timestep, explore)

        # Phase 1 init.
        sess = tf1.get_default_session() or tf1.Session(
            config=tf1.ConfigProto(**self.config["tf_session_args"])
        )

        batch_divisibility_req = self.get_batch_divisibility_req()

        prev_action_input = (
            self._input_dict[SampleBatch.PREV_ACTIONS]
            if SampleBatch.PREV_ACTIONS in self._input_dict.accessed_keys
            else None
        )
        prev_reward_input = (
            self._input_dict[SampleBatch.PREV_REWARDS]
            if SampleBatch.PREV_REWARDS in self._input_dict.accessed_keys
            else None
        )

        super().__init__(
            observation_space=obs_space,
            action_space=action_space,
            config=config,
            sess=sess,
            obs_input=self._input_dict[SampleBatch.OBS],
            action_input=self._input_dict[SampleBatch.ACTIONS],
            sampled_action=sampled_action,
            sampled_action_logp=sampled_action_logp,
            dist_inputs=dist_inputs,
            dist_class=self.dist_class,
            loss=None,  # dynamically initialized on run
            loss_inputs=[],
            model=self.model,
            state_inputs=self._state_inputs,
            state_outputs=self._state_out,
            prev_action_input=prev_action_input,
            prev_reward_input=prev_reward_input,
            seq_lens=self._seq_lens,
            max_seq_len=config["model"].get("max_seq_len", 20),
            batch_divisibility_req=batch_divisibility_req,
            explore=explore,
            timestep=timestep,
        )

    @DeveloperAPI
    @staticmethod
    def enable_eager_execution_if_necessary():
        # This is static graph TF policy.
        # Simply do nothing.
        pass

    @DeveloperAPI
    @OverrideToImplementCustomLogic
    def validate_spaces(
        self,
        obs_space: gym.spaces.Space,
        action_space: gym.spaces.Space,
        config: AlgorithmConfigDict,
    ):
        return {}

    @DeveloperAPI
    @OverrideToImplementCustomLogic
    @override(Policy)
    def loss(
        self,
        model: Union[ModelV2, "tf.keras.Model"],
        dist_class: Type[TFActionDistribution],
        train_batch: SampleBatch,
    ) -> Union[TensorType, List[TensorType]]:
        """Constructs loss computation graph for this TF1 policy.

        Args:
            model: The Model to calculate the loss for.
            dist_class: The action distr. class.
            train_batch: The training data.

        Returns:
            A single loss tensor or a list of loss tensors.
        """
        raise NotImplementedError

    @DeveloperAPI
    @OverrideToImplementCustomLogic
    def stats_fn(self, train_batch: SampleBatch) -> Dict[str, TensorType]:
        """Stats function. Returns a dict of statistics.

        Args:
            train_batch: The SampleBatch (already) used for training.

        Returns:
            The stats dict.
        """
        return {}

    @DeveloperAPI
    @OverrideToImplementCustomLogic
    def grad_stats_fn(
        self, train_batch: SampleBatch, grads: ModelGradients
    ) -> Dict[str, TensorType]:
        """Gradient stats function. Returns a dict of statistics.

        Args:
            train_batch: The SampleBatch (already) used for training.

        Returns:
            The stats dict.
        """
        return {}

    @DeveloperAPI
    @OverrideToImplementCustomLogic
    def make_model(self) -> ModelV2:
        """Build underlying model for this Policy.

        Returns:
            The Model for the Policy to use.
        """
        # Default ModelV2 model.
        _, logit_dim = ModelCatalog.get_action_dist(
            self.action_space, self.config["model"]
        )
        return ModelCatalog.get_model_v2(
            obs_space=self.observation_space,
            action_space=self.action_space,
            num_outputs=logit_dim,
            model_config=self.config["model"],
            framework="tf",
        )

    @DeveloperAPI
    @OverrideToImplementCustomLogic
    def compute_gradients_fn(
        self, optimizer: LocalOptimizer, loss: TensorType
    ) -> ModelGradients:
        """Gradients computing function (from loss tensor, using local optimizer).

        Args:
            policy: The Policy object that generated the loss tensor and
                that holds the given local optimizer.
            optimizer: The tf (local) optimizer object to
                calculate the gradients with.
            loss: The loss tensor for which gradients should be
                calculated.

        Returns:
            ModelGradients: List of the possibly clipped gradients- and variable
                tuples.
        """
        return None

    @DeveloperAPI
    @OverrideToImplementCustomLogic
    def apply_gradients_fn(
        self,
        optimizer: "tf.keras.optimizers.Optimizer",
        grads: ModelGradients,
    ) -> "tf.Operation":
        """Gradients computing function (from loss tensor, using local optimizer).

        Args:
            optimizer: The tf (local) optimizer object to
                calculate the gradients with.
            grads: The gradient tensor to be applied.

        Returns:
            "tf.Operation": TF operation that applies supplied gradients.
        """
        return None

    @DeveloperAPI
    @OverrideToImplementCustomLogic
    def action_sampler_fn(
        self,
        model: ModelV2,
        *,
        obs_batch: TensorType,
        state_batches: TensorType,
        **kwargs,
    ) -> Tuple[TensorType, TensorType, TensorType, List[TensorType]]:
        """Custom function for sampling new actions given policy.

        Args:
            model: Underlying model.
            obs_batch: Observation tensor batch.
            state_batches: Action sampling state batch.

        Returns:
            Sampled action
            Log-likelihood
            Action distribution inputs
            Updated state
        """
        return None, None, None, None

    @DeveloperAPI
    @OverrideToImplementCustomLogic
    def action_distribution_fn(
        self,
        model: ModelV2,
        *,
        obs_batch: TensorType,
        state_batches: TensorType,
        **kwargs,
    ) -> Tuple[TensorType, type, List[TensorType]]:
        """Action distribution function for this Policy.

        Args:
            model: Underlying model.
            obs_batch: Observation tensor batch.
            state_batches: Action sampling state batch.

        Returns:
            Distribution input.
            ActionDistribution class.
            State outs.
        """
        return None, None, None

    @DeveloperAPI
    @OverrideToImplementCustomLogic
    def get_batch_divisibility_req(self) -> int:
        """Get batch divisibility request.

        Returns:
            Size N. A sample batch must be of size K*N.
        """
        # By default, any sized batch is ok, so simply return 1.
        return 1

    @override(TFPolicy)
    @DeveloperAPI
    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def extra_action_out_fn(self) -> Dict[str, TensorType]:
        """Extra values to fetch and return from compute_actions().

        Returns:
             Dict[str, TensorType]: An extra fetch-dict to be passed to and
                returned from the compute_actions() call.
        """
        extra_action_fetches = super().extra_action_out_fn()
        extra_action_fetches.update(self._policy_extra_action_fetches)
        return extra_action_fetches

    @DeveloperAPI
    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def extra_learn_fetches_fn(self) -> Dict[str, TensorType]:
        """Extra stats to be reported after gradient computation.

        Returns:
             Dict[str, TensorType]: An extra fetch-dict.
        """
        return {}

    @override(TFPolicy)
    def extra_compute_grad_fetches(self):
        return dict({LEARNER_STATS_KEY: {}}, **self.extra_learn_fetches_fn())

    @override(Policy)
    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def postprocess_trajectory(
        self,
        sample_batch: SampleBatch,
        other_agent_batches: Optional[SampleBatch] = None,
        episode: Optional["Episode"] = None,
    ):
        """Post process trajectory in the format of a SampleBatch.

        Args:
            sample_batch: sample_batch: batch of experiences for the policy,
                which will contain at most one episode trajectory.
            other_agent_batches: In a multi-agent env, this contains a
                mapping of agent ids to (policy, agent_batch) tuples
                containing the policy and experiences of the other agents.
            episode: An optional multi-agent episode object to provide
                access to all of the internal episode state, which may
                be useful for model-based or multi-agent algorithms.

        Returns:
            The postprocessed sample batch.
        """
        return Policy.postprocess_trajectory(self, sample_batch)

    @override(TFPolicy)
    @OverrideToImplementCustomLogic
    def optimizer(
        self,
    ) -> Union["tf.keras.optimizers.Optimizer", List["tf.keras.optimizers.Optimizer"]]:
        """TF optimizer to use for policy optimization.

        Returns:
            A local optimizer or a list of local optimizers to use for this
                Policy's Model.
        """
        return super().optimizer()

    def _init_dist_class(self):
        if is_overridden(self.action_sampler_fn) or is_overridden(
            self.action_distribution_fn
        ):
            if not is_overridden(self.make_model):
                raise ValueError(
                    "`make_model` is required if `action_sampler_fn` OR "
                    "`action_distribution_fn` is given"
                )
            return None
        else:
            dist_class, _ = ModelCatalog.get_action_dist(
                self.action_space, self.config["model"]
            )
            return dist_class

    def _init_view_requirements(self):
        # If ViewRequirements are explicitly specified.
        if getattr(self, "view_requirements", None):
            return

        # Use default settings.
        # Add NEXT_OBS, STATE_IN_0.., and others.
        self.view_requirements = self._get_default_view_requirements()
        # Combine view_requirements for Model and Policy.
        # TODO(jungong) : models will not carry view_requirements once they
        # are migrated to be organic Keras models.
        self.view_requirements.update(self.model.view_requirements)
        # Disable env-info placeholder.
        if SampleBatch.INFOS in self.view_requirements:
            self.view_requirements[SampleBatch.INFOS].used_for_training = False

    def _init_state_inputs(self, existing_inputs: Dict[str, "tf1.placeholder"]):
        """Initialize input placeholders.

        Args:
            existing_inputs: existing placeholders.
        """
        if existing_inputs:
            self._state_inputs = [
                v for k, v in existing_inputs.items() if k.startswith("state_in_")
            ]
            # Placeholder for RNN time-chunk valid lengths.
            if self._state_inputs:
                self._seq_lens = existing_inputs[SampleBatch.SEQ_LENS]
            # Create new input placeholders.
        else:
            self._state_inputs = [
                get_placeholder(
                    space=vr.space,
                    time_axis=not isinstance(vr.shift, int),
                    name=k,
                )
                for k, vr in self.model.view_requirements.items()
                if k.startswith("state_in_")
            ]
            # Placeholder for RNN time-chunk valid lengths.
            if self._state_inputs:
                self._seq_lens = tf1.placeholder(
                    dtype=tf.int32, shape=[None], name="seq_lens"
                )

    def _init_input_dict_and_dummy_batch(
        self, existing_inputs: Dict[str, "tf1.placeholder"]
    ) -> Tuple[Union[int, TensorType], Union[bool, TensorType]]:
        """Initialized input_dict and dummy_batch data.

        Args:
            existing_inputs: When copying a policy, this specifies an existing
                dict of placeholders to use instead of defining new ones.

        Returns:
            timestep: training timestep.
            explore: whether this policy should explore.
        """
        # Setup standard placeholders.
        if self._is_tower:
            assert existing_inputs is not None
            timestep = existing_inputs["timestep"]
            explore = False
            (
                self._input_dict,
                self._dummy_batch,
            ) = self._create_input_dict_and_dummy_batch(
                self.view_requirements, existing_inputs
            )
        else:
            # Placeholder for (sampling steps) timestep (int).
            timestep = tf1.placeholder_with_default(
                tf.zeros((), dtype=tf.int64), (), name="timestep"
            )
            # Placeholder for `is_exploring` flag.
            explore = tf1.placeholder_with_default(True, (), name="is_exploring")
            (
                self._input_dict,
                self._dummy_batch,
            ) = self._create_input_dict_and_dummy_batch(self.view_requirements, {})

        # Placeholder for `is_training` flag.
        self._input_dict.set_training(self._get_is_training_placeholder())

        return timestep, explore

    def _create_input_dict_and_dummy_batch(self, view_requirements, existing_inputs):
        """Creates input_dict and dummy_batch for loss initialization.

        Used for managing the Policy's input placeholders and for loss
        initialization.
        Input_dict: Str -> tf.placeholders, dummy_batch: str -> np.arrays.

        Args:
            view_requirements: The view requirements dict.
            existing_inputs (Dict[str, tf.placeholder]): A dict of already
                existing placeholders.

        Returns:
            Tuple[Dict[str, tf.placeholder], Dict[str, np.ndarray]]: The
                input_dict/dummy_batch tuple.
        """
        input_dict = {}
        for view_col, view_req in view_requirements.items():
            # Point state_in to the already existing self._state_inputs.
            mo = re.match(r"state_in_(\d+)", view_col)
            if mo is not None:
                input_dict[view_col] = self._state_inputs[int(mo.group(1))]
            # State-outs (no placeholders needed).
            elif view_col.startswith("state_out_"):
                continue
            # Skip action dist inputs placeholder (do later).
            elif view_col == SampleBatch.ACTION_DIST_INPUTS:
                continue
            # This is a tower: Input placeholders already exist.
            elif view_col in existing_inputs:
                input_dict[view_col] = existing_inputs[view_col]
            # All others.
            else:
                time_axis = not isinstance(view_req.shift, int)
                if view_req.used_for_training:
                    # Create a +time-axis placeholder if the shift is not an
                    # int (range or list of ints).
                    # Do not flatten actions if action flattening disabled.
                    if self.config.get("_disable_action_flattening") and view_col in [
                        SampleBatch.ACTIONS,
                        SampleBatch.PREV_ACTIONS,
                    ]:
                        flatten = False
                    # Do not flatten observations if no preprocessor API used.
                    elif (
                        view_col in [SampleBatch.OBS, SampleBatch.NEXT_OBS]
                        and self.config["_disable_preprocessor_api"]
                    ):
                        flatten = False
                    # Flatten everything else.
                    else:
                        flatten = True
                    input_dict[view_col] = get_placeholder(
                        space=view_req.space,
                        name=view_col,
                        time_axis=time_axis,
                        flatten=flatten,
                    )
        dummy_batch = self._get_dummy_batch_from_view_requirements(batch_size=32)

        return SampleBatch(input_dict, seq_lens=self._seq_lens), dummy_batch

    def _init_action_fetches(
        self, timestep: Union[int, TensorType], explore: Union[bool, TensorType]
    ) -> Tuple[TensorType, TensorType, TensorType, type, Dict[str, TensorType]]:
        """Create action related fields for base Policy and loss initialization."""
        # Multi-GPU towers do not need any action computing/exploration
        # graphs.
        sampled_action = None
        sampled_action_logp = None
        dist_inputs = None
        extra_action_fetches = {}
        self._state_out = None
        if not self._is_tower:
            # Create the Exploration object to use for this Policy.
            self.exploration = self._create_exploration()

            # Fully customized action generation (e.g., custom policy).
            if is_overridden(self.action_sampler_fn):
                (
                    sampled_action,
                    sampled_action_logp,
                    dist_inputs,
                    self._state_out,
                ) = self.action_sampler_fn(
                    self.model,
                    obs_batch=self._input_dict[SampleBatch.OBS],
                    state_batches=self._state_inputs,
                    seq_lens=self._seq_lens,
                    prev_action_batch=self._input_dict.get(SampleBatch.PREV_ACTIONS),
                    prev_reward_batch=self._input_dict.get(SampleBatch.PREV_REWARDS),
                    explore=explore,
                    is_training=self._input_dict.is_training,
                )
            # Distribution generation is customized, e.g., DQN, DDPG.
            else:
                if is_overridden(self.action_distribution_fn):
                    # Try new action_distribution_fn signature, supporting
                    # state_batches and seq_lens.
                    in_dict = self._input_dict
                    (
                        dist_inputs,
                        self.dist_class,
                        self._state_out,
                    ) = self.action_distribution_fn(
                        self.model,
                        obs_batch=in_dict[SampleBatch.OBS],
                        state_batches=self._state_inputs,
                        seq_lens=self._seq_lens,
                        explore=explore,
                        timestep=timestep,
                        is_training=in_dict.is_training,
                    )
                # Default distribution generation behavior:
                # Pass through model. E.g., PG, PPO.
                else:
                    if isinstance(self.model, tf.keras.Model):
                        dist_inputs, self._state_out, extra_action_fetches = self.model(
                            self._input_dict
                        )
                    else:
                        dist_inputs, self._state_out = self.model(self._input_dict)

                action_dist = self.dist_class(dist_inputs, self.model)

                # Using exploration to get final action (e.g. via sampling).
                (
                    sampled_action,
                    sampled_action_logp,
                ) = self.exploration.get_exploration_action(
                    action_distribution=action_dist, timestep=timestep, explore=explore
                )

        if dist_inputs is not None:
            extra_action_fetches[SampleBatch.ACTION_DIST_INPUTS] = dist_inputs

        if sampled_action_logp is not None:
            extra_action_fetches[SampleBatch.ACTION_LOGP] = sampled_action_logp
            extra_action_fetches[SampleBatch.ACTION_PROB] = tf.exp(
                tf.cast(sampled_action_logp, tf.float32)
            )

        return (
            sampled_action,
            sampled_action_logp,
            dist_inputs,
            extra_action_fetches,
        )

    def _init_optimizers(self):
        # Create the optimizer/exploration optimizer here. Some initialization
        # steps (e.g. exploration postprocessing) may need this.
        optimizers = force_list(self.optimizer())
        if self.exploration:
            optimizers = self.exploration.get_exploration_optimizer(optimizers)

        # No optimizers produced -> Return.
        if not optimizers:
            return

        # The list of local (tf) optimizers (one per loss term).
        self._optimizers = optimizers
        # Backward compatibility.
        self._optimizer = optimizers[0]

    def maybe_initialize_optimizer_and_loss(self):
        # We don't need to initialize loss calculation for MultiGPUTowerStack.
        if self._is_tower:
            self.get_session().run(tf1.global_variables_initializer())
            return

        # Loss initialization and model/postprocessing test calls.
        self._init_optimizers()
        self._initialize_loss_from_dummy_batch(auto_remove_unneeded_view_reqs=True)

        # Create MultiGPUTowerStacks, if we have at least one actual
        # GPU or >1 CPUs (fake GPUs).
        if len(self.devices) > 1 or any("gpu" in d for d in self.devices):
            # Per-GPU graph copies created here must share vars with the
            # policy. Therefore, `reuse` is set to tf1.AUTO_REUSE because
            # Adam nodes are created after all of the device copies are
            # created.
            with tf1.variable_scope("", reuse=tf1.AUTO_REUSE):
                self.multi_gpu_tower_stacks = [
                    TFMultiGPUTowerStack(policy=self)
                    for _ in range(self.config.get("num_multi_gpu_tower_stacks", 1))
                ]

        # Initialize again after loss and tower init.
        self.get_session().run(tf1.global_variables_initializer())

    @override(Policy)
    def _initialize_loss_from_dummy_batch(
        self, auto_remove_unneeded_view_reqs: bool = True
    ) -> None:
        # Test calls depend on variable init, so initialize model first.
        self.get_session().run(tf1.global_variables_initializer())

        # Fields that have not been accessed are not needed for action
        # computations -> Tag them as `used_for_compute_actions=False`.
        for key, view_req in self.view_requirements.items():
            if (
                not key.startswith("state_in_")
                and key not in self._input_dict.accessed_keys
            ):
                view_req.used_for_compute_actions = False
        for key, value in self.extra_action_out_fn().items():
            self._dummy_batch[key] = get_dummy_batch_for_space(
                gym.spaces.Box(
                    -1.0, 1.0, shape=value.shape.as_list()[1:], dtype=value.dtype.name
                ),
                batch_size=len(self._dummy_batch),
            )
            self._input_dict[key] = get_placeholder(value=value, name=key)
            if key not in self.view_requirements:
                logger.info("Adding extra-action-fetch `{}` to view-reqs.".format(key))
                self.view_requirements[key] = ViewRequirement(
                    space=gym.spaces.Box(
                        -1.0,
                        1.0,
                        shape=value.shape.as_list()[1:],
                        dtype=value.dtype.name,
                    ),
                    used_for_compute_actions=False,
                )
        dummy_batch = self._dummy_batch

        logger.info("Testing `postprocess_trajectory` w/ dummy batch.")
        self.exploration.postprocess_trajectory(self, dummy_batch, self.get_session())
        _ = self.postprocess_trajectory(dummy_batch)
        # Add new columns automatically to (loss) input_dict.
        for key in dummy_batch.added_keys:
            if key not in self._input_dict:
                self._input_dict[key] = get_placeholder(
                    value=dummy_batch[key], name=key
                )
            if key not in self.view_requirements:
                self.view_requirements[key] = ViewRequirement(
                    space=gym.spaces.Box(
                        -1.0,
                        1.0,
                        shape=dummy_batch[key].shape[1:],
                        dtype=dummy_batch[key].dtype,
                    ),
                    used_for_compute_actions=False,
                )

        train_batch = SampleBatch(
            dict(self._input_dict, **self._loss_input_dict),
            _is_training=True,
        )

        if self._state_inputs:
            train_batch[SampleBatch.SEQ_LENS] = self._seq_lens
            self._loss_input_dict.update(
                {SampleBatch.SEQ_LENS: train_batch[SampleBatch.SEQ_LENS]}
            )

        self._loss_input_dict.update({k: v for k, v in train_batch.items()})

        if log_once("loss_init"):
            logger.debug(
                "Initializing loss function with dummy input:\n\n{}\n".format(
                    summarize(train_batch)
                )
            )

        losses = self._do_loss_init(train_batch)

        all_accessed_keys = (
            train_batch.accessed_keys
            | dummy_batch.accessed_keys
            | dummy_batch.added_keys
            | set(self.model.view_requirements.keys())
        )

        TFPolicy._initialize_loss(
            self,
            losses,
            [(k, v) for k, v in train_batch.items() if k in all_accessed_keys]
            + (
                [(SampleBatch.SEQ_LENS, train_batch[SampleBatch.SEQ_LENS])]
                if SampleBatch.SEQ_LENS in train_batch
                else []
            ),
        )

        if "is_training" in self._loss_input_dict:
            del self._loss_input_dict["is_training"]

        # Call the grads stats fn.
        # TODO: (sven) rename to simply stats_fn to match eager and torch.
        self._stats_fetches.update(self.grad_stats_fn(train_batch, self._grads))

        # Add new columns automatically to view-reqs.
        if auto_remove_unneeded_view_reqs:
            # Add those needed for postprocessing and training.
            all_accessed_keys = train_batch.accessed_keys | dummy_batch.accessed_keys
            # Tag those only needed for post-processing (with some exceptions).
            for key in dummy_batch.accessed_keys:
                if (
                    key not in train_batch.accessed_keys
                    and key not in self.model.view_requirements
                    and key
                    not in [
                        SampleBatch.EPS_ID,
                        SampleBatch.AGENT_INDEX,
                        SampleBatch.UNROLL_ID,
                        SampleBatch.TERMINATEDS,
                        SampleBatch.TRUNCATEDS,
                        SampleBatch.REWARDS,
                        SampleBatch.INFOS,
                        SampleBatch.T,
                        SampleBatch.OBS_EMBEDS,
                    ]
                ):
                    if key in self.view_requirements:
                        self.view_requirements[key].used_for_training = False
                    if key in self._loss_input_dict:
                        del self._loss_input_dict[key]
            # Remove those not needed at all (leave those that are needed
            # by Sampler to properly execute sample collection).
            # Also always leave TERMINATEDS, TRUNCATEDS, REWARDS, and INFOS,
            # no matter what.
            for key in list(self.view_requirements.keys()):
                if (
                    key not in all_accessed_keys
                    and key
                    not in [
                        SampleBatch.EPS_ID,
                        SampleBatch.AGENT_INDEX,
                        SampleBatch.UNROLL_ID,
                        SampleBatch.TERMINATEDS,
                        SampleBatch.TRUNCATEDS,
                        SampleBatch.REWARDS,
                        SampleBatch.INFOS,
                        SampleBatch.T,
                    ]
                    and key not in self.model.view_requirements
                ):
                    # If user deleted this key manually in postprocessing
                    # fn, warn about it and do not remove from
                    # view-requirements.
                    if key in dummy_batch.deleted_keys:
                        logger.warning(
                            "SampleBatch key '{}' was deleted manually in "
                            "postprocessing function! RLlib will "
                            "automatically remove non-used items from the "
                            "data stream. Remove the `del` from your "
                            "postprocessing function.".format(key)
                        )
                    # If we are not writing output to disk, safe to erase
                    # this key to save space in the sample batch.
                    elif self.config["output"] is None:
                        del self.view_requirements[key]

                    if key in self._loss_input_dict:
                        del self._loss_input_dict[key]
            # Add those data_cols (again) that are missing and have
            # dependencies by view_cols.
            for key in list(self.view_requirements.keys()):
                vr = self.view_requirements[key]
                if (
                    vr.data_col is not None
                    and vr.data_col not in self.view_requirements
                ):
                    used_for_training = vr.data_col in train_batch.accessed_keys
                    self.view_requirements[vr.data_col] = ViewRequirement(
                        space=vr.space, used_for_training=used_for_training
                    )

        self._loss_input_dict_no_rnn = {
            k: v
            for k, v in self._loss_input_dict.items()
            if (v not in self._state_inputs and v != self._seq_lens)
        }

    def _do_loss_init(self, train_batch: SampleBatch):
        losses = self.loss(self.model, self.dist_class, train_batch)
        losses = force_list(losses)
        self._stats_fetches.update(self.stats_fn(train_batch))
        # Override the update ops to be those of the model.
        self._update_ops = []
        if not isinstance(self.model, tf.keras.Model):
            self._update_ops = self.model.update_ops()
        return losses

    @override(TFPolicy)
    @DeveloperAPI
    def copy(self, existing_inputs: List[Tuple[str, "tf1.placeholder"]]) -> TFPolicy:
        """Creates a copy of self using existing input placeholders."""

        flat_loss_inputs = tree.flatten(self._loss_input_dict)
        flat_loss_inputs_no_rnn = tree.flatten(self._loss_input_dict_no_rnn)

        # Note that there might be RNN state inputs at the end of the list
        if len(flat_loss_inputs) != len(existing_inputs):
            raise ValueError(
                "Tensor list mismatch",
                self._loss_input_dict,
                self._state_inputs,
                existing_inputs,
            )
        for i, v in enumerate(flat_loss_inputs_no_rnn):
            if v.shape.as_list() != existing_inputs[i].shape.as_list():
                raise ValueError(
                    "Tensor shape mismatch", i, v.shape, existing_inputs[i].shape
                )
        # By convention, the loss inputs are followed by state inputs and then
        # the seq len tensor.
        rnn_inputs = []
        for i in range(len(self._state_inputs)):
            rnn_inputs.append(
                (
                    "state_in_{}".format(i),
                    existing_inputs[len(flat_loss_inputs_no_rnn) + i],
                )
            )
        if rnn_inputs:
            rnn_inputs.append((SampleBatch.SEQ_LENS, existing_inputs[-1]))
        existing_inputs_unflattened = tree.unflatten_as(
            self._loss_input_dict_no_rnn,
            existing_inputs[: len(flat_loss_inputs_no_rnn)],
        )
        input_dict = OrderedDict(
            [("is_exploring", self._is_exploring), ("timestep", self._timestep)]
            + [
                (k, existing_inputs_unflattened[k])
                for i, k in enumerate(self._loss_input_dict_no_rnn.keys())
            ]
            + rnn_inputs
        )

        instance = self.__class__(
            self.observation_space,
            self.action_space,
            self.config,
            existing_inputs=input_dict,
            existing_model=[
                self.model,
                # Deprecated: Target models should all reside under
                # `policy.target_model` now.
                ("target_q_model", getattr(self, "target_q_model", None)),
                ("target_model", getattr(self, "target_model", None)),
            ],
        )

        instance._loss_input_dict = input_dict
        losses = instance._do_loss_init(SampleBatch(input_dict))
        loss_inputs = [
            (k, existing_inputs_unflattened[k])
            for i, k in enumerate(self._loss_input_dict_no_rnn.keys())
        ]

        TFPolicy._initialize_loss(instance, losses, loss_inputs)
        instance._stats_fetches.update(
            instance.grad_stats_fn(input_dict, instance._grads)
        )
        return instance

    @override(Policy)
    @DeveloperAPI
    def get_initial_state(self) -> List[TensorType]:
        if self.model:
            return self.model.get_initial_state()
        else:
            return []

    @override(Policy)
    @DeveloperAPI
    def load_batch_into_buffer(
        self,
        batch: SampleBatch,
        buffer_index: int = 0,
    ) -> int:
        # Set the is_training flag of the batch.
        batch.set_training(True)

        # Shortcut for 1 CPU only: Store batch in
        # `self._loaded_single_cpu_batch`.
        if len(self.devices) == 1 and self.devices[0] == "/cpu:0":
            assert buffer_index == 0
            self._loaded_single_cpu_batch = batch
            return len(batch)

        input_dict = self._get_loss_inputs_dict(batch, shuffle=False)
        data_keys = tree.flatten(self._loss_input_dict_no_rnn)
        if self._state_inputs:
            state_keys = self._state_inputs + [self._seq_lens]
        else:
            state_keys = []
        inputs = [input_dict[k] for k in data_keys]
        state_inputs = [input_dict[k] for k in state_keys]

        return self.multi_gpu_tower_stacks[buffer_index].load_data(
            sess=self.get_session(),
            inputs=inputs,
            state_inputs=state_inputs,
            num_grad_updates=batch.num_grad_updates,
        )

    @override(Policy)
    @DeveloperAPI
    def get_num_samples_loaded_into_buffer(self, buffer_index: int = 0) -> int:
        # Shortcut for 1 CPU only: Batch should already be stored in
        # `self._loaded_single_cpu_batch`.
        if len(self.devices) == 1 and self.devices[0] == "/cpu:0":
            assert buffer_index == 0
            return (
                len(self._loaded_single_cpu_batch)
                if self._loaded_single_cpu_batch is not None
                else 0
            )

        return self.multi_gpu_tower_stacks[buffer_index].num_tuples_loaded

    @override(Policy)
    @DeveloperAPI
    def learn_on_loaded_batch(self, offset: int = 0, buffer_index: int = 0):
        # Shortcut for 1 CPU only: Batch should already be stored in
        # `self._loaded_single_cpu_batch`.
        if len(self.devices) == 1 and self.devices[0] == "/cpu:0":
            assert buffer_index == 0
            if self._loaded_single_cpu_batch is None:
                raise ValueError(
                    "Must call Policy.load_batch_into_buffer() before "
                    "Policy.learn_on_loaded_batch()!"
                )
            # Get the correct slice of the already loaded batch to use,
            # based on offset and batch size.
            batch_size = self.config.get(
                "sgd_minibatch_size", self.config["train_batch_size"]
            )
            if batch_size >= len(self._loaded_single_cpu_batch):
                sliced_batch = self._loaded_single_cpu_batch
            else:
                sliced_batch = self._loaded_single_cpu_batch.slice(
                    start=offset, end=offset + batch_size
                )
            return self.learn_on_batch(sliced_batch)

        tower_stack = self.multi_gpu_tower_stacks[buffer_index]
        results = tower_stack.optimize(self.get_session(), offset)
        self.num_grad_updates += 1

        results.update(
            {
                NUM_GRAD_UPDATES_LIFETIME: self.num_grad_updates,
                # -1, b/c we have to measure this diff before we do the update above.
                DIFF_NUM_GRAD_UPDATES_VS_SAMPLER_POLICY: (
                    self.num_grad_updates - 1 - (tower_stack.num_grad_updates or 0)
                ),
            }
        )

        return results

    @override(TFPolicy)
    def gradients(self, optimizer, loss):
        optimizers = force_list(optimizer)
        losses = force_list(loss)

        if is_overridden(self.compute_gradients_fn):
            # New API: Allow more than one optimizer -> Return a list of
            # lists of gradients.
            if self.config["_tf_policy_handles_more_than_one_loss"]:
                return self.compute_gradients_fn(optimizers, losses)
            # Old API: Return a single List of gradients.
            else:
                return self.compute_gradients_fn(optimizers[0], losses[0])
        else:
            return super().gradients(optimizers, losses)
