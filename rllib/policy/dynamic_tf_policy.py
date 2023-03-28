from collections import namedtuple, OrderedDict
import gymnasium as gym
import logging
import re
import tree  # pip install dm_tree
from typing import Callable, Dict, List, Optional, Tuple, Type, Union

from ray.util.debug import log_once
from ray.rllib.models.tf.tf_action_dist import TFActionDistribution
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.tf_policy import TFPolicy
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.utils import force_list
from ray.rllib.utils.annotations import override, DeveloperAPI
from ray.rllib.utils.debug import summarize
from ray.rllib.utils.deprecation import deprecation_warning, DEPRECATED_VALUE
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.metrics import (
    DIFF_NUM_GRAD_UPDATES_VS_SAMPLER_POLICY,
    NUM_GRAD_UPDATES_LIFETIME,
)
from ray.rllib.utils.spaces.space_utils import get_dummy_batch_for_space
from ray.rllib.utils.tf_utils import get_placeholder
from ray.rllib.utils.typing import (
    LocalOptimizer,
    ModelGradients,
    TensorType,
    AlgorithmConfigDict,
)

tf1, tf, tfv = try_import_tf()

logger = logging.getLogger(__name__)

# Variable scope in which created variables will be placed under.
TOWER_SCOPE_NAME = "tower"


@DeveloperAPI
class DynamicTFPolicy(TFPolicy):
    """A TFPolicy that auto-defines placeholders dynamically at runtime.

    Do not sub-class this class directly (neither should you sub-class
    TFPolicy), but rather use rllib.policy.tf_policy_template.build_tf_policy
    to generate your custom tf (graph-mode or eager) Policy classes.
    """

    @DeveloperAPI
    def __init__(
        self,
        obs_space: gym.spaces.Space,
        action_space: gym.spaces.Space,
        config: AlgorithmConfigDict,
        loss_fn: Callable[
            [Policy, ModelV2, Type[TFActionDistribution], SampleBatch], TensorType
        ],
        *,
        stats_fn: Optional[
            Callable[[Policy, SampleBatch], Dict[str, TensorType]]
        ] = None,
        grad_stats_fn: Optional[
            Callable[[Policy, SampleBatch, ModelGradients], Dict[str, TensorType]]
        ] = None,
        before_loss_init: Optional[
            Callable[
                [Policy, gym.spaces.Space, gym.spaces.Space, AlgorithmConfigDict], None
            ]
        ] = None,
        make_model: Optional[
            Callable[
                [Policy, gym.spaces.Space, gym.spaces.Space, AlgorithmConfigDict],
                ModelV2,
            ]
        ] = None,
        action_sampler_fn: Optional[
            Callable[
                [TensorType, List[TensorType]],
                Union[
                    Tuple[TensorType, TensorType],
                    Tuple[TensorType, TensorType, TensorType, List[TensorType]],
                ],
            ]
        ] = None,
        action_distribution_fn: Optional[
            Callable[
                [Policy, ModelV2, TensorType, TensorType, TensorType],
                Tuple[TensorType, type, List[TensorType]],
            ]
        ] = None,
        existing_inputs: Optional[Dict[str, "tf1.placeholder"]] = None,
        existing_model: Optional[ModelV2] = None,
        get_batch_divisibility_req: Optional[Callable[[Policy], int]] = None,
        obs_include_prev_action_reward=DEPRECATED_VALUE,
    ):
        """Initializes a DynamicTFPolicy instance.

        Initialization of this class occurs in two phases and defines the
        static graph.

        Phase 1: The model is created and model variables are initialized.

        Phase 2: A fake batch of data is created, sent to the trajectory
        postprocessor, and then used to create placeholders for the loss
        function. The loss and stats functions are initialized with these
        placeholders.

        Args:
            observation_space: Observation space of the policy.
            action_space: Action space of the policy.
            config: Policy-specific configuration data.
            loss_fn: Function that returns a loss tensor for the policy graph.
            stats_fn: Optional callable that - given the policy and batch
                input tensors - returns a dict mapping str to TF ops.
                These ops are fetched from the graph after loss calculations
                and the resulting values can be found in the results dict
                returned by e.g. `Algorithm.train()` or in tensorboard (if TB
                logging is enabled).
            grad_stats_fn: Optional callable that - given the policy, batch
                input tensors, and calculated loss gradient tensors - returns
                a dict mapping str to TF ops. These ops are fetched from the
                graph after loss and gradient calculations and the resulting
                values can be found in the results dict returned by e.g.
                `Algorithm.train()` or in tensorboard (if TB logging is
                enabled).
            before_loss_init: Optional function to run prior to
                loss init that takes the same arguments as __init__.
            make_model: Optional function that returns a ModelV2 object
                given policy, obs_space, action_space, and policy config.
                All policy variables should be created in this function. If not
                specified, a default model will be created.
            action_sampler_fn: A callable returning either a sampled action and
                its log-likelihood or a sampled action, its log-likelihood,
                action distribution inputs and updated state given Policy,
                ModelV2, observation inputs, explore, and is_training.
                Provide `action_sampler_fn` if you would like to have full
                control over the action computation step, including the
                model forward pass, possible sampling from a distribution,
                and exploration logic.
                Note: If `action_sampler_fn` is given, `action_distribution_fn`
                must be None. If both `action_sampler_fn` and
                `action_distribution_fn` are None, RLlib will simply pass
                inputs through `self.model` to get distribution inputs, create
                the distribution object, sample from it, and apply some
                exploration logic to the results.
                The callable takes as inputs: Policy, ModelV2, obs_batch,
                state_batches (optional), seq_lens (optional),
                prev_actions_batch (optional), prev_rewards_batch (optional),
                explore, and is_training.
            action_distribution_fn: A callable returning distribution inputs
                (parameters), a dist-class to generate an action distribution
                object from, and internal-state outputs (or an empty list if
                not applicable).
                Provide `action_distribution_fn` if you would like to only
                customize the model forward pass call. The resulting
                distribution parameters are then used by RLlib to create a
                distribution object, sample from it, and execute any
                exploration logic.
                Note: If `action_distribution_fn` is given, `action_sampler_fn`
                must be None. If both `action_sampler_fn` and
                `action_distribution_fn` are None, RLlib will simply pass
                inputs through `self.model` to get distribution inputs, create
                the distribution object, sample from it, and apply some
                exploration logic to the results.
                The callable takes as inputs: Policy, ModelV2, input_dict,
                explore, timestep, is_training.
            existing_inputs: When copying a policy, this specifies an existing
                dict of placeholders to use instead of defining new ones.
            existing_model: When copying a policy, this specifies an existing
                model to clone and share weights with.
            get_batch_divisibility_req: Optional callable that returns the
                divisibility requirement for sample batches. If None, will
                assume a value of 1.
        """
        if obs_include_prev_action_reward != DEPRECATED_VALUE:
            deprecation_warning(old="obs_include_prev_action_reward", error=True)
        self.observation_space = obs_space
        self.action_space = action_space
        self.config = config
        self.framework = "tf"
        self._loss_fn = loss_fn
        self._stats_fn = stats_fn
        self._grad_stats_fn = grad_stats_fn
        self._seq_lens = None
        self._is_tower = existing_inputs is not None

        dist_class = None
        if action_sampler_fn or action_distribution_fn:
            if not make_model:
                raise ValueError(
                    "`make_model` is required if `action_sampler_fn` OR "
                    "`action_distribution_fn` is given"
                )
        else:
            dist_class, logit_dim = ModelCatalog.get_action_dist(
                action_space, self.config["model"]
            )

        # Setup self.model.
        if existing_model:
            if isinstance(existing_model, list):
                self.model = existing_model[0]
                # TODO: (sven) hack, but works for `target_[q_]?model`.
                for i in range(1, len(existing_model)):
                    setattr(self, existing_model[i][0], existing_model[i][1])
        elif make_model:
            self.model = make_model(self, obs_space, action_space, config)
        else:
            self.model = ModelCatalog.get_model_v2(
                obs_space=obs_space,
                action_space=action_space,
                num_outputs=logit_dim,
                model_config=self.config["model"],
                framework="tf",
            )
        # Auto-update model's inference view requirements, if recurrent.
        self._update_model_view_requirements_from_init_state()

        # Input placeholders already given -> Use these.
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

        # Use default settings.
        # Add NEXT_OBS, STATE_IN_0.., and others.
        self.view_requirements = self._get_default_view_requirements()
        # Combine view_requirements for Model and Policy.
        self.view_requirements.update(self.model.view_requirements)
        # Disable env-info placeholder.
        if SampleBatch.INFOS in self.view_requirements:
            self.view_requirements[SampleBatch.INFOS].used_for_training = False

        # Setup standard placeholders.
        if self._is_tower:
            timestep = existing_inputs["timestep"]
            explore = False
            self._input_dict, self._dummy_batch = self._get_input_dict_and_dummy_batch(
                self.view_requirements, existing_inputs
            )
        else:
            if not self.config.get("_disable_action_flattening"):
                action_ph = ModelCatalog.get_action_placeholder(action_space)
                prev_action_ph = {}
                if SampleBatch.PREV_ACTIONS not in self.view_requirements:
                    prev_action_ph = {
                        SampleBatch.PREV_ACTIONS: ModelCatalog.get_action_placeholder(
                            action_space, "prev_action"
                        )
                    }
                (
                    self._input_dict,
                    self._dummy_batch,
                ) = self._get_input_dict_and_dummy_batch(
                    self.view_requirements,
                    dict({SampleBatch.ACTIONS: action_ph}, **prev_action_ph),
                )
            else:
                (
                    self._input_dict,
                    self._dummy_batch,
                ) = self._get_input_dict_and_dummy_batch(self.view_requirements, {})
            # Placeholder for (sampling steps) timestep (int).
            timestep = tf1.placeholder_with_default(
                tf.zeros((), dtype=tf.int64), (), name="timestep"
            )
            # Placeholder for `is_exploring` flag.
            explore = tf1.placeholder_with_default(True, (), name="is_exploring")

        # Placeholder for `is_training` flag.
        self._input_dict.set_training(self._get_is_training_placeholder())

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
            if action_sampler_fn:
                action_sampler_outputs = action_sampler_fn(
                    self,
                    self.model,
                    obs_batch=self._input_dict[SampleBatch.CUR_OBS],
                    state_batches=self._state_inputs,
                    seq_lens=self._seq_lens,
                    prev_action_batch=self._input_dict.get(SampleBatch.PREV_ACTIONS),
                    prev_reward_batch=self._input_dict.get(SampleBatch.PREV_REWARDS),
                    explore=explore,
                    is_training=self._input_dict.is_training,
                )
                if len(action_sampler_outputs) == 4:
                    (
                        sampled_action,
                        sampled_action_logp,
                        dist_inputs,
                        self._state_out,
                    ) = action_sampler_outputs
                else:
                    dist_inputs = None
                    self._state_out = []
                    sampled_action, sampled_action_logp = action_sampler_outputs
            # Distribution generation is customized, e.g., DQN, DDPG.
            else:
                if action_distribution_fn:

                    # Try new action_distribution_fn signature, supporting
                    # state_batches and seq_lens.
                    in_dict = self._input_dict
                    try:
                        (
                            dist_inputs,
                            dist_class,
                            self._state_out,
                        ) = action_distribution_fn(
                            self,
                            self.model,
                            input_dict=in_dict,
                            state_batches=self._state_inputs,
                            seq_lens=self._seq_lens,
                            explore=explore,
                            timestep=timestep,
                            is_training=in_dict.is_training,
                        )
                    # Trying the old way (to stay backward compatible).
                    # TODO: Remove in future.
                    except TypeError as e:
                        if (
                            "positional argument" in e.args[0]
                            or "unexpected keyword argument" in e.args[0]
                        ):
                            (
                                dist_inputs,
                                dist_class,
                                self._state_out,
                            ) = action_distribution_fn(
                                self,
                                self.model,
                                obs_batch=in_dict[SampleBatch.CUR_OBS],
                                state_batches=self._state_inputs,
                                seq_lens=self._seq_lens,
                                prev_action_batch=in_dict.get(SampleBatch.PREV_ACTIONS),
                                prev_reward_batch=in_dict.get(SampleBatch.PREV_REWARDS),
                                explore=explore,
                                is_training=in_dict.is_training,
                            )
                        else:
                            raise e

                # Default distribution generation behavior:
                # Pass through model. E.g., PG, PPO.
                else:
                    if isinstance(self.model, tf.keras.Model):
                        dist_inputs, self._state_out, extra_action_fetches = self.model(
                            self._input_dict
                        )
                    else:
                        dist_inputs, self._state_out = self.model(self._input_dict)

                action_dist = dist_class(dist_inputs, self.model)

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

        # Phase 1 init.
        sess = tf1.get_default_session() or tf1.Session(
            config=tf1.ConfigProto(**self.config["tf_session_args"])
        )

        batch_divisibility_req = (
            get_batch_divisibility_req(self)
            if callable(get_batch_divisibility_req)
            else (get_batch_divisibility_req or 1)
        )

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
            dist_class=dist_class,
            loss=None,  # dynamically initialized on run
            loss_inputs=[],
            model=self.model,
            state_inputs=self._state_inputs,
            state_outputs=self._state_out,
            prev_action_input=prev_action_input,
            prev_reward_input=prev_reward_input,
            seq_lens=self._seq_lens,
            max_seq_len=config["model"]["max_seq_len"],
            batch_divisibility_req=batch_divisibility_req,
            explore=explore,
            timestep=timestep,
        )

        # Phase 2 init.
        if before_loss_init is not None:
            before_loss_init(self, obs_space, action_space, config)
        if hasattr(self, "_extra_action_fetches"):
            self._extra_action_fetches.update(extra_action_fetches)
        else:
            self._extra_action_fetches = extra_action_fetches

        # Loss initialization and model/postprocessing test calls.
        if not self._is_tower:
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
                        for i in range(self.config.get("num_multi_gpu_tower_stacks", 1))
                    ]

            # Initialize again after loss and tower init.
            self.get_session().run(tf1.global_variables_initializer())

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
        if instance._grad_stats_fn:
            instance._stats_fetches.update(
                instance._grad_stats_fn(instance, input_dict, instance._grads)
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

    def _get_input_dict_and_dummy_batch(self, view_requirements, existing_inputs):
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

    @override(Policy)
    def _initialize_loss_from_dummy_batch(
        self, auto_remove_unneeded_view_reqs: bool = True, stats_fn=None
    ) -> None:

        # Create the optimizer/exploration optimizer here. Some initialization
        # steps (e.g. exploration postprocessing) may need this.
        if not self._optimizers:
            self._optimizers = force_list(self.optimizer())
            # Backward compatibility.
            self._optimizer = self._optimizers[0]

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
        for key, value in self._extra_action_fetches.items():
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
        if self._grad_stats_fn:
            self._stats_fetches.update(
                self._grad_stats_fn(self, train_batch, self._grads)
            )

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
        losses = self._loss_fn(self, self.model, self.dist_class, train_batch)
        losses = force_list(losses)
        if self._stats_fn:
            self._stats_fetches.update(self._stats_fn(self, train_batch))
        # Override the update ops to be those of the model.
        self._update_ops = []
        if not isinstance(self.model, tf.keras.Model):
            self._update_ops = self.model.update_ops()
        return losses


@DeveloperAPI
class TFMultiGPUTowerStack:
    """Optimizer that runs in parallel across multiple local devices.

    TFMultiGPUTowerStack automatically splits up and loads training data
    onto specified local devices (e.g. GPUs) with `load_data()`. During a call
    to `optimize()`, the devices compute gradients over slices of the data in
    parallel. The gradients are then averaged and applied to the shared
    weights.

    The data loaded is pinned in device memory until the next call to
    `load_data`, so you can make multiple passes (possibly in randomized order)
    over the same data once loaded.

    This is similar to tf1.train.SyncReplicasOptimizer, but works within a
    single TensorFlow graph, i.e. implements in-graph replicated training:

    https://www.tensorflow.org/api_docs/python/tf/train/SyncReplicasOptimizer
    """

    def __init__(
        self,
        # Deprecated.
        optimizer=None,
        devices=None,
        input_placeholders=None,
        rnn_inputs=None,
        max_per_device_batch_size=None,
        build_graph=None,
        grad_norm_clipping=None,
        # Use only `policy` argument from here on.
        policy: TFPolicy = None,
    ):
        """Initializes a TFMultiGPUTowerStack instance.

        Args:
            policy: The TFPolicy object that this tower stack
                belongs to.
        """
        # Obsoleted usage, use only `policy` arg from here on.
        if policy is None:
            deprecation_warning(
                old="TFMultiGPUTowerStack(...)",
                new="TFMultiGPUTowerStack(policy=[Policy])",
                error=True,
            )
            self.policy = None
            self.optimizers = optimizer
            self.devices = devices
            self.max_per_device_batch_size = max_per_device_batch_size
            self.policy_copy = build_graph
        else:
            self.policy: TFPolicy = policy
            self.optimizers: List[LocalOptimizer] = self.policy._optimizers
            self.devices = self.policy.devices
            self.max_per_device_batch_size = (
                max_per_device_batch_size
                or policy.config.get(
                    "sgd_minibatch_size", policy.config.get("train_batch_size", 999999)
                )
            ) // len(self.devices)
            input_placeholders = tree.flatten(self.policy._loss_input_dict_no_rnn)
            rnn_inputs = []
            if self.policy._state_inputs:
                rnn_inputs = self.policy._state_inputs + [self.policy._seq_lens]
            grad_norm_clipping = self.policy.config.get("grad_clip")
            self.policy_copy = self.policy.copy

        assert len(self.devices) > 1 or "gpu" in self.devices[0]
        self.loss_inputs = input_placeholders + rnn_inputs

        shared_ops = tf1.get_collection(
            tf1.GraphKeys.UPDATE_OPS, scope=tf1.get_variable_scope().name
        )

        # Then setup the per-device loss graphs that use the shared weights
        self._batch_index = tf1.placeholder(tf.int32, name="batch_index")

        # Dynamic batch size, which may be shrunk if there isn't enough data
        self._per_device_batch_size = tf1.placeholder(
            tf.int32, name="per_device_batch_size"
        )
        self._loaded_per_device_batch_size = max_per_device_batch_size

        # When loading RNN input, we dynamically determine the max seq len
        self._max_seq_len = tf1.placeholder(tf.int32, name="max_seq_len")
        self._loaded_max_seq_len = 1

        device_placeholders = [[] for _ in range(len(self.devices))]

        for t in tree.flatten(self.loss_inputs):
            # Split on the CPU in case the data doesn't fit in GPU memory.
            with tf.device("/cpu:0"):
                splits = tf.split(t, len(self.devices))
            for i, d in enumerate(self.devices):
                device_placeholders[i].append(splits[i])

        self._towers = []
        for tower_i, (device, placeholders) in enumerate(
            zip(self.devices, device_placeholders)
        ):
            self._towers.append(
                self._setup_device(
                    tower_i, device, placeholders, len(tree.flatten(input_placeholders))
                )
            )

        if self.policy.config["_tf_policy_handles_more_than_one_loss"]:
            avgs = []
            for i, optim in enumerate(self.optimizers):
                avg = _average_gradients([t.grads[i] for t in self._towers])
                if grad_norm_clipping:
                    clipped = []
                    for grad, _ in avg:
                        clipped.append(grad)
                    clipped, _ = tf.clip_by_global_norm(clipped, grad_norm_clipping)
                    for i, (grad, var) in enumerate(avg):
                        avg[i] = (clipped[i], var)
                avgs.append(avg)

            # Gather update ops for any batch norm layers.
            # TODO(ekl) here we
            #  will use all the ops found which won't work for DQN / DDPG, but
            #  those aren't supported with multi-gpu right now anyways.
            self._update_ops = tf1.get_collection(
                tf1.GraphKeys.UPDATE_OPS, scope=tf1.get_variable_scope().name
            )
            for op in shared_ops:
                self._update_ops.remove(op)  # only care about tower update ops
            if self._update_ops:
                logger.debug(
                    "Update ops to run on apply gradient: {}".format(self._update_ops)
                )

            with tf1.control_dependencies(self._update_ops):
                self._train_op = tf.group(
                    [o.apply_gradients(a) for o, a in zip(self.optimizers, avgs)]
                )
        else:
            avg = _average_gradients([t.grads for t in self._towers])
            if grad_norm_clipping:
                clipped = []
                for grad, _ in avg:
                    clipped.append(grad)
                clipped, _ = tf.clip_by_global_norm(clipped, grad_norm_clipping)
                for i, (grad, var) in enumerate(avg):
                    avg[i] = (clipped[i], var)

            # Gather update ops for any batch norm layers.
            # TODO(ekl) here we
            #  will use all the ops found which won't work for DQN / DDPG, but
            #  those aren't supported with multi-gpu right now anyways.
            self._update_ops = tf1.get_collection(
                tf1.GraphKeys.UPDATE_OPS, scope=tf1.get_variable_scope().name
            )
            for op in shared_ops:
                self._update_ops.remove(op)  # only care about tower update ops
            if self._update_ops:
                logger.debug(
                    "Update ops to run on apply gradient: {}".format(self._update_ops)
                )

            with tf1.control_dependencies(self._update_ops):
                self._train_op = self.optimizers[0].apply_gradients(avg)

        # The lifetime number of gradient updates that the policy having sent
        # some data (SampleBatchType) into this tower stack's GPU buffer(s) has already
        # undergone.
        self.num_grad_updates = 0

    def load_data(self, sess, inputs, state_inputs, num_grad_updates=None):
        """Bulk loads the specified inputs into device memory.

        The shape of the inputs must conform to the shapes of the input
        placeholders this optimizer was constructed with.

        The data is split equally across all the devices. If the data is not
        evenly divisible by the batch size, excess data will be discarded.

        Args:
            sess: TensorFlow session.
            inputs: List of arrays matching the input placeholders, of shape
                [BATCH_SIZE, ...].
            state_inputs: List of RNN input arrays. These arrays have size
                [BATCH_SIZE / MAX_SEQ_LEN, ...].
            num_grad_updates: The lifetime number of gradient updates that the
                policy having collected the data has already undergone.

        Returns:
            The number of tuples loaded per device.
        """
        self.num_grad_updates = num_grad_updates

        if log_once("load_data"):
            logger.info(
                "Training on concatenated sample batches:\n\n{}\n".format(
                    summarize(
                        {
                            "placeholders": self.loss_inputs,
                            "inputs": inputs,
                            "state_inputs": state_inputs,
                        }
                    )
                )
            )

        feed_dict = {}
        assert len(self.loss_inputs) == len(inputs + state_inputs), (
            self.loss_inputs,
            inputs,
            state_inputs,
        )

        # Let's suppose we have the following input data, and 2 devices:
        # 1 2 3 4 5 6 7                              <- state inputs shape
        # A A A B B B C C C D D D E E E F F F G G G  <- inputs shape
        # The data is truncated and split across devices as follows:
        # |---| seq len = 3
        # |---------------------------------| seq batch size = 6 seqs
        # |----------------| per device batch size = 9 tuples

        if len(state_inputs) > 0:
            smallest_array = state_inputs[0]
            seq_len = len(inputs[0]) // len(state_inputs[0])
            self._loaded_max_seq_len = seq_len
        else:
            smallest_array = inputs[0]
            self._loaded_max_seq_len = 1

        sequences_per_minibatch = (
            self.max_per_device_batch_size
            // self._loaded_max_seq_len
            * len(self.devices)
        )
        if sequences_per_minibatch < 1:
            logger.warning(
                (
                    "Target minibatch size is {}, however the rollout sequence "
                    "length is {}, hence the minibatch size will be raised to "
                    "{}."
                ).format(
                    self.max_per_device_batch_size,
                    self._loaded_max_seq_len,
                    self._loaded_max_seq_len * len(self.devices),
                )
            )
            sequences_per_minibatch = 1

        if len(smallest_array) < sequences_per_minibatch:
            # Dynamically shrink the batch size if insufficient data
            sequences_per_minibatch = _make_divisible_by(
                len(smallest_array), len(self.devices)
            )

        if log_once("data_slicing"):
            logger.info(
                (
                    "Divided {} rollout sequences, each of length {}, among "
                    "{} devices."
                ).format(
                    len(smallest_array), self._loaded_max_seq_len, len(self.devices)
                )
            )

        if sequences_per_minibatch < len(self.devices):
            raise ValueError(
                "Must load at least 1 tuple sequence per device. Try "
                "increasing `sgd_minibatch_size` or reducing `max_seq_len` "
                "to ensure that at least one sequence fits per device."
            )
        self._loaded_per_device_batch_size = (
            sequences_per_minibatch // len(self.devices) * self._loaded_max_seq_len
        )

        if len(state_inputs) > 0:
            # First truncate the RNN state arrays to the sequences_per_minib.
            state_inputs = [
                _make_divisible_by(arr, sequences_per_minibatch) for arr in state_inputs
            ]
            # Then truncate the data inputs to match
            inputs = [arr[: len(state_inputs[0]) * seq_len] for arr in inputs]
            assert len(state_inputs[0]) * seq_len == len(inputs[0]), (
                len(state_inputs[0]),
                sequences_per_minibatch,
                seq_len,
                len(inputs[0]),
            )
            for ph, arr in zip(self.loss_inputs, inputs + state_inputs):
                feed_dict[ph] = arr
            truncated_len = len(inputs[0])
        else:
            truncated_len = 0
            for ph, arr in zip(self.loss_inputs, inputs):
                truncated_arr = _make_divisible_by(arr, sequences_per_minibatch)
                feed_dict[ph] = truncated_arr
                if truncated_len == 0:
                    truncated_len = len(truncated_arr)

        sess.run([t.init_op for t in self._towers], feed_dict=feed_dict)

        self.num_tuples_loaded = truncated_len
        samples_per_device = truncated_len // len(self.devices)
        assert samples_per_device > 0, "No data loaded?"
        assert samples_per_device % self._loaded_per_device_batch_size == 0
        # Return loaded samples per-device.
        return samples_per_device

    def optimize(self, sess, batch_index):
        """Run a single step of SGD.

        Runs a SGD step over a slice of the preloaded batch with size given by
        self._loaded_per_device_batch_size and offset given by the batch_index
        argument.

        Updates shared model weights based on the averaged per-device
        gradients.

        Args:
            sess: TensorFlow session.
            batch_index: Offset into the preloaded data. This value must be
                between `0` and `tuples_per_device`. The amount of data to
                process is at most `max_per_device_batch_size`.

        Returns:
            The outputs of extra_ops evaluated over the batch.
        """
        feed_dict = {
            self._batch_index: batch_index,
            self._per_device_batch_size: self._loaded_per_device_batch_size,
            self._max_seq_len: self._loaded_max_seq_len,
        }
        for tower in self._towers:
            feed_dict.update(tower.loss_graph.extra_compute_grad_feed_dict())

        fetches = {"train": self._train_op}
        for tower_num, tower in enumerate(self._towers):
            tower_fetch = tower.loss_graph._get_grad_and_stats_fetches()
            fetches["tower_{}".format(tower_num)] = tower_fetch

        return sess.run(fetches, feed_dict=feed_dict)

    def get_device_losses(self):
        return [t.loss_graph for t in self._towers]

    def _setup_device(self, tower_i, device, device_input_placeholders, num_data_in):
        assert num_data_in <= len(device_input_placeholders)
        with tf.device(device):
            with tf1.name_scope(TOWER_SCOPE_NAME + f"_{tower_i}"):
                device_input_batches = []
                device_input_slices = []
                for i, ph in enumerate(device_input_placeholders):
                    current_batch = tf1.Variable(
                        ph, trainable=False, validate_shape=False, collections=[]
                    )
                    device_input_batches.append(current_batch)
                    if i < num_data_in:
                        scale = self._max_seq_len
                        granularity = self._max_seq_len
                    else:
                        scale = self._max_seq_len
                        granularity = 1
                    current_slice = tf.slice(
                        current_batch,
                        (
                            [self._batch_index // scale * granularity]
                            + [0] * len(ph.shape[1:])
                        ),
                        (
                            [self._per_device_batch_size // scale * granularity]
                            + [-1] * len(ph.shape[1:])
                        ),
                    )
                    current_slice.set_shape(ph.shape)
                    device_input_slices.append(current_slice)
                graph_obj = self.policy_copy(device_input_slices)
                device_grads = graph_obj.gradients(self.optimizers, graph_obj._losses)
            return _Tower(
                tf.group(*[batch.initializer for batch in device_input_batches]),
                device_grads,
                graph_obj,
            )


# Each tower is a copy of the loss graph pinned to a specific device.
_Tower = namedtuple("Tower", ["init_op", "grads", "loss_graph"])


def _make_divisible_by(a, n):
    if type(a) is int:
        return a - a % n
    return a[0 : a.shape[0] - a.shape[0] % n]


def _average_gradients(tower_grads):
    """Averages gradients across towers.

    Calculate the average gradient for each shared variable across all towers.
    Note that this function provides a synchronization point across all towers.

    Args:
        tower_grads: List of lists of (gradient, variable) tuples. The outer
            list is over individual gradients. The inner list is over the
            gradient calculation for each tower.

    Returns:
       List of pairs of (gradient, variable) where the gradient has been
           averaged across all towers.

    TODO(ekl): We could use NCCL if this becomes a bottleneck.
    """

    average_grads = []
    for grad_and_vars in zip(*tower_grads):

        # Note that each grad_and_vars looks like the following:
        #   ((grad0_gpu0, var0_gpu0), ... , (grad0_gpuN, var0_gpuN))
        grads = []
        for g, _ in grad_and_vars:
            if g is not None:
                # Add 0 dimension to the gradients to represent the tower.
                expanded_g = tf.expand_dims(g, 0)

                # Append on a 'tower' dimension which we will average over
                # below.
                grads.append(expanded_g)

        if not grads:
            continue

        # Average over the 'tower' dimension.
        grad = tf.concat(axis=0, values=grads)
        grad = tf.reduce_mean(grad, 0)

        # Keep in mind that the Variables are redundant because they are shared
        # across towers. So .. we will just return the first tower's pointer to
        # the Variable.
        v = grad_and_vars[0][1]
        grad_and_var = (grad, v)
        average_grads.append(grad_and_var)

    return average_grads
