from collections import OrderedDict
import gym
import logging
import numpy as np
import re
from typing import Callable, Dict, List, Optional, Tuple, Type

from ray.util.debug import log_once
from ray.rllib.models.tf.tf_action_dist import TFActionDistribution
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.tf_policy import TFPolicy
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.utils.annotations import override, DeveloperAPI
from ray.rllib.utils.debug import summarize
from ray.rllib.utils.deprecation import deprecation_warning, DEPRECATED_VALUE
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.tf_ops import get_placeholder
from ray.rllib.utils.typing import ModelGradients, TensorType, \
    TrainerConfigDict

tf1, tf, tfv = try_import_tf()

logger = logging.getLogger(__name__)


@DeveloperAPI
class DynamicTFPolicy(TFPolicy):
    """A TFPolicy that auto-defines placeholders dynamically at runtime.

    Do not sub-class this class directly (neither should you sub-class
    TFPolicy), but rather use rllib.policy.tf_policy_template.build_tf_policy
    to generate your custom tf (graph-mode or eager) Policy classes.

    Initialization of this class occurs in two phases.
      * Phase 1: the model is created and model variables are initialized.
      * Phase 2: a fake batch of data is created, sent to the trajectory
        postprocessor, and then used to create placeholders for the loss
        function. The loss and stats functions are initialized with these
        placeholders.

    Initialization defines the static graph.

    Attributes:
        observation_space (gym.Space): observation space of the policy.
        action_space (gym.Space): action space of the policy.
        config (dict): config of the policy
        model (TorchModel): TF model instance
        dist_class (type): TF action distribution class
    """

    @DeveloperAPI
    def __init__(
            self,
            obs_space: gym.spaces.Space,
            action_space: gym.spaces.Space,
            config: TrainerConfigDict,
            loss_fn: Callable[[
                Policy, ModelV2, Type[TFActionDistribution], SampleBatch
            ], TensorType],
            *,
            stats_fn: Optional[Callable[[Policy, SampleBatch], Dict[
                str, TensorType]]] = None,
            grad_stats_fn: Optional[Callable[[
                Policy, SampleBatch, ModelGradients
            ], Dict[str, TensorType]]] = None,
            before_loss_init: Optional[Callable[[
                Policy, gym.spaces.Space, gym.spaces.Space, TrainerConfigDict
            ], None]] = None,
            make_model: Optional[Callable[[
                Policy, gym.spaces.Space, gym.spaces.Space, TrainerConfigDict
            ], ModelV2]] = None,
            action_sampler_fn: Optional[Callable[[
                TensorType, List[TensorType]
            ], Tuple[TensorType, TensorType]]] = None,
            action_distribution_fn: Optional[Callable[[
                Policy, ModelV2, TensorType, TensorType, TensorType
            ], Tuple[TensorType, type, List[TensorType]]]] = None,
            existing_inputs: Optional[Dict[str, "tf1.placeholder"]] = None,
            existing_model: Optional[ModelV2] = None,
            get_batch_divisibility_req: Optional[Callable[[Policy],
                                                          int]] = None,
            obs_include_prev_action_reward=DEPRECATED_VALUE):
        """Initialize a dynamic TF policy.

        Args:
            observation_space (gym.spaces.Space): Observation space of the
                policy.
            action_space (gym.spaces.Space): Action space of the policy.
            config (TrainerConfigDict): Policy-specific configuration data.
            loss_fn (Callable[[Policy, ModelV2, Type[TFActionDistribution],
                SampleBatch], TensorType]): Function that returns a loss tensor
                for the policy graph.
            stats_fn (Optional[Callable[[Policy, SampleBatch],
                Dict[str, TensorType]]]): Optional function that returns a dict
                of TF fetches given the policy and batch input tensors.
            grad_stats_fn (Optional[Callable[[Policy, SampleBatch,
                ModelGradients], Dict[str, TensorType]]]):
                Optional function that returns a dict of TF fetches given the
                policy, sample batch, and loss gradient tensors.
            before_loss_init (Optional[Callable[
                [Policy, gym.spaces.Space, gym.spaces.Space,
                TrainerConfigDict], None]]): Optional function to run prior to
                loss init that takes the same arguments as __init__.
            make_model (Optional[Callable[[Policy, gym.spaces.Space,
                gym.spaces.Space, TrainerConfigDict], ModelV2]]): Optional
                function that returns a ModelV2 object given
                policy, obs_space, action_space, and policy config.
                All policy variables should be created in this function. If not
                specified, a default model will be created.
            action_sampler_fn (Optional[Callable[[Policy, ModelV2, Dict[
                str, TensorType], TensorType, TensorType], Tuple[TensorType,
                TensorType]]]): A callable returning a sampled action and its
                log-likelihood given Policy, ModelV2, input_dict, explore,
                timestep, and is_training.
            action_distribution_fn (Optional[Callable[[Policy, ModelV2,
                Dict[str, TensorType], TensorType, TensorType],
                Tuple[TensorType, type, List[TensorType]]]]): A callable
                returning distribution inputs (parameters), a dist-class to
                generate an action distribution object from, and
                internal-state outputs (or an empty list if not applicable).
                Note: No Exploration hooks have to be called from within
                `action_distribution_fn`. It's should only perform a simple
                forward pass through some model.
                If None, pass inputs through `self.model()` to get distribution
                inputs.
                The callable takes as inputs: Policy, ModelV2, input_dict,
                explore, timestep, is_training.
            existing_inputs (Optional[Dict[str, tf1.placeholder]]): When
                copying a policy, this specifies an existing dict of
                placeholders to use instead of defining new ones.
            existing_model (Optional[ModelV2]): When copying a policy, this
                specifies an existing model to clone and share weights with.
            get_batch_divisibility_req (Optional[Callable[[Policy], int]]):
                Optional callable that returns the divisibility requirement for
                sample batches. If None, will assume a value of 1.
        """
        if obs_include_prev_action_reward != DEPRECATED_VALUE:
            deprecation_warning(
                old="obs_include_prev_action_reward", error=False)
        self.observation_space = obs_space
        self.action_space = action_space
        self.config = config
        self.framework = "tf"
        self._loss_fn = loss_fn
        self._stats_fn = stats_fn
        self._grad_stats_fn = grad_stats_fn

        dist_class = dist_inputs = None
        if action_sampler_fn or action_distribution_fn:
            if not make_model:
                raise ValueError(
                    "`make_model` is required if `action_sampler_fn` OR "
                    "`action_distribution_fn` is given")
        else:
            dist_class, logit_dim = ModelCatalog.get_action_dist(
                action_space, self.config["model"])

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
                framework="tf")
        # Auto-update model's inference view requirements, if recurrent.
        self._update_model_view_requirements_from_init_state()

        if existing_inputs:
            self._state_inputs = [
                v for k, v in existing_inputs.items()
                if k.startswith("state_in_")
            ]
            if self._state_inputs:
                self._seq_lens = existing_inputs["seq_lens"]
        else:
            self._state_inputs = [
                get_placeholder(
                    space=vr.space,
                    time_axis=not isinstance(vr.shift, int),
                ) for k, vr in self.model.view_requirements.items()
                if k.startswith("state_in_")
            ]

        # Use default settings.
        # Add NEXT_OBS, STATE_IN_0.., and others.
        self.view_requirements = self._get_default_view_requirements()
        # Combine view_requirements for Model and Policy.
        self.view_requirements.update(self.model.view_requirements)

        # Setup standard placeholders.
        if existing_inputs is not None:
            timestep = existing_inputs["timestep"]
            explore = existing_inputs["is_exploring"]
            self._input_dict, self._dummy_batch = \
                self._get_input_dict_and_dummy_batch(
                    self.view_requirements, existing_inputs)
        else:
            action_ph = ModelCatalog.get_action_placeholder(action_space)
            prev_action_ph = {}
            if SampleBatch.PREV_ACTIONS not in self.view_requirements:
                prev_action_ph = {
                    SampleBatch.PREV_ACTIONS: ModelCatalog.
                    get_action_placeholder(action_space, "prev_action")
                }
            self._input_dict, self._dummy_batch = \
                self._get_input_dict_and_dummy_batch(
                    self.view_requirements,
                    dict({SampleBatch.ACTIONS: action_ph},
                         **prev_action_ph))
            # Placeholder for (sampling steps) timestep (int).
            timestep = tf1.placeholder_with_default(
                tf.zeros((), dtype=tf.int64), (), name="timestep")
            # Placeholder for `is_exploring` flag.
            explore = tf1.placeholder_with_default(
                True, (), name="is_exploring")

        # Placeholder for RNN time-chunk valid lengths.
        self._seq_lens = tf1.placeholder(
            dtype=tf.int32, shape=[None], name="seq_lens")
        # Placeholder for `is_training` flag.
        self._input_dict["is_training"] = self._get_is_training_placeholder()

        # Create the Exploration object to use for this Policy.
        self.exploration = self._create_exploration()

        # Fully customized action generation (e.g., custom policy).
        if action_sampler_fn:
            sampled_action, sampled_action_logp = action_sampler_fn(
                self,
                self.model,
                obs_batch=self._input_dict[SampleBatch.CUR_OBS],
                state_batches=self._state_inputs,
                seq_lens=self._seq_lens,
                prev_action_batch=self._input_dict.get(
                    SampleBatch.PREV_ACTIONS),
                prev_reward_batch=self._input_dict.get(
                    SampleBatch.PREV_REWARDS),
                explore=explore,
                is_training=self._input_dict["is_training"])
        # Distribution generation is customized, e.g., DQN, DDPG.
        else:
            if action_distribution_fn:

                # Try new action_distribution_fn signature, supporting
                # state_batches and seq_lens.
                in_dict = self._input_dict
                try:
                    dist_inputs, dist_class, self._state_out = \
                        action_distribution_fn(
                            self,
                            self.model,
                            input_dict=in_dict,
                            state_batches=self._state_inputs,
                            seq_lens=self._seq_lens,
                            explore=explore,
                            timestep=timestep,
                            is_training=in_dict["is_training"])
                # Trying the old way (to stay backward compatible).
                # TODO: Remove in future.
                except TypeError as e:
                    if "positional argument" in e.args[0] or \
                            "unexpected keyword argument" in e.args[0]:
                        dist_inputs, dist_class, self._state_out = \
                            action_distribution_fn(
                                self, self.model,
                                obs_batch=in_dict[SampleBatch.CUR_OBS],
                                state_batches=self._state_inputs,
                                seq_lens=self._seq_lens,
                                prev_action_batch=in_dict.get(
                                    SampleBatch.PREV_ACTIONS),
                                prev_reward_batch=in_dict.get(
                                    SampleBatch.PREV_REWARDS),
                                explore=explore,
                                is_training=in_dict["is_training"])
                    else:
                        raise e

            # Default distribution generation behavior:
            # Pass through model. E.g., PG, PPO.
            else:
                dist_inputs, self._state_out = self.model(
                    self._input_dict, self._state_inputs, self._seq_lens)

            action_dist = dist_class(dist_inputs, self.model)

            # Using exploration to get final action (e.g. via sampling).
            sampled_action, sampled_action_logp = \
                self.exploration.get_exploration_action(
                    action_distribution=action_dist,
                    timestep=timestep,
                    explore=explore)

        # Phase 1 init.
        sess = tf1.get_default_session() or tf1.Session()

        batch_divisibility_req = get_batch_divisibility_req(self) if \
            callable(get_batch_divisibility_req) else \
            (get_batch_divisibility_req or 1)

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
            prev_action_input=self._input_dict.get(SampleBatch.PREV_ACTIONS),
            prev_reward_input=self._input_dict.get(SampleBatch.PREV_REWARDS),
            seq_lens=self._seq_lens,
            max_seq_len=config["model"]["max_seq_len"],
            batch_divisibility_req=batch_divisibility_req,
            explore=explore,
            timestep=timestep)

        # Phase 2 init.
        if before_loss_init is not None:
            before_loss_init(self, obs_space, action_space, config)

        # Loss initialization and model/postprocessing test calls.
        if not existing_inputs:
            self._initialize_loss_from_dummy_batch(
                auto_remove_unneeded_view_reqs=True)

    @override(TFPolicy)
    @DeveloperAPI
    def copy(self,
             existing_inputs: List[Tuple[str, "tf1.placeholder"]]) -> TFPolicy:
        """Creates a copy of self using existing input placeholders."""

        # Note that there might be RNN state inputs at the end of the list
        if len(self._loss_input_dict) != len(existing_inputs):
            raise ValueError("Tensor list mismatch", self._loss_input_dict,
                             self._state_inputs, existing_inputs)
        for i, (k, v) in enumerate(self._loss_input_dict_no_rnn.items()):
            if v.shape.as_list() != existing_inputs[i].shape.as_list():
                raise ValueError("Tensor shape mismatch", i, k, v.shape,
                                 existing_inputs[i].shape)
        # By convention, the loss inputs are followed by state inputs and then
        # the seq len tensor
        rnn_inputs = []
        for i in range(len(self._state_inputs)):
            rnn_inputs.append(
                ("state_in_{}".format(i),
                 existing_inputs[len(self._loss_input_dict_no_rnn) + i]))
        if rnn_inputs:
            rnn_inputs.append(("seq_lens", existing_inputs[-1]))
        input_dict = OrderedDict(
            [("is_exploring", self._is_exploring), ("timestep",
                                                    self._timestep)] +
            [(k, existing_inputs[i])
             for i, k in enumerate(self._loss_input_dict_no_rnn.keys())] +
            rnn_inputs)
        instance = self.__class__(
            self.observation_space,
            self.action_space,
            self.config,
            existing_inputs=input_dict,
            existing_model=[
                self.model,
                ("target_q_model", getattr(self, "target_q_model", None)),
                ("target_model", getattr(self, "target_model", None)),
            ])

        instance._loss_input_dict = input_dict
        loss = instance._do_loss_init(input_dict)
        loss_inputs = [
            (k, existing_inputs[i])
            for i, k in enumerate(self._loss_input_dict_no_rnn.keys())
        ]

        TFPolicy._initialize_loss(instance, loss, loss_inputs)
        if instance._grad_stats_fn:
            instance._stats_fetches.update(
                instance._grad_stats_fn(instance, input_dict, instance._grads))
        return instance

    @override(Policy)
    @DeveloperAPI
    def get_initial_state(self) -> List[TensorType]:
        if self.model:
            return self.model.get_initial_state()
        else:
            return []

    def _get_input_dict_and_dummy_batch(self, view_requirements,
                                        existing_inputs):
        """Creates input_dict and dummy_batch for loss initialization.

        Used for managing the Policy's input placeholders and for loss
        initialization.
        Input_dict: Str -> tf.placeholders, dummy_batch: str -> np.arrays.

        Args:
            view_requirements (ViewReqs): The view requirements dict.
            existing_inputs (Dict[str, tf.placeholder]): A dict of already
                existing placeholders.

        Returns:
            Tuple[Dict[str, tf.placeholder], Dict[str, np.ndarray]]: The
                input_dict/dummy_batch tuple.
        """
        input_dict = {}
        for view_col, view_req in view_requirements.items():
            # Point state_in to the already existing self._state_inputs.
            mo = re.match("state_in_(\d+)", view_col)
            if mo is not None:
                input_dict[view_col] = self._state_inputs[int(mo.group(1))]
            # State-outs (no placeholders needed).
            elif view_col.startswith("state_out_"):
                continue
            # Skip action dist inputs placeholder (do later).
            elif view_col == SampleBatch.ACTION_DIST_INPUTS:
                continue
            elif view_col in existing_inputs:
                input_dict[view_col] = existing_inputs[view_col]
            # All others.
            else:
                time_axis = not isinstance(view_req.shift, int)
                if view_req.used_for_training:
                    # Create a +time-axis placeholder if the shift is not an
                    # int (range or list of ints).
                    input_dict[view_col] = get_placeholder(
                        space=view_req.space,
                        name=view_col,
                        time_axis=time_axis)
        dummy_batch = self._get_dummy_batch_from_view_requirements(
            batch_size=32)

        return input_dict, dummy_batch

    def _initialize_loss_from_dummy_batch(
            self, auto_remove_unneeded_view_reqs: bool = True,
            stats_fn=None) -> None:

        # Create the optimizer/exploration optimizer here. Some initialization
        # steps (e.g. exploration postprocessing) may need this.
        self._optimizer = self.optimizer()

        # Test calls depend on variable init, so initialize model first.
        self._sess.run(tf1.global_variables_initializer())

        logger.info("Testing `compute_actions` w/ dummy batch.")
        actions, state_outs, extra_fetches = \
            self.compute_actions_from_input_dict(
                self._dummy_batch, explore=False, timestep=0)
        for key, value in extra_fetches.items():
            self._dummy_batch[key] = np.zeros_like(value)
            self._input_dict[key] = get_placeholder(value=value, name=key)
            if key not in self.view_requirements:
                logger.info("Adding extra-action-fetch `{}` to "
                            "view-reqs.".format(key))
                self.view_requirements[key] = \
                    ViewRequirement(space=gym.spaces.Box(
                        -1.0, 1.0, shape=value.shape[1:],
                        dtype=value.dtype))
        dummy_batch = self._dummy_batch

        logger.info("Testing `postprocess_trajectory` w/ dummy batch.")
        self.exploration.postprocess_trajectory(self, dummy_batch, self._sess)
        _ = self.postprocess_trajectory(dummy_batch)
        # Add new columns automatically to (loss) input_dict.
        for key in dummy_batch.added_keys:
            if key not in self._input_dict:
                self._input_dict[key] = get_placeholder(
                    value=dummy_batch[key], name=key)
            if key not in self.view_requirements:
                self.view_requirements[key] = \
                    ViewRequirement(space=gym.spaces.Box(
                        -1.0, 1.0, shape=dummy_batch[key].shape[1:],
                        dtype=dummy_batch[key].dtype))

        train_batch = SampleBatch(
            dict(self._input_dict, **self._loss_input_dict))

        if self._state_inputs:
            train_batch["seq_lens"] = self._seq_lens

        if log_once("loss_init"):
            logger.debug(
                "Initializing loss function with dummy input:\n\n{}\n".format(
                    summarize(train_batch)))

        self._loss_input_dict.update({k: v for k, v in train_batch.items()})
        loss = self._do_loss_init(train_batch)

        all_accessed_keys = \
            train_batch.accessed_keys | dummy_batch.accessed_keys | \
            dummy_batch.added_keys | set(
                self.model.view_requirements.keys())

        TFPolicy._initialize_loss(self, loss, [(k, v)
                                               for k, v in train_batch.items()
                                               if k in all_accessed_keys])

        if "is_training" in self._loss_input_dict:
            del self._loss_input_dict["is_training"]

        # Call the grads stats fn.
        # TODO: (sven) rename to simply stats_fn to match eager and torch.
        if self._grad_stats_fn:
            self._stats_fetches.update(
                self._grad_stats_fn(self, train_batch, self._grads))

        # Add new columns automatically to view-reqs.
        if auto_remove_unneeded_view_reqs:
            # Add those needed for postprocessing and training.
            all_accessed_keys = train_batch.accessed_keys | \
                                dummy_batch.accessed_keys
            # Tag those only needed for post-processing (with some exceptions).
            for key in dummy_batch.accessed_keys:
                if key not in train_batch.accessed_keys and \
                        key not in self.model.view_requirements and \
                        key not in [
                            SampleBatch.EPS_ID, SampleBatch.AGENT_INDEX,
                            SampleBatch.UNROLL_ID, SampleBatch.DONES,
                            SampleBatch.REWARDS, SampleBatch.INFOS]:
                    if key in self.view_requirements:
                        self.view_requirements[key].used_for_training = False
                    if key in self._loss_input_dict:
                        del self._loss_input_dict[key]
            # Remove those not needed at all (leave those that are needed
            # by Sampler to properly execute sample collection).
            # Also always leave DONES, REWARDS, and INFOS, no matter what.
            for key in list(self.view_requirements.keys()):
                if key not in all_accessed_keys and key not in [
                    SampleBatch.EPS_ID, SampleBatch.AGENT_INDEX,
                    SampleBatch.UNROLL_ID, SampleBatch.DONES,
                    SampleBatch.REWARDS, SampleBatch.INFOS] and \
                        key not in self.model.view_requirements:
                    # If user deleted this key manually in postprocessing
                    # fn, warn about it and do not remove from
                    # view-requirements.
                    if key in dummy_batch.deleted_keys:
                        logger.warning(
                            "SampleBatch key '{}' was deleted manually in "
                            "postprocessing function! RLlib will "
                            "automatically remove non-used items from the "
                            "data stream. Remove the `del` from your "
                            "postprocessing function.".format(key))
                    else:
                        del self.view_requirements[key]
                    if key in self._loss_input_dict:
                        del self._loss_input_dict[key]
            # Add those data_cols (again) that are missing and have
            # dependencies by view_cols.
            for key in list(self.view_requirements.keys()):
                vr = self.view_requirements[key]
                if (vr.data_col is not None
                        and vr.data_col not in self.view_requirements):
                    used_for_training = \
                        vr.data_col in train_batch.accessed_keys
                    self.view_requirements[vr.data_col] = ViewRequirement(
                        space=vr.space, used_for_training=used_for_training)

        self._loss_input_dict_no_rnn = {
            k: v
            for k, v in self._loss_input_dict.items()
            if (v not in self._state_inputs and v != self._seq_lens)
        }

        # Initialize again after loss init.
        self._sess.run(tf1.global_variables_initializer())

    def _do_loss_init(self, train_batch: SampleBatch):
        loss = self._loss_fn(self, self.model, self.dist_class, train_batch)
        if self._stats_fn:
            self._stats_fetches.update(self._stats_fn(self, train_batch))
        # override the update ops to be those of the model
        self._update_ops = self.model.update_ops()
        return loss
