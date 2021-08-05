import errno
import gym
import logging
import math
import numpy as np
import os
from typing import Dict, List, Optional, Tuple, Union, TYPE_CHECKING

import ray
import ray.experimental.tf_utils
from ray.util.debug import log_once
from ray.rllib.policy.policy import Policy, LEARNER_STATS_KEY
from ray.rllib.policy.rnn_sequencing import pad_batch_to_sequences_of_same_size
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.utils.annotations import override, DeveloperAPI
from ray.rllib.utils.debug import summarize
from ray.rllib.utils.annotations import Deprecated
from ray.rllib.utils.framework import try_import_tf, get_variable
from ray.rllib.utils.schedules import PiecewiseSchedule
from ray.rllib.utils.spaces.space_utils import normalize_action
from ray.rllib.utils.tf_ops import get_gpu_devices
from ray.rllib.utils.tf_run_builder import TFRunBuilder
from ray.rllib.utils.typing import ModelGradients, TensorType, \
    TrainerConfigDict

if TYPE_CHECKING:
    from ray.rllib.evaluation import MultiAgentEpisode

tf1, tf, tfv = try_import_tf()
logger = logging.getLogger(__name__)


@DeveloperAPI
class TFPolicy(Policy):
    """An agent policy and loss implemented in TensorFlow.

    Do not sub-class this class directly (neither should you sub-class
    DynamicTFPolicy), but rather use
    rllib.policy.tf_policy_template.build_tf_policy
    to generate your custom tf (graph-mode or eager) Policy classes.

    Extending this class enables RLlib to perform TensorFlow specific
    optimizations on the policy, e.g., parallelization across gpus or
    fusing multiple graphs together in the multi-agent setting.

    Input tensors are typically shaped like [BATCH_SIZE, ...].

    Attributes:
        observation_space (gym.Space): observation space of the policy.
        action_space (gym.Space): action space of the policy.
        model (rllib.models.Model): RLlib model used for the policy.

    Examples:
        >>> policy = TFPolicySubclass(
            sess, obs_input, sampled_action, loss, loss_inputs)

        >>> print(policy.compute_actions([1, 0, 2]))
        (array([0, 1, 1]), [], {})

        >>> print(policy.postprocess_trajectory(SampleBatch({...})))
        SampleBatch({"action": ..., "advantages": ..., ...})
    """

    @DeveloperAPI
    def __init__(self,
                 observation_space: gym.spaces.Space,
                 action_space: gym.spaces.Space,
                 config: TrainerConfigDict,
                 sess: "tf1.Session",
                 obs_input: TensorType,
                 sampled_action: TensorType,
                 loss: TensorType,
                 loss_inputs: List[Tuple[str, TensorType]],
                 model: ModelV2 = None,
                 sampled_action_logp: Optional[TensorType] = None,
                 action_input: Optional[TensorType] = None,
                 log_likelihood: Optional[TensorType] = None,
                 dist_inputs: Optional[TensorType] = None,
                 dist_class: Optional[type] = None,
                 state_inputs: Optional[List[TensorType]] = None,
                 state_outputs: Optional[List[TensorType]] = None,
                 prev_action_input: Optional[TensorType] = None,
                 prev_reward_input: Optional[TensorType] = None,
                 seq_lens: Optional[TensorType] = None,
                 max_seq_len: int = 20,
                 batch_divisibility_req: int = 1,
                 update_ops: List[TensorType] = None,
                 explore: Optional[TensorType] = None,
                 timestep: Optional[TensorType] = None):
        """Initializes a Policy object.

        Args:
            observation_space (gym.spaces.Space): Observation space of the env.
            action_space (gym.spaces.Space): Action space of the env.
            config (TrainerConfigDict): The Policy config dict.
            sess (tf1.Session): The TensorFlow session to use.
            obs_input (TensorType): Input placeholder for observations, of
                shape [BATCH_SIZE, obs...].
            sampled_action (TensorType): Tensor for sampling an action, of
                shape [BATCH_SIZE, action...]
            loss (TensorType): Scalar policy loss output tensor.
            loss_inputs (List[Tuple[str, TensorType]]): A (name, placeholder)
                tuple for each loss input argument. Each placeholder name must
                correspond to a SampleBatch column key returned by
                postprocess_trajectory(), and has shape [BATCH_SIZE, data...].
                These keys will be read from postprocessed sample batches and
                fed into the specified placeholders during loss computation.
            model (ModelV2): used to integrate custom losses and
                stats from user-defined RLlib models.
            sampled_action_logp (Optional[TensorType]): log probability of the
                sampled action.
            action_input (Optional[TensorType]): Input placeholder for actions
                for logp/log-likelihood calculations.
            log_likelihood (Optional[TensorType]): Tensor to calculate the
                log_likelihood (given action_input and obs_input).
            dist_class (Optional[type]): An optional ActionDistribution class
                to use for generating a dist object from distribution inputs.
            dist_inputs (Optional[TensorType]): Tensor to calculate the
                distribution inputs/parameters.
            state_inputs (Optional[List[TensorType]]): List of RNN state input
                Tensors.
            state_outputs (Optional[List[TensorType]]): List of RNN state
                output Tensors.
            prev_action_input (Optional[TensorType]): placeholder for previous
                actions.
            prev_reward_input (Optional[TensorType]): placeholder for previous
                rewards.
            seq_lens (Optional[TensorType]): Placeholder for RNN sequence
                lengths, of shape [NUM_SEQUENCES].
                Note that NUM_SEQUENCES << BATCH_SIZE. See
                policy/rnn_sequencing.py for more information.
            max_seq_len (int): Max sequence length for LSTM training.
            batch_divisibility_req (int): pad all agent experiences batches to
                multiples of this value. This only has an effect if not using
                a LSTM model.
            update_ops (List[TensorType]): override the batchnorm update ops
                to run when applying gradients. Otherwise we run all update
                ops found in the current variable scope.
            explore (Optional[Union[TensorType, bool]]): Placeholder for
                `explore` parameter into call to
                Exploration.get_exploration_action. Explicitly set this to
                False for not creating any Exploration component.
            timestep (Optional[TensorType]): Placeholder for the global
                sampling timestep.
        """
        self.framework = "tf"
        super().__init__(observation_space, action_space, config)

        # Get devices to build the graph on.
        worker_idx = self.config.get("worker_index", 0)
        if not config["_fake_gpus"] and \
                ray.worker._mode() == ray.worker.LOCAL_MODE:
            num_gpus = 0
        elif worker_idx == 0:
            num_gpus = config["num_gpus"]
        else:
            num_gpus = config["num_gpus_per_worker"]
        gpu_ids = get_gpu_devices()

        # Place on one or more CPU(s) when either:
        # - Fake GPU mode.
        # - num_gpus=0 (either set by user or we are in local_mode=True).
        # - no GPUs available.
        if config["_fake_gpus"] or num_gpus == 0 or not gpu_ids:
            logger.info("TFPolicy (worker={}) running on {}.".format(
                worker_idx
                if worker_idx > 0 else "local", f"{num_gpus} fake-GPUs"
                if config["_fake_gpus"] else "CPU"))
            self.devices = [
                "/cpu:0" for _ in range(int(math.ceil(num_gpus)) or 1)
            ]
        # Place on one or more actual GPU(s), when:
        # - num_gpus > 0 (set by user) AND
        # - local_mode=False AND
        # - actual GPUs available AND
        # - non-fake GPU mode.
        else:
            logger.info("TFPolicy (worker={}) running on {} GPU(s).".format(
                worker_idx if worker_idx > 0 else "local", num_gpus))

            # We are a remote worker (WORKER_MODE=1):
            # GPUs should be assigned to us by ray.
            if ray.worker._mode() == ray.worker.WORKER_MODE:
                gpu_ids = ray.get_gpu_ids()

            if len(gpu_ids) < num_gpus:
                raise ValueError(
                    "TFPolicy was not able to find enough GPU IDs! Found "
                    f"{gpu_ids}, but num_gpus={num_gpus}.")

            self.devices = [
                f"/gpu:{i}" for i, _ in enumerate(gpu_ids) if i < num_gpus
            ]

        # Disable env-info placeholder.
        if SampleBatch.INFOS in self.view_requirements:
            self.view_requirements[SampleBatch.INFOS].used_for_training = False
            self.view_requirements[
                SampleBatch.INFOS].used_for_compute_actions = False

        assert model is None or isinstance(model, (ModelV2, tf.keras.Model)), \
            "Model classes for TFPolicy other than `ModelV2|tf.keras.Model` " \
            "not allowed! You passed in {}.".format(model)
        self.model = model
        # Auto-update model's inference view requirements, if recurrent.
        if self.model is not None:
            self._update_model_view_requirements_from_init_state()

        # If `explore` is explicitly set to False, don't create an exploration
        # component.
        self.exploration = self._create_exploration() if explore is not False \
            else None

        self._sess = sess
        self._obs_input = obs_input
        self._prev_action_input = prev_action_input
        self._prev_reward_input = prev_reward_input
        self._sampled_action = sampled_action
        self._is_training = self._get_is_training_placeholder()
        self._is_exploring = explore if explore is not None else \
            tf1.placeholder_with_default(True, (), name="is_exploring")
        self._sampled_action_logp = sampled_action_logp
        self._sampled_action_prob = (tf.math.exp(self._sampled_action_logp)
                                     if self._sampled_action_logp is not None
                                     else None)
        self._action_input = action_input  # For logp calculations.
        self._dist_inputs = dist_inputs
        self.dist_class = dist_class

        self._state_inputs = state_inputs or []
        self._state_outputs = state_outputs or []
        self._seq_lens = seq_lens
        self._max_seq_len = max_seq_len

        if self._state_inputs and self._seq_lens is None:
            raise ValueError(
                "seq_lens tensor must be given if state inputs are defined")

        self._batch_divisibility_req = batch_divisibility_req
        self._update_ops = update_ops
        self._apply_op = None
        self._stats_fetches = {}
        self._timestep = timestep if timestep is not None else \
            tf1.placeholder_with_default(
                tf.zeros((), dtype=tf.int64), (), name="timestep")

        self._optimizer = None
        self._grads_and_vars = None
        self._grads = None
        # Policy tf-variables (weights), whose values to get/set via
        # get_weights/set_weights.
        self._variables = None
        # Local optimizer's tf-variables (e.g. state vars for Adam).
        # Will be stored alongside `self._variables` when checkpointing.
        self._optimizer_variables = None

        # The loss tf-op.
        self._loss = None
        # A batch dict passed into loss function as input.
        self._loss_input_dict = {}
        if loss is not None:
            self._initialize_loss(loss, loss_inputs)

        # The log-likelihood calculator op.
        self._log_likelihood = log_likelihood
        if self._log_likelihood is None and self._dist_inputs is not None and \
                self.dist_class is not None:
            self._log_likelihood = self.dist_class(
                self._dist_inputs, self.model).logp(self._action_input)

    def variables(self):
        """Return the list of all savable variables for this policy."""
        if isinstance(self.model, tf.keras.Model):
            return self.model.variables
        else:
            return self.model.variables()

    def get_placeholder(self, name) -> "tf1.placeholder":
        """Returns the given action or loss input placeholder by name.

        If the loss has not been initialized and a loss input placeholder is
        requested, an error is raised.

        Args:
            name (str): The name of the placeholder to return. One of
                SampleBatch.CUR_OBS|PREV_ACTION/REWARD or a valid key from
                `self._loss_input_dict`.

        Returns:
            tf1.placeholder: The placeholder under the given str key.
        """
        if name == SampleBatch.CUR_OBS:
            return self._obs_input
        elif name == SampleBatch.PREV_ACTIONS:
            return self._prev_action_input
        elif name == SampleBatch.PREV_REWARDS:
            return self._prev_reward_input

        assert self._loss_input_dict, \
            "You need to populate `self._loss_input_dict` before " \
            "`get_placeholder()` can be called"
        return self._loss_input_dict[name]

    @override(Policy)
    def get_session(self) -> Optional["tf1.Session"]:
        """Returns a reference to the TF session for this policy."""
        return self._sess

    def loss_initialized(self) -> bool:
        """Returns whether the loss function has been initialized."""
        return self._loss is not None

    def _initialize_loss(self, loss: TensorType,
                         loss_inputs: List[Tuple[str, TensorType]]) -> None:
        """Initializes the loss op from given loss tensor and placeholders.

        Args:
            loss (TensorType): The loss op generated by some loss function.
            loss_inputs (List[Tuple[str, TensorType]]): The list of Tuples:
                (name, tf1.placeholders) needed for calculating the loss.
        """
        self._loss_input_dict = dict(loss_inputs)
        self._loss_input_dict_no_rnn = {
            k: v
            for k, v in self._loss_input_dict.items()
            if (v not in self._state_inputs and v != self._seq_lens)
        }
        for i, ph in enumerate(self._state_inputs):
            self._loss_input_dict["state_in_{}".format(i)] = ph

        if self.model and not isinstance(self.model, tf.keras.Model):
            self._loss = self.model.custom_loss(loss, self._loss_input_dict)
            self._stats_fetches.update({"model": self.model.metrics()})
        else:
            self._loss = loss

        if self._optimizer is None:
            self._optimizer = self.optimizer()
        self._grads_and_vars = [
            (g, v) for (g, v) in self.gradients(self._optimizer, self._loss)
            if g is not None
        ]
        self._grads = [g for (g, v) in self._grads_and_vars]

        if self.model:
            self._variables = ray.experimental.tf_utils.TensorFlowVariables(
                [], self.get_session(), self.variables())

        # gather update ops for any batch norm layers
        if not self._update_ops:
            self._update_ops = tf1.get_collection(
                tf1.GraphKeys.UPDATE_OPS, scope=tf1.get_variable_scope().name)
        if self._update_ops:
            logger.info("Update ops to run on apply gradient: {}".format(
                self._update_ops))
        with tf1.control_dependencies(self._update_ops):
            self._apply_op = self.build_apply_op(self._optimizer,
                                                 self._grads_and_vars)

        if log_once("loss_used"):
            logger.debug(
                "These tensors were used in the loss_fn:\n\n{}\n".format(
                    summarize(self._loss_input_dict)))

        self.get_session().run(tf1.global_variables_initializer())
        self._optimizer_variables = None
        if self._optimizer:
            self._optimizer_variables = \
                ray.experimental.tf_utils.TensorFlowVariables(
                    self._optimizer.variables(), self.get_session())

    @override(Policy)
    def compute_actions(
            self,
            obs_batch: Union[List[TensorType], TensorType],
            state_batches: Optional[List[TensorType]] = None,
            prev_action_batch: Union[List[TensorType], TensorType] = None,
            prev_reward_batch: Union[List[TensorType], TensorType] = None,
            info_batch: Optional[Dict[str, list]] = None,
            episodes: Optional[List["MultiAgentEpisode"]] = None,
            explore: Optional[bool] = None,
            timestep: Optional[int] = None,
            **kwargs):

        explore = explore if explore is not None else self.config["explore"]
        timestep = timestep if timestep is not None else self.global_timestep

        builder = TFRunBuilder(self.get_session(), "compute_actions")
        to_fetch = self._build_compute_actions(
            builder,
            obs_batch=obs_batch,
            state_batches=state_batches,
            prev_action_batch=prev_action_batch,
            prev_reward_batch=prev_reward_batch,
            explore=explore,
            timestep=timestep)

        # Execute session run to get action (and other fetches).
        fetched = builder.get(to_fetch)

        # Update our global timestep by the batch size.
        self.global_timestep += len(obs_batch) if isinstance(obs_batch, list) \
            else obs_batch.shape[0]

        return fetched

    @override(Policy)
    def compute_actions_from_input_dict(
            self,
            input_dict: Dict[str, TensorType],
            explore: bool = None,
            timestep: Optional[int] = None,
            episodes: Optional[List["MultiAgentEpisode"]] = None,
            **kwargs) -> \
            Tuple[TensorType, List[TensorType], Dict[str, TensorType]]:

        explore = explore if explore is not None else self.config["explore"]
        timestep = timestep if timestep is not None else self.global_timestep

        builder = TFRunBuilder(self.get_session(),
                               "compute_actions_from_input_dict")
        obs_batch = input_dict[SampleBatch.OBS]
        to_fetch = self._build_compute_actions(
            builder, input_dict=input_dict, explore=explore, timestep=timestep)

        # Execute session run to get action (and other fetches).
        fetched = builder.get(to_fetch)

        # Update our global timestep by the batch size.
        self.global_timestep += len(obs_batch) if isinstance(obs_batch, list) \
            else obs_batch.shape[0]

        return fetched

    @override(Policy)
    def compute_log_likelihoods(
            self,
            actions: Union[List[TensorType], TensorType],
            obs_batch: Union[List[TensorType], TensorType],
            state_batches: Optional[List[TensorType]] = None,
            prev_action_batch: Optional[Union[List[TensorType],
                                              TensorType]] = None,
            prev_reward_batch: Optional[Union[List[TensorType],
                                              TensorType]] = None,
            actions_normalized: bool = True,
    ) -> TensorType:

        if self._log_likelihood is None:
            raise ValueError("Cannot compute log-prob/likelihood w/o a "
                             "self._log_likelihood op!")

        # Exploration hook before each forward pass.
        self.exploration.before_compute_actions(
            explore=False, tf_sess=self.get_session())

        builder = TFRunBuilder(self.get_session(), "compute_log_likelihoods")

        # Normalize actions if necessary.
        if actions_normalized is False and self.config["normalize_actions"]:
            actions = normalize_action(actions, self.action_space_struct)

        # Feed actions (for which we want logp values) into graph.
        builder.add_feed_dict({self._action_input: actions})
        # Feed observations.
        builder.add_feed_dict({self._obs_input: obs_batch})
        # Internal states.
        state_batches = state_batches or []
        if len(self._state_inputs) != len(state_batches):
            raise ValueError(
                "Must pass in RNN state batches for placeholders {}, got {}".
                format(self._state_inputs, state_batches))
        builder.add_feed_dict(
            {k: v
             for k, v in zip(self._state_inputs, state_batches)})
        if state_batches:
            builder.add_feed_dict({self._seq_lens: np.ones(len(obs_batch))})
        # Prev-a and r.
        if self._prev_action_input is not None and \
           prev_action_batch is not None:
            builder.add_feed_dict({self._prev_action_input: prev_action_batch})
        if self._prev_reward_input is not None and \
           prev_reward_batch is not None:
            builder.add_feed_dict({self._prev_reward_input: prev_reward_batch})
        # Fetch the log_likelihoods output and return.
        fetches = builder.add_fetches([self._log_likelihood])
        return builder.get(fetches)[0]

    @override(Policy)
    @DeveloperAPI
    def learn_on_batch(
            self, postprocessed_batch: SampleBatch) -> Dict[str, TensorType]:
        assert self.loss_initialized()

        builder = TFRunBuilder(self.get_session(), "learn_on_batch")

        # Callback handling.
        learn_stats = {}
        self.callbacks.on_learn_on_batch(
            policy=self, train_batch=postprocessed_batch, result=learn_stats)

        fetches = self._build_learn_on_batch(builder, postprocessed_batch)
        stats = builder.get(fetches)
        stats.update({"custom_metrics": learn_stats})
        return stats

    @override(Policy)
    @DeveloperAPI
    def compute_gradients(
            self,
            postprocessed_batch: SampleBatch) -> \
            Tuple[ModelGradients, Dict[str, TensorType]]:
        assert self.loss_initialized()
        builder = TFRunBuilder(self.get_session(), "compute_gradients")
        fetches = self._build_compute_gradients(builder, postprocessed_batch)
        return builder.get(fetches)

    @override(Policy)
    @DeveloperAPI
    def apply_gradients(self, gradients: ModelGradients) -> None:
        assert self.loss_initialized()
        builder = TFRunBuilder(self.get_session(), "apply_gradients")
        fetches = self._build_apply_gradients(builder, gradients)
        builder.get(fetches)

    @override(Policy)
    @DeveloperAPI
    def get_exploration_state(self) -> Dict[str, TensorType]:
        return self.exploration.get_state(sess=self.get_session())

    @Deprecated(new="get_exploration_state", error=False)
    def get_exploration_info(self) -> Dict[str, TensorType]:
        return self.get_exploration_state()

    @override(Policy)
    @DeveloperAPI
    def get_weights(self) -> Union[Dict[str, TensorType], List[TensorType]]:
        return self._variables.get_weights()

    @override(Policy)
    @DeveloperAPI
    def set_weights(self, weights) -> None:
        return self._variables.set_weights(weights)

    @override(Policy)
    @DeveloperAPI
    def get_state(self) -> Union[Dict[str, TensorType], List[TensorType]]:
        # For tf Policies, return Policy weights and optimizer var values.
        state = super().get_state()
        if self._optimizer_variables and \
                len(self._optimizer_variables.variables) > 0:
            state["_optimizer_variables"] = \
                self.get_session().run(self._optimizer_variables.variables)
        # Add exploration state.
        state["_exploration_state"] = \
            self.exploration.get_state(self.get_session())
        return state

    @override(Policy)
    @DeveloperAPI
    def set_state(self, state: dict) -> None:
        # Set optimizer vars first.
        optimizer_vars = state.get("_optimizer_variables", None)
        if optimizer_vars:
            self._optimizer_variables.set_weights(optimizer_vars)
        # Set exploration's state.
        if hasattr(self, "exploration") and "_exploration_state" in state:
            self.exploration.set_state(
                state=state["_exploration_state"], sess=self.get_session())

        # Set the Policy's (NN) weights.
        super().set_state(state)

    @override(Policy)
    @DeveloperAPI
    def export_model(self, export_dir: str,
                     onnx: Optional[int] = None) -> None:
        """Export tensorflow graph to export_dir for serving."""

        if onnx:
            try:
                import tf2onnx
            except ImportError as e:
                raise RuntimeError(
                    "Converting a TensorFlow model to ONNX requires "
                    "`tf2onnx` to be installed. Install with "
                    "`pip install tf2onnx`.") from e

            with self.get_session().graph.as_default():
                signature_def_map = self._build_signature_def()

                sd = signature_def_map[tf1.saved_model.signature_constants.
                                       DEFAULT_SERVING_SIGNATURE_DEF_KEY]
                inputs = [v.name for k, v in sd.inputs.items()]
                outputs = [v.name for k, v in sd.outputs.items()]

                from tf2onnx import tf_loader
                frozen_graph_def = tf_loader.freeze_session(
                    self._sess, input_names=inputs, output_names=outputs)

            with tf1.Session(graph=tf.Graph()) as session:
                tf.import_graph_def(frozen_graph_def, name="")

                g = tf2onnx.tfonnx.process_tf_graph(
                    session.graph,
                    input_names=inputs,
                    output_names=outputs,
                    inputs_as_nchw=inputs)

                model_proto = g.make_model("onnx_model")
                tf2onnx.utils.save_onnx_model(
                    export_dir,
                    "saved_model",
                    feed_dict={},
                    model_proto=model_proto)
        else:
            with self.get_session().graph.as_default():
                signature_def_map = self._build_signature_def()
                builder = tf1.saved_model.builder.SavedModelBuilder(export_dir)
                builder.add_meta_graph_and_variables(
                    self.get_session(),
                    [tf1.saved_model.tag_constants.SERVING],
                    signature_def_map=signature_def_map,
                    saver=tf1.summary.FileWriter(export_dir).add_graph(
                        graph=self.get_session().graph))
                builder.save()

    @override(Policy)
    @DeveloperAPI
    def export_checkpoint(self,
                          export_dir: str,
                          filename_prefix: str = "model") -> None:
        """Export tensorflow checkpoint to export_dir."""
        try:
            os.makedirs(export_dir)
        except OSError as e:
            # ignore error if export dir already exists
            if e.errno != errno.EEXIST:
                raise
        save_path = os.path.join(export_dir, filename_prefix)
        with self.get_session().graph.as_default():
            saver = tf1.train.Saver()
            saver.save(self.get_session(), save_path)

    @override(Policy)
    @DeveloperAPI
    def import_model_from_h5(self, import_file: str) -> None:
        """Imports weights into tf model."""
        # Make sure the session is the right one (see issue #7046).
        with self.get_session().graph.as_default():
            with self.get_session().as_default():
                return self.model.import_from_h5(import_file)

    @DeveloperAPI
    def copy(self,
             existing_inputs: List[Tuple[str, "tf1.placeholder"]]) -> \
            "TFPolicy":
        """Creates a copy of self using existing input placeholders.

        Optional: Only required to work with the multi-GPU optimizer.

        Args:
            existing_inputs (List[Tuple[str, tf1.placeholder]]): Dict mapping
                names (str) to tf1.placeholders to re-use (share) with the
                returned copy of self.

        Returns:
            TFPolicy: A copy of self.
        """
        raise NotImplementedError

    @override(Policy)
    @DeveloperAPI
    def is_recurrent(self) -> bool:
        return len(self._state_inputs) > 0

    @override(Policy)
    @DeveloperAPI
    def num_state_tensors(self) -> int:
        return len(self._state_inputs)

    @DeveloperAPI
    def extra_compute_action_feed_dict(self) -> Dict[TensorType, TensorType]:
        """Extra dict to pass to the compute actions session run.

        Returns:
            Dict[TensorType, TensorType]: A feed dict to be added to the
                feed_dict passed to the compute_actions session.run() call.
        """
        return {}

    @DeveloperAPI
    def extra_compute_action_fetches(self) -> Dict[str, TensorType]:
        """Extra values to fetch and return from compute_actions().

        By default we return action probability/log-likelihood info
        and action distribution inputs (if present).

        Returns:
             Dict[str, TensorType]: An extra fetch-dict to be passed to and
                returned from the compute_actions() call.
        """
        extra_fetches = {}
        # Action-logp and action-prob.
        if self._sampled_action_logp is not None:
            extra_fetches[SampleBatch.ACTION_PROB] = self._sampled_action_prob
            extra_fetches[SampleBatch.ACTION_LOGP] = self._sampled_action_logp
        # Action-dist inputs.
        if self._dist_inputs is not None:
            extra_fetches[SampleBatch.ACTION_DIST_INPUTS] = self._dist_inputs
        return extra_fetches

    @DeveloperAPI
    def extra_compute_grad_feed_dict(self) -> Dict[TensorType, TensorType]:
        """Extra dict to pass to the compute gradients session run.

        Returns:
            Dict[TensorType, TensorType]: Extra feed_dict to be passed to the
                compute_gradients Session.run() call.
        """
        return {}  # e.g, kl_coeff

    @DeveloperAPI
    def extra_compute_grad_fetches(self) -> Dict[str, any]:
        """Extra values to fetch and return from compute_gradients().

        Returns:
            Dict[str, any]: Extra fetch dict to be added to the fetch dict
                of the compute_gradients Session.run() call.
        """
        return {LEARNER_STATS_KEY: {}}  # e.g, stats, td error, etc.

    @DeveloperAPI
    def optimizer(self) -> "tf.keras.optimizers.Optimizer":
        """TF optimizer to use for policy optimization.

        Returns:
            tf.keras.optimizers.Optimizer: The local optimizer to use for this
                Policy's Model.
        """
        if hasattr(self, "config"):
            return tf1.train.AdamOptimizer(learning_rate=self.config["lr"])
        else:
            return tf1.train.AdamOptimizer()

    @DeveloperAPI
    def gradients(self, optimizer: "tf.keras.optimizers.Optimizer",
                  loss: TensorType) -> List[Tuple[TensorType, TensorType]]:
        """Override this for a custom gradient computation behavior.

        Returns:
            List[Tuple[TensorType, TensorType]]: List of tuples with grad
                values and the grad-value's corresponding tf.variable in it.
        """
        return optimizer.compute_gradients(loss)

    @DeveloperAPI
    def build_apply_op(
            self,
            optimizer: "tf.keras.optimizers.Optimizer",
            grads_and_vars: List[Tuple[TensorType, TensorType]]) -> \
            "tf.Operation":
        """Override this for a custom gradient apply computation behavior.

        Args:
            optimizer (tf.keras.optimizers.Optimizer): The local tf optimizer
                to use for applying the grads and vars.
            grads_and_vars (List[Tuple[TensorType, TensorType]]): List of
                tuples with grad values and the grad-value's corresponding
                tf.variable in it.
        """
        # Specify global_step for TD3 which needs to count the num updates.
        return optimizer.apply_gradients(
            self._grads_and_vars,
            global_step=tf1.train.get_or_create_global_step())

    def _get_is_training_placeholder(self):
        """Get the placeholder for _is_training, i.e., for batch norm layers.

        This can be called safely before __init__ has run.
        """
        if not hasattr(self, "_is_training"):
            self._is_training = tf1.placeholder_with_default(
                False, (), name="is_training")
        return self._is_training

    def _debug_vars(self):
        if log_once("grad_vars"):
            for _, v in self._grads_and_vars:
                logger.info("Optimizing variable {}".format(v))

    def _extra_input_signature_def(self):
        """Extra input signatures to add when exporting tf model.
        Inferred from extra_compute_action_feed_dict()
        """
        feed_dict = self.extra_compute_action_feed_dict()
        return {
            k.name: tf1.saved_model.utils.build_tensor_info(k)
            for k in feed_dict.keys()
        }

    def _extra_output_signature_def(self):
        """Extra output signatures to add when exporting tf model.
        Inferred from extra_compute_action_fetches()
        """
        fetches = self.extra_compute_action_fetches()
        return {
            k: tf1.saved_model.utils.build_tensor_info(fetches[k])
            for k in fetches.keys()
        }

    def _build_signature_def(self):
        """Build signature def map for tensorflow SavedModelBuilder.
        """
        # build input signatures
        input_signature = self._extra_input_signature_def()
        input_signature["observations"] = \
            tf1.saved_model.utils.build_tensor_info(self._obs_input)

        if self._seq_lens is not None:
            input_signature["seq_lens"] = \
                tf1.saved_model.utils.build_tensor_info(self._seq_lens)
        if self._prev_action_input is not None:
            input_signature["prev_action"] = \
                tf1.saved_model.utils.build_tensor_info(
                    self._prev_action_input)
        if self._prev_reward_input is not None:
            input_signature["prev_reward"] = \
                tf1.saved_model.utils.build_tensor_info(
                    self._prev_reward_input)

        input_signature["is_training"] = \
            tf1.saved_model.utils.build_tensor_info(self._is_training)

        if self._timestep is not None:
            input_signature["timestep"] = \
                tf1.saved_model.utils.build_tensor_info(self._timestep)

        for state_input in self._state_inputs:
            input_signature[state_input.name] = \
                tf1.saved_model.utils.build_tensor_info(state_input)

        # build output signatures
        output_signature = self._extra_output_signature_def()
        for i, a in enumerate(tf.nest.flatten(self._sampled_action)):
            output_signature["actions_{}".format(i)] = \
                tf1.saved_model.utils.build_tensor_info(a)

        for state_output in self._state_outputs:
            output_signature[state_output.name] = \
                tf1.saved_model.utils.build_tensor_info(state_output)
        signature_def = (
            tf1.saved_model.signature_def_utils.build_signature_def(
                input_signature, output_signature,
                tf1.saved_model.signature_constants.PREDICT_METHOD_NAME))
        signature_def_key = (tf1.saved_model.signature_constants.
                             DEFAULT_SERVING_SIGNATURE_DEF_KEY)
        signature_def_map = {signature_def_key: signature_def}
        return signature_def_map

    def _build_compute_actions(self,
                               builder,
                               *,
                               input_dict=None,
                               obs_batch=None,
                               state_batches=None,
                               prev_action_batch=None,
                               prev_reward_batch=None,
                               episodes=None,
                               explore=None,
                               timestep=None):
        explore = explore if explore is not None else self.config["explore"]
        timestep = timestep if timestep is not None else self.global_timestep

        # Call the exploration before_compute_actions hook.
        self.exploration.before_compute_actions(
            timestep=timestep, explore=explore, tf_sess=self.get_session())

        builder.add_feed_dict(self.extra_compute_action_feed_dict())

        # `input_dict` given: Simply build what's in that dict.
        if input_dict is not None:
            if hasattr(self, "_input_dict"):
                for key, value in input_dict.items():
                    if key in self._input_dict:
                        builder.add_feed_dict({self._input_dict[key]: value})
            # For policies that inherit directly from TFPolicy.
            else:
                builder.add_feed_dict({
                    self._obs_input: input_dict[SampleBatch.OBS]
                })
                if SampleBatch.PREV_ACTIONS in input_dict:
                    builder.add_feed_dict({
                        self._prev_action_input: input_dict[
                            SampleBatch.PREV_ACTIONS]
                    })
                if SampleBatch.PREV_REWARDS in input_dict:
                    builder.add_feed_dict({
                        self._prev_reward_input: input_dict[
                            SampleBatch.PREV_REWARDS]
                    })
                state_batches = []
                i = 0
                while "state_in_{}".format(i) in input_dict:
                    state_batches.append(input_dict["state_in_{}".format(i)])
                    i += 1
                builder.add_feed_dict(
                    dict(zip(self._state_inputs, state_batches)))

            if "state_in_0" in input_dict:
                builder.add_feed_dict({
                    self._seq_lens: np.ones(len(input_dict["state_in_0"]))
                })

        # Hardcoded old way: Build fixed fields, if provided.
        # TODO: (sven) This can be deprecated after trajectory view API flag is
        #  removed and always True.
        else:
            state_batches = state_batches or []
            if len(self._state_inputs) != len(state_batches):
                raise ValueError(
                    "Must pass in RNN state batches for placeholders {}, "
                    "got {}".format(self._state_inputs, state_batches))

            builder.add_feed_dict({self._obs_input: obs_batch})
            if state_batches:
                builder.add_feed_dict({
                    self._seq_lens: np.ones(len(obs_batch))
                })
            if self._prev_action_input is not None and \
               prev_action_batch is not None:
                builder.add_feed_dict({
                    self._prev_action_input: prev_action_batch
                })
            if self._prev_reward_input is not None and \
               prev_reward_batch is not None:
                builder.add_feed_dict({
                    self._prev_reward_input: prev_reward_batch
                })
            builder.add_feed_dict(dict(zip(self._state_inputs, state_batches)))

        builder.add_feed_dict({self._is_training: False})
        builder.add_feed_dict({self._is_exploring: explore})
        if timestep is not None:
            builder.add_feed_dict({self._timestep: timestep})

        # Determine, what exactly to fetch from the graph.
        to_fetch = [self._sampled_action] + self._state_outputs + \
                   [self.extra_compute_action_fetches()]

        # Perform the session call.
        fetches = builder.add_fetches(to_fetch)
        return fetches[0], fetches[1:-1], fetches[-1]

    def _build_compute_gradients(self, builder, postprocessed_batch):
        self._debug_vars()
        builder.add_feed_dict(self.extra_compute_grad_feed_dict())
        builder.add_feed_dict(
            self._get_loss_inputs_dict(postprocessed_batch, shuffle=False))
        fetches = builder.add_fetches(
            [self._grads, self._get_grad_and_stats_fetches()])
        return fetches[0], fetches[1]

    def _build_apply_gradients(self, builder, gradients):
        if len(gradients) != len(self._grads):
            raise ValueError(
                "Unexpected number of gradients to apply, got {} for {}".
                format(gradients, self._grads))
        builder.add_feed_dict({self._is_training: True})
        builder.add_feed_dict(dict(zip(self._grads, gradients)))
        fetches = builder.add_fetches([self._apply_op])
        return fetches[0]

    def _build_learn_on_batch(self, builder, postprocessed_batch):
        self._debug_vars()

        builder.add_feed_dict(self.extra_compute_grad_feed_dict())
        builder.add_feed_dict(
            self._get_loss_inputs_dict(postprocessed_batch, shuffle=False))
        fetches = builder.add_fetches([
            self._apply_op,
            self._get_grad_and_stats_fetches(),
        ])
        return fetches[1]

    def _get_grad_and_stats_fetches(self):
        fetches = self.extra_compute_grad_fetches()
        if LEARNER_STATS_KEY not in fetches:
            raise ValueError(
                "Grad fetches should contain 'stats': {...} entry")
        if self._stats_fetches:
            fetches[LEARNER_STATS_KEY] = dict(self._stats_fetches,
                                              **fetches[LEARNER_STATS_KEY])
        return fetches

    def _get_loss_inputs_dict(self, train_batch: SampleBatch, shuffle: bool):
        """Return a feed dict from a batch.

        Args:
            train_batch (SampleBatch): batch of data to derive inputs from.
            shuffle (bool): whether to shuffle batch sequences. Shuffle may
                be done in-place. This only makes sense if you're further
                applying minibatch SGD after getting the outputs.

        Returns:
            Feed dict of data.
        """

        # Get batch ready for RNNs, if applicable.
        if not isinstance(train_batch,
                          SampleBatch) or not train_batch.zero_padded:
            pad_batch_to_sequences_of_same_size(
                train_batch,
                max_seq_len=self._max_seq_len,
                shuffle=shuffle,
                batch_divisibility_req=self._batch_divisibility_req,
                feature_keys=list(self._loss_input_dict_no_rnn.keys()),
                view_requirements=self.view_requirements,
            )

        # Mark the batch as "is_training" so the Model can use this
        # information.
        train_batch.is_training = True

        # Build the feed dict from the batch.
        feed_dict = {}
        for key, placeholder in self._loss_input_dict.items():
            feed_dict[placeholder] = train_batch[key]

        state_keys = [
            "state_in_{}".format(i) for i in range(len(self._state_inputs))
        ]
        for key in state_keys:
            feed_dict[self._loss_input_dict[key]] = train_batch[key]
        if state_keys:
            feed_dict[self._seq_lens] = train_batch["seq_lens"]

        return feed_dict


@DeveloperAPI
class LearningRateSchedule:
    """Mixin for TFPolicy that adds a learning rate schedule."""

    @DeveloperAPI
    def __init__(self, lr, lr_schedule):
        self._lr_schedule = None
        if lr_schedule is None:
            self.cur_lr = tf1.get_variable(
                "lr", initializer=lr, trainable=False)
        else:
            self._lr_schedule = PiecewiseSchedule(
                lr_schedule, outside_value=lr_schedule[-1][-1], framework=None)
            self.cur_lr = tf1.get_variable(
                "lr", initializer=self._lr_schedule.value(0), trainable=False)
            if self.framework == "tf":
                self._lr_placeholder = tf1.placeholder(
                    dtype=tf.float32, name="lr")
                self._lr_update = self.cur_lr.assign(
                    self._lr_placeholder, read_value=False)

    @override(Policy)
    def on_global_var_update(self, global_vars):
        super(LearningRateSchedule, self).on_global_var_update(global_vars)
        if self._lr_schedule is not None:
            new_val = self._lr_schedule.value(global_vars["timestep"])
            if self.framework == "tf":
                self.get_session().run(
                    self._lr_update, feed_dict={self._lr_placeholder: new_val})
            else:
                self.cur_lr.assign(new_val, read_value=False)
                self._optimizer.learning_rate.assign(self.cur_lr)

    @override(TFPolicy)
    def optimizer(self):
        return tf1.train.AdamOptimizer(learning_rate=self.cur_lr)


@DeveloperAPI
class EntropyCoeffSchedule:
    """Mixin for TFPolicy that adds entropy coeff decay."""

    @DeveloperAPI
    def __init__(self, entropy_coeff, entropy_coeff_schedule):
        self._entropy_coeff_schedule = None
        if entropy_coeff_schedule is None:
            self.entropy_coeff = get_variable(
                entropy_coeff,
                framework="tf",
                tf_name="entropy_coeff",
                trainable=False)
        else:
            # Allows for custom schedule similar to lr_schedule format
            if isinstance(entropy_coeff_schedule, list):
                self._entropy_coeff_schedule = PiecewiseSchedule(
                    entropy_coeff_schedule,
                    outside_value=entropy_coeff_schedule[-1][-1],
                    framework=None)
            else:
                # Implements previous version but enforces outside_value
                self._entropy_coeff_schedule = PiecewiseSchedule(
                    [[0, entropy_coeff], [entropy_coeff_schedule, 0.0]],
                    outside_value=0.0,
                    framework=None)

            self.entropy_coeff = get_variable(
                self._entropy_coeff_schedule.value(0),
                framework="tf",
                tf_name="entropy_coeff",
                trainable=False)
            if self.framework == "tf":
                self._entropy_coeff_placeholder = tf1.placeholder(
                    dtype=tf.float32, name="entropy_coeff")
                self._entropy_coeff_update = self.entropy_coeff.assign(
                    self._entropy_coeff_placeholder, read_value=False)

    @override(Policy)
    def on_global_var_update(self, global_vars):
        super(EntropyCoeffSchedule, self).on_global_var_update(global_vars)
        if self._entropy_coeff_schedule is not None:
            new_val = self._entropy_coeff_schedule.value(
                global_vars["timestep"])
            if self.framework == "tf":
                self.get_session().run(
                    self._entropy_coeff_update,
                    feed_dict={self._entropy_coeff_placeholder: new_val})
            else:
                self.entropy_coeff.assign(new_val, read_value=False)
