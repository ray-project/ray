import logging

import ray
import tensorflow as tf

from ray.rllib import models
from ray.rllib.evaluation import TFPolicyGraph

from ray.rllib.agents.sac.sac.dev_utils import using_ray_8
from ray.rllib.agents.sac.sac.rllib_proxy._todo import (
    log_once,
    summarize,
    TensorFlowVariables,
    DeveloperAPI,
    RLLIB_MODEL,
    _global_registry
)
from ray.rllib.agents.sac.sac.rllib_proxy._utils import executing_eagerly
from ray.rllib.agents.sac.sac.rllib_proxy._tf_model_v2 import ModelV2
from ray.rllib.agents.sac.sac.rllib_proxy._modelv1_compat import make_v1_wrapper

logger = logging.getLogger(__name__)

__all__ = ["ModelCatalog", "TFPolicy"]


class TFPolicy(TFPolicyGraph):

    @DeveloperAPI
    def __init__(self,
                 observation_space,
                 action_space,
                 sess,
                 obs_input,
                 action_sampler,
                 loss,
                 loss_inputs,
                 model=None,
                 action_prob=None,
                 state_inputs=None,
                 state_outputs=None,
                 prev_action_input=None,
                 prev_reward_input=None,
                 seq_lens=None,
                 max_seq_len=20,
                 batch_divisibility_req=1,
                 update_ops=None):
        """Initialize the policy graph.

        Arguments:
            observation_space (gym.Space): Observation space of the env.
            action_space (gym.Space): Action space of the env.
            sess (Session): TensorFlow session to use.
            obs_input (Tensor): input placeholder for observations, of shape
                [BATCH_SIZE, obs...].
            action_sampler (Tensor): Tensor for sampling an action, of shape
                [BATCH_SIZE, action...]
            loss (Tensor): scalar policy loss output tensor.
            loss_inputs (list): a (name, placeholder) tuple for each loss
                input argument. Each placeholder name must correspond to a
                SampleBatch column key returned by postprocess_trajectory(),
                and has shape [BATCH_SIZE, data...]. These keys will be read
                from postprocessed sample batches and fed into the specified
                placeholders during loss computation.
            model (rllib.models.Model): used to integrate custom losses and
                stats from user-defined RLlib models.
            action_prob (Tensor): probability of the sampled action.
            state_inputs (list): list of RNN state input Tensors.
            state_outputs (list): list of RNN state output Tensors.
            prev_action_input (Tensor): placeholder for previous actions
            prev_reward_input (Tensor): placeholder for previous rewards
            seq_lens (Tensor): placeholder for RNN sequence lengths, of shape
                [NUM_SEQUENCES]. Note that NUM_SEQUENCES << BATCH_SIZE. See
                models/lstm.py for more information.
            max_seq_len (int): max sequence length for LSTM training.
            batch_divisibility_req (int): pad all agent experiences batches to
                multiples of this value. This only has an effect if not using
                a LSTM model.
            update_ops (list): override the batchnorm update ops to run when
                applying gradients. Otherwise we run all update ops found in
                the current variable scope.
        """

        self.observation_space = observation_space
        self.action_space = action_space
        self.model = model
        self._sess = sess
        self._obs_input = obs_input
        self._prev_action_input = prev_action_input
        self._prev_reward_input = prev_reward_input
        self._sampler = action_sampler
        self._loss_inputs = loss_inputs
        self._loss_input_dict = dict(self._loss_inputs)
        self._is_training = self._get_is_training_placeholder()
        self._action_prob = action_prob
        self._state_inputs = state_inputs or []
        self._state_outputs = state_outputs or []
        for i, ph in enumerate(self._state_inputs):
            self._loss_input_dict["state_in_{}".format(i)] = ph
        self._seq_lens = seq_lens
        self._max_seq_len = max_seq_len
        self._batch_divisibility_req = batch_divisibility_req

        # The porting changes below.
        self._loss = None
        self._stats_fetches = {}
        self._optimizer = None
        self._grads_and_vars = []
        self._grads = []
        self._variables = None
        self._update_ops = None
        self._apply_op = None

    def get_session(self):
        return self._sess

    def variables(self):
        return self.model.variables()

    def _initialize_loss(self, loss, loss_inputs):
        self._loss_inputs = loss_inputs
        self._loss_input_dict = dict(self._loss_inputs)
        for i, ph in enumerate(self._state_inputs):
            self._loss_input_dict["state_in_{}".format(i)] = ph

        if self.model:
            self._loss = self.model.custom_loss(loss, self._loss_input_dict)
            self._stats_fetches.update({"model": self.model.metrics()})
        else:
            self._loss = loss

        self._optimizer = self.optimizer()
        self._grads_and_vars = [
            (g, v)
            for (g, v) in self.gradients(self._optimizer, self._loss)
            if g is not None
        ]
        self._grads = [g for (g, v) in self._grads_and_vars]
        try:
            self._variables = TensorFlowVariables([], self._sess, self.variables())
        except AttributeError:
            # TODO(ekl) deprecate support for v1 models
            self._variables = TensorFlowVariables(self._loss, self._sess)

        # gather update ops for any batch norm layers
        if not self._update_ops:
            self._update_ops = tf.get_collection(
                tf.GraphKeys.UPDATE_OPS, scope=tf.get_variable_scope().name
            )
        if self._update_ops:
            logger.info(
                "Update ops to run on apply gradient: {}".format(self._update_ops)
            )
        with tf.control_dependencies(self._update_ops):
            self._apply_op = self.build_apply_op(self._optimizer, self._grads_and_vars)

        if log_once("loss_used"):
            logger.debug(
                "These tensors were used in the loss_fn:\n\n{}\n".format(
                    summarize(self._loss_input_dict)
                )
            )

        self._sess.run(tf.global_variables_initializer())


class ModelCatalog(models.ModelCatalog):
    @staticmethod
    def _wrap_if_needed(model_cls, model_interface):

        if not model_interface or issubclass(model_cls, model_interface):
            return model_cls

        class wrapper(model_interface, model_cls):
            pass

        name = "{}_as_{}".format(model_cls.__name__, model_interface.__name__)
        wrapper.__name__ = name
        wrapper.__qualname__ = name

        return wrapper

    @staticmethod
    @DeveloperAPI
    def get_model_v2(
        obs_space,
        action_space,
        num_outputs,
        model_config,
        framework,
        name="default_model",
        model_interface=None,
        default_model=None,
        **model_kwargs
    ):
        """Returns a suitable model compatible with given spaces and output.

        Args:
            obs_space (Space): Observation space of the target gym env. This
                may have an `original_space` attribute that specifies how to
                unflatten the tensor into a ragged tensor.
            action_space (Space): Action space of the target gym env.
            num_outputs (int): The size of the output vector of the model.
            framework (str): Either "tf" or "torch".
            name (str): Name (scope) for the model.
            model_interface (cls): Interface required for the model
            default_model (cls): Override the default class for the model. This
                only has an effect when not using a custom model
            model_kwargs (dict): args to pass to the ModelV2 constructor

        Returns:
            model (ModelV2): Model to use for the policy.
        """

        if model_config.get("custom_model"):
            model_cls = _global_registry.get(RLLIB_MODEL, model_config["custom_model"])
            if issubclass(model_cls, ModelV2):
                if framework == "tf":
                    logger.info("Wrapping {} as {}".format(model_cls, model_interface))
                    model_cls = ModelCatalog._wrap_if_needed(model_cls, model_interface)
                    created = set()

                    # Track and warn if vars were created but not registered
                    def track_var_creation(next_creator, **kw):
                        v = next_creator(**kw)
                        created.add(v)
                        return v

                    with tf.variable_creator_scope(track_var_creation):
                        instance = model_cls(
                            obs_space,
                            action_space,
                            num_outputs,
                            model_config,
                            name,
                            **model_kwargs
                        )
                    registered = set(instance.variables())
                    not_registered = set()
                    for var in created:
                        if var not in registered:
                            not_registered.add(var)
                    if not_registered:
                        raise ValueError(
                            "It looks like variables {} were created as part "
                            "of {} but does not appear in model.variables() "
                            "({}). Did you forget to call "
                            "model.register_variables() on the variables in "
                            "question?".format(not_registered, instance, registered)
                        )
                else:
                    # no variable tracking
                    instance = model_cls(
                        obs_space,
                        action_space,
                        num_outputs,
                        model_config,
                        name,
                        **model_kwargs
                    )
                return instance
            elif executing_eagerly():
                raise ValueError(
                    "Eager execution requires a TFModelV2 model to be "
                    "used, however you specified a custom model {}".format(model_cls)
                )

        if framework == "tf":
            v2_class = None
            # try to get a default v2 model
            if not model_config.get("custom_model"):
                v2_class = default_model or ModelCatalog._get_v2_model(
                    obs_space, model_config
                )
            # fallback to a default v1 model
            if v2_class is None:
                if executing_eagerly():
                    raise ValueError(
                        "Eager execution requires a TFModelV2 model to be "
                        "used, however there is no default V2 model for this "
                        "observation space: {}, use_lstm={}".format(
                            obs_space, model_config.get("use_lstm")
                        )
                    )
                v2_class = make_v1_wrapper(ModelCatalog.get_model)
            # wrap in the requested interface
            wrapper = ModelCatalog._wrap_if_needed(v2_class, model_interface)
            return wrapper(
                obs_space, action_space, num_outputs, model_config, name, **model_kwargs
            )
        elif framework == "torch":
            if default_model:
                return default_model(
                    obs_space, action_space, num_outputs, model_config, name
                )
            return ModelCatalog._get_default_torch_model_v2(
                obs_space, action_space, num_outputs, model_config, name
            )
        else:
            raise NotImplementedError(
                "Framework must be 'tf' or 'torch': {}".format(framework)
            )
