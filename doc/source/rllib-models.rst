RLlib Models and Preprocessors
==============================

The following diagram provides a conceptual overview of data flow between different components in RLlib. We start with an ``Environment``, which given an action produces an observation. The observation is preprocessed by a ``Preprocessor`` and ``Filter`` (e.g. for running mean normalization) before being sent to a neural network ``Model``. The model output is in turn interpreted by an ``ActionDistribution`` to determine the next action.

.. image:: rllib-components.svg

The components highlighted in green can be replaced with custom user-defined implementations, as described in the next sections. The purple components are RLlib internal, which means they can only be modified by changing the algorithm source code.


Default Behaviours
------------------

Built-in Models and Preprocessors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

RLlib picks default models based on a simple heuristic: a `vision network <https://github.com/ray-project/ray/blob/master/python/ray/rllib/models/visionnet.py>`__ for image observations, and a `fully connected network <https://github.com/ray-project/ray/blob/master/python/ray/rllib/models/fcnet.py>`__ for everything else. These models can be configured via the ``model`` config key, documented in the model `catalog <https://github.com/ray-project/ray/blob/master/python/ray/rllib/models/catalog.py>`__. Note that you'll probably have to configure ``conv_filters`` if your environment observations have custom sizes, e.g., ``"model": {"dim": 42, "conv_filters": [[16, [4, 4], 2], [32, [4, 4], 2], [512, [11, 11], 1]]}`` for 42x42 observations.

In addition, if you set ``"model": {"use_lstm": true}``, then the model output will be further processed by a `LSTM cell <https://github.com/ray-project/ray/blob/master/python/ray/rllib/models/lstm.py>`__. More generally, RLlib supports the use of recurrent models for its policy gradient algorithms (A3C, PPO, PG, IMPALA), and RNN support is built into its policy evaluation utilities.

For preprocessors, RLlib tries to pick one of its built-in preprocessor based on the environment's observation space. Discrete observations are one-hot encoded, Atari observations downscaled, and Tuple and Dict observations flattened (these are unflattened and accessible via the ``input_dict`` parameter in custom models). Note that for Atari, RLlib defaults to using the `DeepMind preprocessors <https://github.com/ray-project/ray/blob/master/python/ray/rllib/env/atari_wrappers.py>`__, which are also used by the OpenAI baselines library.

Built-in Model Parameters
~~~~~~~~~~~~~~~~~~~~~~~~~

The following is a list of the built-in model hyperparameters:

.. literalinclude:: ../../python/ray/rllib/models/catalog.py
   :language: python
   :start-after: __sphinx_doc_begin__
   :end-before: __sphinx_doc_end__

TensorFlow Models
-----------------

.. note::

    TFModelV2 replaces the previous ``rllib.models.Model`` class, which did not support Keras-style reuse of variables. The ``rllib.models.Model`` class is deprecated and should not be used.

Custom TF models should subclass `TFModelV2 <https://github.com/ray-project/ray/blob/master/python/ray/rllib/models/tf/tf_modelv2.py>`__ to implement the ``__init__()`` and ``forward()`` methods. Forward takes in a dict of tensor inputs (the observation ``obs``, ``prev_action``, and ``prev_reward``, ``is_training``), optional RNN state, and returns the model output of size ``num_outputs`` and the new state. You can also override extra methods of the model such as ``value_function`` to implement a custom value branch. Additional supervised / self-supervised losses can be added via the ``custom_loss`` method. The model can then be registered and used in place of a built-in model:

.. code-block:: python

    import ray
    import ray.rllib.agents.ppo as ppo
    from ray.rllib.models import ModelCatalog
    from ray.rllib.models.tf.tf_modelv2 import TFModelV2

    class MyModelClass(TFModelV2):
        def __init__(self, obs_space, action_space, num_outputs, model_config,
                     name):
            """Initialize the model.

            This method should create any variables used by the model.
            """
            super(MyModelModel, self).__init__(obs_space, action_space,
                                               num_outputs, model_config, name)
            input_layer = tf.keras.layers.Input(...)
            hidden_layer = tf.keras.layers.Dense(...)(input_layer)
            output_layer = tf.keras.layers.Dense(...)(hidden_layer)
            value_layer = tf.keras.layers.Dense(...)(hidden_layer)
            self.base_model = tf.keras.Model(input_layer, [output_layer, value_layer])
            self.register_variables(self.base_model.variables)

        def forward(self, input_dict, state, seq_lens):
            """Call the model with the given input tensors and state.

            Any complex observations (dicts, tuples, etc.) will be unpacked by
            __call__ before being passed to forward(). To access the flattened
            observation tensor, refer to input_dict["obs_flat"].

            This method can be called any number of times. In eager execution,
            each call to forward() will eagerly evaluate the model. In symbolic
            execution, each call to forward creates a computation graph that
            operates over the variables of this model (i.e., shares weights).

            Custom models should override this instead of __call__.

            Arguments:
                input_dict (dict): dictionary of input tensors, including "obs",
                    "obs_flat", "prev_action", "prev_reward", "is_training"
                state (list): list of state tensors with sizes matching those
                    returned by get_initial_state + the batch dimension
                seq_lens (Tensor): 1d tensor holding input sequence lengths

            Returns:
                (outputs, state): The model output tensor of size
                    [BATCH, num_outputs]
            """
            model_out, self._value_out = self.base_model(input_dict["obs"])
            return model_out, state

        def value_function(self):
            """Return the value function estimate for the most recent forward pass.

            Returns:
                value estimate tensor of shape [BATCH].
            """
            return self._value_out

        def custom_loss(self, policy_loss, loss_inputs):
            """Override to customize the loss function used to optimize this model.

            This can be used to incorporate self-supervised losses (by defining
            a loss over existing input and output tensors of this model), and
            supervised losses (by defining losses over a variable-sharing copy of
            this model's layers).

            You can find an runnable example in examples/custom_loss.py.

            Arguments:
                policy_loss (Tensor): scalar policy loss from the policy.
                loss_inputs (dict): map of input placeholders for rollout data.

            Returns:
                Scalar tensor for the customized loss for this model.
            """
            return policy_loss

        def metrics(self):
            """Override to return custom metrics from your model.

            The stats will be reported as part of the learner stats, i.e.,
                info:
                    learner:
                        model:
                            key1: metric1
                            key2: metric2

            Returns:
                Dict of string keys to scalar tensors.
            """
            return {}

    ModelCatalog.register_custom_model("my_model", MyModelClass)

    ray.init()
    trainer = ppo.PPOTrainer(env="CartPole-v0", config={
        "model": {
            "custom_model": "my_model",
            "custom_options": {},  # extra options to pass to your model
        },
    })

For a full example of a custom model in code, see the `keras model example <https://github.com/ray-project/ray/blob/master/python/ray/rllib/examples/custom_keras_model.py>`__. You can also reference the `unit tests <https://github.com/ray-project/ray/blob/master/python/ray/rllib/tests/test_nested_spaces.py>`__ for Tuple and Dict spaces, which show how to access nested observation fields.

Recurrent Models
~~~~~~~~~~~~~~~~

Instead of using the ``use_lstm: True`` option, it can be preferable use a custom recurrent model. This provides more control over postprocessing of the LSTM output and can also allow the use of multiple LSTM cells to process different portions of the input. For a RNN model it is preferred to subclass ``RecurrentTFModelV2`` to implement ``__init__()``, ``get_initial_state()``, and ``forward_rnn()``. You can check out the `custom_keras_rnn_model.py <https://github.com/ray-project/ray/blob/master/python/ray/rllib/examples/custom_keras_rnn_model.py>`__ model as an example to implement your own model:

.. code-block:: python

    class MyKerasRNN(RecurrentTFModelV2):
        def __init__(self,
                     obs_space,
                     action_space,
                     num_outputs,
                     model_config,
                     name,
                     cell_size=64):
            super(MyKerasRNN, self).__init__(obs_space, action_space, num_outputs,
                                             model_config, name)
            self.cell_size = cell_size

            # Define input layers
            input_layer = tf.keras.layers.Input(shape=(None, obs_space.shape[0]))
            state_in_h = tf.keras.layers.Input(shape=(cell_size, ))
            state_in_c = tf.keras.layers.Input(shape=(cell_size, ))
            seq_in = tf.keras.layers.Input(shape=())

            # Send to LSTM cell
            lstm_out, state_h, state_c = tf.keras.layers.LSTM(
                cell_size, return_sequences=True, return_state=True, name="lstm")(
                    inputs=input_layer,
                    mask=tf.sequence_mask(seq_in),
                    initial_state=[state_in_h, state_in_c])
            output_layer = tf.keras.layers.Dense(...)(lstm_out)

            # Create the RNN model
            self.rnn_model = tf.keras.Model(
                inputs=[input_layer, seq_in, state_in_h, state_in_c],
                outputs=[output_layer, state_h, state_c])
            self.register_variables(self.rnn_model.variables)
            self.rnn_model.summary()

        @override(RecurrentTFModelV2)
        def forward_rnn(self, inputs, state, seq_lens):
            model_out, h, c = self.rnn_model([inputs, seq_lens] + state)
            return model_out, [h, c]

        @override(ModelV2)
        def get_initial_state(self):
            return [
                np.zeros(self.cell_size, np.float32),
                np.zeros(self.cell_size, np.float32),
            ]


Batch Normalization
~~~~~~~~~~~~~~~~~~~

You can use ``tf.layers.batch_normalization(x, training=input_dict["is_training"])`` to add batch norm layers to your custom model: `code example <https://github.com/ray-project/ray/blob/master/python/ray/rllib/examples/batch_norm_model.py>`__. RLlib will automatically run the update ops for the batch norm layers during optimization (see `tf_policy.py <https://github.com/ray-project/ray/blob/master/python/ray/rllib/policy/tf_policy.py>`__ and `multi_gpu_impl.py <https://github.com/ray-project/ray/blob/master/python/ray/rllib/optimizers/multi_gpu_impl.py>`__ for the exact handling of these updates).

In case RLlib does not properly detect the update ops for your custom model, you can override the ``update_ops()`` method to return the list of ops to run for updates.

PyTorch Models
--------------

Similarly, you can create and register custom PyTorch models for use with PyTorch-based algorithms (e.g., A2C, PG, QMIX). See these examples of `fully connected <https://github.com/ray-project/ray/blob/master/python/ray/rllib/models/torch/fcnet.py>`__, `convolutional <https://github.com/ray-project/ray/blob/master/python/ray/rllib/models/torch/visionnet.py>`__, and `recurrent <https://github.com/ray-project/ray/blob/master/python/ray/rllib/agents/qmix/model.py>`__ torch models.

.. code-block:: python

    import ray
    from ray.rllib.agents import a3c
    from ray.rllib.models import ModelCatalog
    from ray.rllib.models.torch.torch_modelv2 import TorchModelV2

    class CustomTorchModel(TorchModelV2):

        def __init__(self, obs_space, action_space, num_outputs, model_config,
                     name):
            super(CustomTorchModel, self).__init__(
                obs_space, action_space, num_outputs, model_config, name)
            ...  # setup hidden layers

        def forward(self, input_dict, state, seq_lens):
            """Call the model with the given input tensors and state.

            Any complex observations (dicts, tuples, etc.) will be unpacked by
            __call__ before being passed to forward(). To access the flattened
            observation tensor, refer to input_dict["obs_flat"].

            This method can be called any number of times. In eager execution,
            each call to forward() will eagerly evaluate the model. In symbolic
            execution, each call to forward creates a computation graph that
            operates over the variables of this model (i.e., shares weights).

            Custom models should override this instead of __call__.

            Arguments:
                input_dict (dict): dictionary of input tensors, including "obs",
                    "obs_flat", "prev_action", "prev_reward", "is_training"
                state (list): list of state tensors with sizes matching those
                    returned by get_initial_state + the batch dimension
                seq_lens (Tensor): 1d tensor holding input sequence lengths

            Returns:
                (outputs, state): The model output tensor of size
                    [BATCH, num_outputs]
            """
            obs = input_dict["obs"]
            ...
            return logits, state

    ModelCatalog.register_custom_model("my_model", CustomTorchModel)

    ray.init()
    trainer = a3c.A2CTrainer(env="CartPole-v0", config={
        "use_pytorch": True,
        "model": {
            "custom_model": "my_model",
            "custom_options": {},  # extra options to pass to your model
        },
    })

Custom Preprocessors
--------------------

Custom preprocessors should subclass the RLlib `preprocessor class <https://github.com/ray-project/ray/blob/master/python/ray/rllib/models/preprocessors.py>`__ and be registered in the model catalog. Note that you can alternatively use `gym wrapper classes <https://github.com/openai/gym/tree/master/gym/wrappers>`__ around your environment instead of preprocessors.

.. code-block:: python

    import ray
    import ray.rllib.agents.ppo as ppo
    from ray.rllib.models.preprocessors import Preprocessor

    class MyPreprocessorClass(Preprocessor):
        def _init_shape(self, obs_space, options):
            return new_shape  # can vary depending on inputs

        def transform(self, observation):
            return ...  # return the preprocessed observation

    ModelCatalog.register_custom_preprocessor("my_prep", MyPreprocessorClass)

    ray.init()
    trainer = ppo.PPOTrainer(env="CartPole-v0", config={
        "model": {
            "custom_preprocessor": "my_prep",
            "custom_options": {},  # extra options to pass to your preprocessor
        },
    })

Supervised Model Losses
-----------------------

You can mix supervised losses into any RLlib algorithm through custom models. For example, you can add an imitation learning loss on expert experiences, or a self-supervised autoencoder loss within the model. These losses can be defined over either policy evaluation inputs, or data read from `offline storage <rllib-offline.html#input-pipeline-for-supervised-losses>`__.

**TensorFlow**: To add a supervised loss to a custom TF model, you need to override the ``custom_loss()`` method. This method takes in the existing policy loss for the algorithm, which you can add your own supervised loss to before returning. For debugging, you can also return a dictionary of scalar tensors in the ``metrics()`` method. Here is a `runnable example <https://github.com/ray-project/ray/blob/master/python/ray/rllib/examples/custom_loss.py>`__ of adding an imitation loss to CartPole training that is defined over a `offline dataset <rllib-offline.html#input-pipeline-for-supervised-losses>`__.

**PyTorch**: There is no explicit API for adding losses to custom torch models. However, you can modify the loss in the policy definition directly. Like for TF models, offline datasets can be incorporated by creating an input reader and calling ``reader.next()`` in the loss forward pass.


Variable-length / Parametric Action Spaces
------------------------------------------

Custom models can be used to work with environments where (1) the set of valid actions `varies per step <https://neuro.cs.ut.ee/the-use-of-embeddings-in-openai-five>`__, and/or (2) the number of valid actions is `very large <https://arxiv.org/abs/1811.00260>`__. The general idea is that the meaning of actions can be completely conditioned on the observation, i.e., the ``a`` in ``Q(s, a)`` becomes just a token in ``[0, MAX_AVAIL_ACTIONS)`` that only has meaning in the context of ``s``. This works with algorithms in the `DQN and policy-gradient families <rllib-env.html>`__ and can be implemented as follows:

1. The environment should return a mask and/or list of valid action embeddings as part of the observation for each step. To enable batching, the number of actions can be allowed to vary from 1 to some max number:

.. code-block:: python

   class MyParamActionEnv(gym.Env):
       def __init__(self, max_avail_actions):
           self.action_space = Discrete(max_avail_actions)
           self.observation_space = Dict({
               "action_mask": Box(0, 1, shape=(max_avail_actions, )),
               "avail_actions": Box(-1, 1, shape=(max_avail_actions, action_embedding_sz)),
               "real_obs": ...,
           })

2. A custom model can be defined that can interpret the ``action_mask`` and ``avail_actions`` portions of the observation. Here the model computes the action logits via the dot product of some network output and each action embedding. Invalid actions can be masked out of the softmax by scaling the probability to zero:

.. code-block:: python

    class MyParamActionModel(Model):
        def _build_layers_v2(self, input_dict, num_outputs, options):
            avail_actions = input_dict["obs"]["avail_actions"]
            action_mask = input_dict["obs"]["action_mask"]

            output = FullyConnectedNetwork(
                input_dict["obs"]["real_obs"], num_outputs=action_embedding_sz)

            # Expand the model output to [BATCH, 1, EMBED_SIZE]. Note that the
            # avail actions tensor is of shape [BATCH, MAX_ACTIONS, EMBED_SIZE].
            intent_vector = tf.expand_dims(output, 1)

            # Shape of logits is [BATCH, MAX_ACTIONS].
            action_logits = tf.reduce_sum(avail_actions * intent_vector, axis=2)

            # Mask out invalid actions (use tf.float32.min for stability)
            inf_mask = tf.maximum(tf.log(action_mask), tf.float32.min)
            masked_logits = inf_mask + action_logits

            return masked_logits, last_layer


Depending on your use case it may make sense to use just the masking, just action embeddings, or both. For a runnable example of this in code, check out `parametric_action_cartpole.py <https://github.com/ray-project/ray/blob/master/python/ray/rllib/examples/parametric_action_cartpole.py>`__. Note that since masking introduces ``tf.float32.min`` values into the model output, this technique might not work with all algorithm options. For example, algorithms might crash if they incorrectly process the ``tf.float32.min`` values. The cartpole example has working configurations for DQN (must set ``hiddens=[]``), PPO (must disable running mean and set ``vf_share_layers=True``), and several other algorithms.

Customizing Policies
-------------------------

For deeper customization of algorithms, you can modify the policies of the trainer classes. Here's an example of extending the DDPG policy to specify custom sub-network modules:

.. code-block:: python

    from ray.rllib.models import ModelCatalog
    from ray.rllib.agents.ddpg.ddpg_policy import DDPGTFPolicy as BaseDDPGTFPolicy

    class CustomPNetwork(object):
        def __init__(self, dim_actions, hiddens, activation):
            action_out = ...
            # Use sigmoid layer to bound values within (0, 1)
            # shape of action_scores is [batch_size, dim_actions]
            self.action_scores = layers.fully_connected(
                action_out, num_outputs=dim_actions, activation_fn=tf.nn.sigmoid)

    class CustomQNetwork(object):
        def __init__(self, action_inputs, hiddens, activation):
            q_out = ...
            self.value = layers.fully_connected(
                q_out, num_outputs=1, activation_fn=None)

    class CustomDDPGTFPolicy(BaseDDPGTFPolicy):
        def _build_p_network(self, obs):
            return CustomPNetwork(
                self.dim_actions,
                self.config["actor_hiddens"],
                self.config["actor_hidden_activation"]).action_scores

        def _build_q_network(self, obs, actions):
            return CustomQNetwork(
                actions,
                self.config["critic_hiddens"],
                self.config["critic_hidden_activation"]).value

Then, you can create an trainer with your custom policy by:

.. code-block:: python

    from ray.rllib.agents.ddpg.ddpg import DDPGTrainer
    from custom_policy import CustomDDPGTFPolicy

    DDPGTrainer._policy = CustomDDPGTFPolicy
    trainer = DDPGTrainer(...)

In this example we overrode existing methods of the existing DDPG policy, i.e., `_build_q_network`, `_build_p_network`, `_build_action_network`, `_build_actor_critic_loss`, but you can also replace the entire graph class entirely.

Model-Based Rollouts
~~~~~~~~~~~~~~~~~~~~

With a custom policy, you can also perform model-based rollouts and optionally incorporate the results of those rollouts as training data. For example, suppose you wanted to extend PGPolicy for model-based rollouts. This involves overriding the ``compute_actions`` method of that policy:

.. code-block:: python

        class ModelBasedPolicy(PGPolicy):
             def compute_actions(self,
                                 obs_batch,
                                 state_batches,
                                 prev_action_batch=None,
                                 prev_reward_batch=None,
                                 episodes=None):
                # compute a batch of actions based on the current obs_batch
                # and state of each episode (i.e., for multiagent). You can do
                # whatever is needed here, e.g., MCTS rollouts.
                return action_batch


If you want take this rollouts data and append it to the sample batch, use the ``add_extra_batch()`` method of the `episode objects <https://github.com/ray-project/ray/blob/master/python/ray/rllib/evaluation/episode.py>`__ passed in. For an example of this, see the ``testReturningModelBasedRolloutsData`` `unit test <https://github.com/ray-project/ray/blob/master/python/ray/rllib/tests/test_multi_agent_env.py>`__.
