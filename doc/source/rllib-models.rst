RLlib Models, Preprocessors, and Action Distributions
=====================================================

The following diagram provides a conceptual overview of data flow between different components in RLlib. We start with an ``Environment``, which given an action produces an observation. The observation is preprocessed by a ``Preprocessor`` and ``Filter`` (e.g. for running mean normalization) before being sent to a neural network ``Model``. The model output is in turn interpreted by an ``ActionDistribution`` to determine the next action.

.. image:: rllib-components.svg

The components highlighted in green can be replaced with custom user-defined implementations, as described in the next sections. The purple components are RLlib internal, which means they can only be modified by changing the algorithm source code.


Default Behaviours
------------------

Built-in Models and Preprocessors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

RLlib picks default models based on a simple heuristic: a `vision network <https://github.com/ray-project/ray/blob/master/rllib/models/tf/visionnet_v1.py>`__ for observations that have shape of length larger than 2 (for example, (84 x 84 x 3)), and a `fully connected network <https://github.com/ray-project/ray/blob/master/rllib/models/tf/fcnet_v1.py>`__ for everything else. These models can be configured via the ``model`` config key, documented in the model `catalog <https://github.com/ray-project/ray/blob/master/rllib/models/catalog.py>`__. Note that you'll probably have to configure ``conv_filters`` if your environment observations have custom sizes, e.g., ``"model": {"dim": 42, "conv_filters": [[16, [4, 4], 2], [32, [4, 4], 2], [512, [11, 11], 1]]}`` for 42x42 observations.

In addition, if you set ``"model": {"use_lstm": true}``, then the model output will be further processed by a `LSTM cell <https://github.com/ray-project/ray/blob/master/rllib/models/tf/lstm_v1.py>`__. More generally, RLlib supports the use of recurrent models for its policy gradient algorithms (A3C, PPO, PG, IMPALA), and RNN support is built into its policy evaluation utilities.

For preprocessors, RLlib tries to pick one of its built-in preprocessor based on the environment's observation space. Discrete observations are one-hot encoded, Atari observations downscaled, and Tuple and Dict observations flattened (these are unflattened and accessible via the ``input_dict`` parameter in custom models). Note that for Atari, RLlib defaults to using the `DeepMind preprocessors <https://github.com/ray-project/ray/blob/master/rllib/env/atari_wrappers.py>`__, which are also used by the OpenAI baselines library.

Built-in Model Parameters
~~~~~~~~~~~~~~~~~~~~~~~~~

The following is a list of the built-in model hyperparameters:

.. literalinclude:: ../../rllib/models/catalog.py
   :language: python
   :start-after: __sphinx_doc_begin__
   :end-before: __sphinx_doc_end__

TensorFlow Models
-----------------

.. note::

    TFModelV2 replaces the previous ``rllib.models.Model`` class, which did not support Keras-style reuse of variables. The ``rllib.models.Model`` class is deprecated and should not be used.

Custom TF models should subclass `TFModelV2 <https://github.com/ray-project/ray/blob/master/rllib/models/tf/tf_modelv2.py>`__ to implement the ``__init__()`` and ``forward()`` methods. Forward takes in a dict of tensor inputs (the observation ``obs``, ``prev_action``, and ``prev_reward``, ``is_training``), optional RNN state, and returns the model output of size ``num_outputs`` and the new state. You can also override extra methods of the model such as ``value_function`` to implement a custom value branch. Additional supervised / self-supervised losses can be added via the ``custom_loss`` method:

.. autoclass:: ray.rllib.models.tf.tf_modelv2.TFModelV2

    .. automethod:: __init__
    .. automethod:: forward
    .. automethod:: value_function
    .. automethod:: custom_loss
    .. automethod:: metrics
    .. automethod:: update_ops
    .. automethod:: register_variables
    .. automethod:: variables
    .. automethod:: trainable_variables

Once implemented, the model can then be registered and used in place of a built-in model:

.. code-block:: python

    import ray
    import ray.rllib.agents.ppo as ppo
    from ray.rllib.models import ModelCatalog
    from ray.rllib.models.tf.tf_modelv2 import TFModelV2

    class MyModelClass(TFModelV2):
        def __init__(self, obs_space, action_space, num_outputs, model_config, name): ...
        def forward(self, input_dict, state, seq_lens): ...
        def value_function(self): ...

    ModelCatalog.register_custom_model("my_model", MyModelClass)

    ray.init()
    trainer = ppo.PPOTrainer(env="CartPole-v0", config={
        "model": {
            "custom_model": "my_model",
            "custom_options": {},  # extra options to pass to your model
        },
    })

For a full example of a custom model in code, see the `keras model example <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_keras_model.py>`__. You can also reference the `unit tests <https://github.com/ray-project/ray/blob/master/rllib/tests/test_nested_spaces.py>`__ for Tuple and Dict spaces, which show how to access nested observation fields.

Recurrent Models
~~~~~~~~~~~~~~~~

Instead of using the ``use_lstm: True`` option, it can be preferable use a custom recurrent model. This provides more control over postprocessing of the LSTM output and can also allow the use of multiple LSTM cells to process different portions of the input. For a RNN model it is preferred to subclass ``RecurrentTFModelV2`` to implement ``__init__()``, ``get_initial_state()``, and ``forward_rnn()``. You can check out the `custom_keras_rnn_model.py <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_keras_rnn_model.py>`__ model as an example to implement your own model:

.. autoclass:: ray.rllib.models.tf.recurrent_tf_modelv2.RecurrentTFModelV2

    .. automethod:: __init__
    .. automethod:: forward_rnn
    .. automethod:: get_initial_state

Batch Normalization
~~~~~~~~~~~~~~~~~~~

You can use ``tf.layers.batch_normalization(x, training=input_dict["is_training"])`` to add batch norm layers to your custom model: `code example <https://github.com/ray-project/ray/blob/master/rllib/examples/batch_norm_model.py>`__. RLlib will automatically run the update ops for the batch norm layers during optimization (see `tf_policy.py <https://github.com/ray-project/ray/blob/master/rllib/policy/tf_policy.py>`__ and `multi_gpu_impl.py <https://github.com/ray-project/ray/blob/master/rllib/optimizers/multi_gpu_impl.py>`__ for the exact handling of these updates).

In case RLlib does not properly detect the update ops for your custom model, you can override the ``update_ops()`` method to return the list of ops to run for updates.

PyTorch Models
--------------

Similarly, you can create and register custom PyTorch models for use with PyTorch-based algorithms (e.g., A2C, PG, QMIX). See these examples of `fully connected <https://github.com/ray-project/ray/blob/master/rllib/models/torch/fcnet.py>`__, `convolutional <https://github.com/ray-project/ray/blob/master/rllib/models/torch/visionnet.py>`__, and `recurrent <https://github.com/ray-project/ray/blob/master/rllib/agents/qmix/model.py>`__ torch models.

.. autoclass:: ray.rllib.models.torch.torch_modelv2.TorchModelV2

    .. automethod:: __init__
    .. automethod:: forward
    .. automethod:: value_function
    .. automethod:: custom_loss
    .. automethod:: metrics
    .. automethod:: get_initial_state

Once implemented, the model can then be registered and used in place of a built-in model:

.. code-block:: python

    import torch.nn as nn

    import ray
    from ray.rllib.agents import a3c
    from ray.rllib.models import ModelCatalog
    from ray.rllib.models.torch.torch_modelv2 import TorchModelV2

    class CustomTorchModel(nn.Module, TorchModelV2):
        def __init__(self, obs_space, action_space, num_outputs, model_config, name): ...
        def forward(self, input_dict, state, seq_lens): ...
        def value_function(self): ...

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

.. warning::

    Custom preprocessors are deprecated, since they sometimes conflict with the built-in preprocessors for handling complex observation spaces. Please use `wrapper classes <https://github.com/openai/gym/tree/master/gym/wrappers>`__ around your environment instead of preprocessors.

Custom preprocessors should subclass the RLlib `preprocessor class <https://github.com/ray-project/ray/blob/master/rllib/models/preprocessors.py>`__ and be registered in the model catalog:

.. code-block:: python

    import ray
    import ray.rllib.agents.ppo as ppo
    from ray.rllib.models import ModelCatalog
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

Custom Action Distributions
---------------------------

Similar to custom models and preprocessors, you can also specify a custom action distribution class as follows. The action dist class is passed a reference to the ``model``, which you can use to access ``model.model_config`` or other attributes of the model. This is commonly used to implement `autoregressive action outputs <#autoregressive-action-distributions>`__.

.. code-block:: python

    import ray
    import ray.rllib.agents.ppo as ppo
    from ray.rllib.models import ModelCatalog
    from ray.rllib.models.preprocessors import Preprocessor

    class MyActionDist(ActionDistribution):
        @staticmethod
        def required_model_output_shape(action_space, model_config):
            return 7  # controls model output feature vector size

        def __init__(self, inputs, model):
            super(MyActionDist, self).__init__(inputs, model)
            assert model.num_outputs == 7

        def sample(self): ...
        def logp(self, actions): ...
        def entropy(self): ...

    ModelCatalog.register_custom_action_dist("my_dist", MyActionDist)

    ray.init()
    trainer = ppo.PPOTrainer(env="CartPole-v0", config={
        "model": {
            "custom_action_dist": "my_dist",
        },
    })

Supervised Model Losses
-----------------------

You can mix supervised losses into any RLlib algorithm through custom models. For example, you can add an imitation learning loss on expert experiences, or a self-supervised autoencoder loss within the model. These losses can be defined over either policy evaluation inputs, or data read from `offline storage <rllib-offline.html#input-pipeline-for-supervised-losses>`__.

**TensorFlow**: To add a supervised loss to a custom TF model, you need to override the ``custom_loss()`` method. This method takes in the existing policy loss for the algorithm, which you can add your own supervised loss to before returning. For debugging, you can also return a dictionary of scalar tensors in the ``metrics()`` method. Here is a `runnable example <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_loss.py>`__ of adding an imitation loss to CartPole training that is defined over a `offline dataset <rllib-offline.html#input-pipeline-for-supervised-losses>`__.

**PyTorch**: There is no explicit API for adding losses to custom torch models. However, you can modify the loss in the policy definition directly. Like for TF models, offline datasets can be incorporated by creating an input reader and calling ``reader.next()`` in the loss forward pass.

Self-Supervised Model Losses
----------------------------

You can also use the ``custom_loss()`` API to add in self-supervised losses such as VAE reconstruction loss and L2-regularization.

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

    class ParametricActionsModel(TFModelV2):
        def __init__(self,
                     obs_space,
                     action_space,
                     num_outputs,
                     model_config,
                     name,
                     true_obs_shape=(4,),
                     action_embed_size=2):
            super(ParametricActionsModel, self).__init__(
                obs_space, action_space, num_outputs, model_config, name)
            self.action_embed_model = FullyConnectedNetwork(...)

        def forward(self, input_dict, state, seq_lens):
            # Extract the available actions tensor from the observation.
            avail_actions = input_dict["obs"]["avail_actions"]
            action_mask = input_dict["obs"]["action_mask"]

            # Compute the predicted action embedding
            action_embed, _ = self.action_embed_model({
                "obs": input_dict["obs"]["cart"]
            })

            # Expand the model output to [BATCH, 1, EMBED_SIZE]. Note that the
            # avail actions tensor is of shape [BATCH, MAX_ACTIONS, EMBED_SIZE].
            intent_vector = tf.expand_dims(action_embed, 1)

            # Batch dot product => shape of logits is [BATCH, MAX_ACTIONS].
            action_logits = tf.reduce_sum(avail_actions * intent_vector, axis=2)

            # Mask out invalid actions (use tf.float32.min for stability)
            inf_mask = tf.maximum(tf.log(action_mask), tf.float32.min)
            return action_logits + inf_mask, state


Depending on your use case it may make sense to use just the masking, just action embeddings, or both. For a runnable example of this in code, check out `parametric_action_cartpole.py <https://github.com/ray-project/ray/blob/master/rllib/examples/parametric_action_cartpole.py>`__. Note that since masking introduces ``tf.float32.min`` values into the model output, this technique might not work with all algorithm options. For example, algorithms might crash if they incorrectly process the ``tf.float32.min`` values. The cartpole example has working configurations for DQN (must set ``hiddens=[]``), PPO (must disable running mean and set ``vf_share_layers=True``), and several other algorithms. Not all algorithms support parametric actions; see the `feature compatibility matrix <rllib-env.html#feature-compatibility-matrix>`__.


Autoregressive Action Distributions
-----------------------------------

In an action space with multiple components (e.g., ``Tuple(a1, a2)``), you might want ``a2`` to be conditioned on the sampled value of ``a1``, i.e., ``a2_sampled ~ P(a2 | a1_sampled, obs)``. Normally, ``a1`` and ``a2`` would be sampled independently, reducing the expressivity of the policy.

To do this, you need both a custom model that implements the autoregressive pattern, and a custom action distribution class that leverages that model. The `autoregressive_action_dist.py <https://github.com/ray-project/ray/blob/master/rllib/examples/autoregressive_action_dist.py>`__ example shows how this can be implemented for a simple binary action space. For a more complex space, a more efficient architecture such as a `MADE <https://arxiv.org/abs/1502.03509>`__ is recommended. Note that sampling a `N-part` action requires `N` forward passes through the model, however computing the log probability of an action can be done in one pass:

.. code-block:: python

    class BinaryAutoregressiveOutput(ActionDistribution):
        """Action distribution P(a1, a2) = P(a1) * P(a2 | a1)"""

        @staticmethod
        def required_model_output_shape(self, model_config):
            return 16  # controls model output feature vector size

        def sample(self):
            # first, sample a1
            a1_dist = self._a1_distribution()
            a1 = a1_dist.sample()

            # sample a2 conditioned on a1
            a2_dist = self._a2_distribution(a1)
            a2 = a2_dist.sample()

            # return the action tuple
            return TupleActions([a1, a2])

        def logp(self, actions):
            a1, a2 = actions[:, 0], actions[:, 1]
            a1_vec = tf.expand_dims(tf.cast(a1, tf.float32), 1)
            a1_logits, a2_logits = self.model.action_model([self.inputs, a1_vec])
            return (Categorical(a1_logits, None).logp(a1) + Categorical(
                a2_logits, None).logp(a2))

        def _a1_distribution(self):
            BATCH = tf.shape(self.inputs)[0]
            a1_logits, _ = self.model.action_model(
                [self.inputs, tf.zeros((BATCH, 1))])
            a1_dist = Categorical(a1_logits, None)
            return a1_dist

        def _a2_distribution(self, a1):
            a1_vec = tf.expand_dims(tf.cast(a1, tf.float32), 1)
            _, a2_logits = self.model.action_model([self.inputs, a1_vec])
            a2_dist = Categorical(a2_logits, None)
            return a2_dist

    class AutoregressiveActionsModel(TFModelV2):
        """Implements the `.action_model` branch required above."""

        def __init__(self, obs_space, action_space, num_outputs, model_config,
                     name):
            super(AutoregressiveActionsModel, self).__init__(
                obs_space, action_space, num_outputs, model_config, name)
            if action_space != Tuple([Discrete(2), Discrete(2)]):
                raise ValueError(
                    "This model only supports the [2, 2] action space")

            # Inputs
            obs_input = tf.keras.layers.Input(
                shape=obs_space.shape, name="obs_input")
            a1_input = tf.keras.layers.Input(shape=(1, ), name="a1_input")
            ctx_input = tf.keras.layers.Input(
                shape=(num_outputs, ), name="ctx_input")

            # Output of the model (normally 'logits', but for an autoregressive
            # dist this is more like a context/feature layer encoding the obs)
            context = tf.keras.layers.Dense(
                num_outputs,
                name="hidden",
                activation=tf.nn.tanh,
                kernel_initializer=normc_initializer(1.0))(obs_input)

            # P(a1 | obs)
            a1_logits = tf.keras.layers.Dense(
                2,
                name="a1_logits",
                activation=None,
                kernel_initializer=normc_initializer(0.01))(ctx_input)

            # P(a2 | a1)
            # --note: typically you'd want to implement P(a2 | a1, obs) as follows:
            # a2_context = tf.keras.layers.Concatenate(axis=1)(
            #     [ctx_input, a1_input])
            a2_context = a1_input
            a2_hidden = tf.keras.layers.Dense(
                16,
                name="a2_hidden",
                activation=tf.nn.tanh,
                kernel_initializer=normc_initializer(1.0))(a2_context)
            a2_logits = tf.keras.layers.Dense(
                2,
                name="a2_logits",
                activation=None,
                kernel_initializer=normc_initializer(0.01))(a2_hidden)

            # Base layers
            self.base_model = tf.keras.Model(obs_input, context)
            self.register_variables(self.base_model.variables)
            self.base_model.summary()

            # Autoregressive action sampler
            self.action_model = tf.keras.Model([ctx_input, a1_input],
                                               [a1_logits, a2_logits])
            self.action_model.summary()
            self.register_variables(self.action_model.variables)



.. note::

   Not all algorithms support autoregressive action distributions; see the `feature compatibility matrix <rllib-env.html#feature-compatibility-matrix>`__.
