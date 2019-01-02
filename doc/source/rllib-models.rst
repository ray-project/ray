RLlib Models and Preprocessors
==============================

The following diagram provides a conceptual overview of data flow between different components in RLlib. We start with an ``Environment``, which given an action produces an observation. The observation is preprocessed by a ``Preprocessor`` and ``Filter`` (e.g. for running mean normalization) before being sent to a neural network ``Model``. The model output is in turn interpreted by an ``ActionDistribution`` to determine the next action.

.. image:: rllib-components.svg

The components highlighted in green can be replaced with custom user-defined implementations, as described in the next sections. The purple components are RLlib internal, which means they can only be modified by changing the algorithm source code.


Built-in Models and Preprocessors
---------------------------------

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

Custom Models
-------------

Custom models should subclass the common RLlib `model class <https://github.com/ray-project/ray/blob/master/python/ray/rllib/models/model.py>`__ and override the ``_build_layers_v2`` method. This method takes in a dict of tensor inputs (the observation ``obs``, ``prev_action``, and ``prev_reward``, ``is_training``), and returns a feature layer and float vector of the specified output size. You can also override the ``value_function`` method to implement a custom value branch. A self-supervised loss can be defined via the ``loss`` method. The model can then be registered and used in place of a built-in model:

.. code-block:: python

    import ray
    import ray.rllib.agents.ppo as ppo
    from ray.rllib.models import ModelCatalog, Model

    class MyModelClass(Model):
        def _build_layers_v2(self, input_dict, num_outputs, options):
            """Define the layers of a custom model.

            Arguments:
                input_dict (dict): Dictionary of input tensors, including "obs",
                    "prev_action", "prev_reward", "is_training".
                num_outputs (int): Output tensor must be of size
                    [BATCH_SIZE, num_outputs].
                options (dict): Model options.

            Returns:
                (outputs, feature_layer): Tensors of size [BATCH_SIZE, num_outputs]
                    and [BATCH_SIZE, desired_feature_size].

            When using dict or tuple observation spaces, you can access
            the nested sub-observation batches here as well:

            Examples:
                >>> print(input_dict)
                {'prev_actions': <tf.Tensor shape=(?,) dtype=int64>,
                 'prev_rewards': <tf.Tensor shape=(?,) dtype=float32>,
                 'is_training': <tf.Tensor shape=(), dtype=bool>,
                 'obs': OrderedDict([
                    ('sensors', OrderedDict([
                        ('front_cam', [
                            <tf.Tensor shape=(?, 10, 10, 3) dtype=float32>,
                            <tf.Tensor shape=(?, 10, 10, 3) dtype=float32>]),
                        ('position', <tf.Tensor shape=(?, 3) dtype=float32>),
                        ('velocity', <tf.Tensor shape=(?, 3) dtype=float32>)]))])}
            """

            layer1 = slim.fully_connected(input_dict["obs"], 64, ...)
            layer2 = slim.fully_connected(layer1, 64, ...)
            ...
            return layerN, layerN_minus_1

        def value_function(self):
            """Builds the value function output.

            This method can be overridden to customize the implementation of the
            value function (e.g., not sharing hidden layers).

            Returns:
                Tensor of size [BATCH_SIZE] for the value function.
            """
            return tf.reshape(
                linear(self.last_layer, 1, "value", normc_initializer(1.0)), [-1])

        def loss(self):
            """Builds any built-in (self-supervised) loss for the model.

            For example, this can be used to incorporate auto-encoder style losses.
            Note that this loss has to be included in the policy graph loss to have
            an effect (done for built-in algorithms).

            Returns:
                Scalar tensor for the self-supervised loss.
            """
            return tf.constant(0.0)

    ModelCatalog.register_custom_model("my_model", MyModelClass)

    ray.init()
    agent = ppo.PPOAgent(env="CartPole-v0", config={
        "model": {
            "custom_model": "my_model",
            "custom_options": {},  # extra options to pass to your model
        },
    })

For a full example of a custom model in code, see the `Carla RLlib model <https://github.com/ray-project/ray/blob/master/python/ray/rllib/examples/carla/models.py>`__ and associated `training scripts <https://github.com/ray-project/ray/tree/master/python/ray/rllib/examples/carla>`__. You can also reference the `unit tests <https://github.com/ray-project/ray/blob/master/python/ray/rllib/test/test_nested_spaces.py>`__ for Tuple and Dict spaces, which show how to access nested observation fields.

Custom Recurrent Models
~~~~~~~~~~~~~~~~~~~~~~~

Instead of using the ``use_lstm: True`` option, it can be preferable use a custom recurrent model. This provides more control over postprocessing of the LSTM output and can also allow the use of multiple LSTM cells to process different portions of the input. The only difference from a normal custom model is that you have to define ``self.state_init``, ``self.state_in``, and ``self.state_out``. You can refer to the existing `lstm.py <https://github.com/ray-project/ray/blob/master/python/ray/rllib/models/lstm.py>`__ model as an example to implement your own model:

.. code-block:: python

    class MyCustomLSTM(Model):
        def _build_layers_v2(self, input_dict, num_outputs, options):
            # Some initial layers to process inputs, shape [BATCH, OBS...].
            features = some_hidden_layers(input_dict["obs"])

            # Add back the nested time dimension for tf.dynamic_rnn, new shape
            # will be [BATCH, MAX_SEQ_LEN, OBS...].
            last_layer = add_time_dimension(features, self.seq_lens)

            # Setup the LSTM cell (see lstm.py for an example)
            lstm = rnn.BasicLSTMCell(256, state_is_tuple=True)
            self.state_init = ...
            self.state_in = ...
            lstm_out, lstm_state = tf.nn.dynamic_rnn(
                lstm,
                last_layer,
                initial_state=...,
                sequence_length=self.seq_lens,
                time_major=False,
                dtype=tf.float32)
            self.state_out = list(lstm_state)

            # Drop the time dimension again so back to shape [BATCH, OBS...].
            # Note that we retain the zero padding (see issue #2992).
            last_layer = tf.reshape(lstm_out, [-1, cell_size])
            logits = linear(last_layer, num_outputs, "action",
                            normc_initializer(0.01))
            return logits, last_layer

Batch Normalization
~~~~~~~~~~~~~~~~~~~

You can use ``tf.layers.batch_normalization(x, training=input_dict["is_training"])`` to add batch norm layers to your custom model: `code example <https://github.com/ray-project/ray/blob/master/python/ray/rllib/examples/batch_norm_model.py>`__. RLlib will automatically run the update ops for the batch norm layers during optimization (see `tf_policy_graph.py <https://github.com/ray-project/ray/blob/master/python/ray/rllib/evaluation/tf_policy_graph.py>`__ and `multi_gpu_impl.py <https://github.com/ray-project/ray/blob/master/python/ray/rllib/optimizers/multi_gpu_impl.py>`__ for the exact handling of these updates).

Custom Preprocessors
--------------------

Similarly, custom preprocessors should subclass the RLlib `preprocessor class <https://github.com/ray-project/ray/blob/master/python/ray/rllib/models/preprocessors.py>`__ and be registered in the model catalog. Note that you can alternatively use `gym wrapper classes <https://github.com/openai/gym/tree/master/gym/wrappers>`__ around your environment instead of preprocessors.

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
    agent = ppo.PPOAgent(env="CartPole-v0", config={
        "model": {
            "custom_preprocessor": "my_prep",
            "custom_options": {},  # extra options to pass to your preprocessor
        },
    })


Customizing Policy Graphs
-------------------------

For deeper customization of algorithms, you can modify the policy graphs of the agent classes. Here's an example of extending the DDPG policy graph to specify custom sub-network modules:

.. code-block:: python

    from ray.rllib.models import ModelCatalog
    from ray.rllib.agents.ddpg.ddpg_policy_graph import DDPGPolicyGraph as BaseDDPGPolicyGraph

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

    class CustomDDPGPolicyGraph(BaseDDPGPolicyGraph):
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

Then, you can create an agent with your custom policy graph by:

.. code-block:: python

    from ray.rllib.agents.ddpg.ddpg import DDPGAgent
    from custom_policy_graph import CustomDDPGPolicyGraph

    DDPGAgent._policy_graph = CustomDDPGPolicyGraph
    agent = DDPGAgent(...)

In this example we overrode existing methods of the existing DDPG policy graph, i.e., `_build_q_network`, `_build_p_network`, `_build_action_network`, `_build_actor_critic_loss`, but you can also replace the entire graph class entirely.

Variable-length / Parametric Action Spaces
------------------------------------------

Custom models can be used to work with environments where (1) the set of valid actions varies per step, and/or (2) the number of valid actions is very large, as in `OpenAI Five <https://neuro.cs.ut.ee/the-use-of-embeddings-in-openai-five/>`__ and `Horizon <https://arxiv.org/abs/1811.00260>`__. The general idea is that the meaning of actions can be completely conditioned on the observation, i.e., the ``a`` in ``Q(s, a)`` becomes just a token in ``[0, MAX_AVAIL_ACTIONS)`` that only has meaning in the context of ``s``. This works with algorithms in the `DQN and policy-gradient families <rllib-env.html>`__ and can be implemented as follows:

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


Model-Based Rollouts
--------------------

With a custom policy graph, you can also perform model-based rollouts and optionally incorporate the results of those rollouts as training data. For example, suppose you wanted to extend PGPolicyGraph for model-based rollouts. This involves overriding the ``compute_actions`` method of that policy graph:

.. code-block:: python

        class ModelBasedPolicyGraph(PGPolicyGraph):
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


If you want take this rollouts data and append it to the sample batch, use the ``add_extra_batch()`` method of the `episode objects <https://github.com/ray-project/ray/blob/master/python/ray/rllib/evaluation/episode.py>`__ passed in. For an example of this, see the ``testReturningModelBasedRolloutsData`` `unit test <https://github.com/ray-project/ray/blob/master/python/ray/rllib/test/test_multi_agent_env.py>`__.
