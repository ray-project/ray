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

Custom models should subclass the common RLlib `model class <https://github.com/ray-project/ray/blob/master/python/ray/rllib/models/model.py>`__ and override the ``_build_layers_v2`` method. This method takes in a dict of tensor inputs (the observation ``obs``, ``prev_action``, and ``prev_reward``), and returns a feature layer and float vector of the specified output size. The model can then be registered and used in place of a built-in model:

.. code-block:: python

    import ray
    import ray.rllib.agents.ppo as ppo
    from ray.rllib.models import ModelCatalog, Model

    class MyModelClass(Model):
        def _build_layers_v2(self, input_dict, num_outputs, options):
            """Define the layers of a custom model.

            Arguments:
                input_dict (dict): Dictionary of input tensors, including "obs",
                    "prev_action", "prev_reward".
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

    ModelCatalog.register_custom_model("my_model", MyModelClass)

    ray.init()
    agent = ppo.PPOAgent(env="CartPole-v0", config={
        "model": {
            "custom_model": "my_model",
            "custom_options": {},  # extra options to pass to your model
        },
    })

For a full example of a custom model in code, see the `Carla RLlib model <https://github.com/ray-project/ray/blob/master/python/ray/rllib/examples/carla/models.py>`__ and associated `training scripts <https://github.com/ray-project/ray/tree/master/python/ray/rllib/examples/carla>`__. You can also reference the `unit tests <https://github.com/ray-project/ray/blob/master/python/ray/rllib/test/test_nested_spaces.py>`__ for Tuple and Dict spaces, which show how to access nested observation fields.

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

Model-Based Rollouts
--------------------

With a custom policy graph, you can also perform model-based rollouts and optionally incorporate the results of those rollouts as training data. For example, suppose you wanted to extend PGPolicyGraph for model-based rollouts. This involves overriding the ``compute_actions`` method of that policy graph:

.. code-block:: python

        class ModelBasedPolicyGraph(PGPolicyGraph):
             def compute_actions(self,
                                 obs_batch,
                                 state_batches,
                                 is_training=False,
                                 episodes=None):
                # compute a batch of actions based on the current obs_batch
                # and state of each episode (i.e., for multiagent). You can do
                # whatever is needed here, e.g., MCTS rollouts.
                return action_batch


If you want take this rollouts data and append it to the sample batch, use the ``add_extra_batch()`` method of the `episode objects <https://github.com/ray-project/ray/blob/master/python/ray/rllib/evaluation/episode.py>`__ passed in. For an example of this, see the ``testReturningModelBasedRolloutsData`` `unit test <https://github.com/ray-project/ray/blob/master/python/ray/rllib/test/test_multi_agent_env.py>`__.
