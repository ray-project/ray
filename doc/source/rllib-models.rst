RLlib Models and Preprocessors
==============================

The following diagram provides a conceptual overview of data flow between different components in RLlib. We start with an ``Environment``, which given an action produces an observation. The observation is preprocessed by a ``Preprocessor`` and ``Filter`` (e.g. for running mean normalization) before being sent to a neural network ``Model``. The model output is in turn interpreted by an ``ActionDistribution`` to determine the next action.

.. image:: rllib-components.svg

The components highlighted in green can be replaced with custom user-defined implementations, as described in the next sections. The purple components are RLlib internal, which means they can only be modified by changing the algorithm source code.


Built-in Models and Preprocessors
---------------------------------

RLlib picks default models based on a simple heuristic: a `vision network <https://github.com/ray-project/ray/blob/master/python/ray/rllib/models/visionnet.py>`__ for image observations, and a `fully connected network <https://github.com/ray-project/ray/blob/master/python/ray/rllib/models/fcnet.py>`__ for everything else. These models can be configured via the ``model`` config key, documented in the model `catalog <https://github.com/ray-project/ray/blob/master/python/ray/rllib/models/catalog.py>`__. Note that you'll probably have to configure ``conv_filters`` if your environment observations have custom sizes, e.g., ``"model": {"dim": 42, "conv_filters": [[16, [4, 4], 2], [32, [4, 4], 2], [512, [11, 11], 1]]}`` for 42x42 observations.

In addition, if you set ``"model": {"use_lstm": true}``, then the model output will be further processed by a `LSTM cell <https://github.com/ray-project/ray/blob/master/python/ray/rllib/models/lstm.py>`__. More generally, RLlib supports the use of recurrent models for its algorithms (A3C, PG out of the box), and RNN support is built into its policy evaluation utilities.

For preprocessors, RLlib tries to pick one of its built-in preprocessor based on the environment's observation space. Discrete observations are one-hot encoded, Atari observations downscaled, and Tuple observations flattened (there isn't native tuple support yet, but you can reshape the flattened observation in a custom model). Note that for Atari, DQN defaults to using the `DeepMind preprocessors <https://github.com/ray-project/ray/blob/master/python/ray/rllib/env/atari_wrappers.py>`__, which are also used by the OpenAI baselines library.


Custom Models
-------------

Custom models should subclass the common RLlib `model class <https://github.com/ray-project/ray/blob/master/python/ray/rllib/models/model.py>`__ and override the ``_build_layers`` method. This method takes in a tensor input (observation), and returns a feature layer and float vector of the specified output size. The model can then be registered and used in place of a built-in model:

.. code-block:: python

    import ray
    import ray.rllib.agents.ppo as ppo
    from ray.rllib.models import ModelCatalog, Model

    class MyModelClass(Model):
        def _build_layers(self, inputs, num_outputs, options):
            layer1 = slim.fully_connected(inputs, 64, ...)
            layer2 = slim.fully_connected(inputs, 64, ...)
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

For a full example of a custom model in code, see the `Carla RLlib model <https://github.com/ray-project/ray/blob/master/examples/carla/models.py>`__ and associated `training scripts <https://github.com/ray-project/ray/tree/master/examples/carla>`__. The ``CarlaModel`` class defined there operates over a composite (Tuple) observation space including both images and scalar measurements.

Custom Preprocessors
--------------------

Similarly, custom preprocessors should subclass the RLlib `preprocessor class <https://github.com/ray-project/ray/blob/master/python/ray/rllib/models/preprocessors.py>`__ and registered in the model catalog:

.. code-block:: python

    import ray
    import ray.rllib.agents.ppo as ppo
    from ray.rllib.models.preprocessors import Preprocessor

    class MyPreprocessorClass(Preprocessor):
        def _init(self):
            self.shape = ...  # perhaps varies depending on self._options 

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
