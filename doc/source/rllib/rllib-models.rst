

Built-in Models
~~~~~~~~~~~~~~~

After preprocessing (if applicable) the raw environment outputs, the processed observations are fed through the policy's model.
In case, no custom model is specified (see further below on how to customize models), RLlib will pick a default model
based on simple heuristics:

- A vision network (`TF <https://github.com/ray-project/ray/blob/master/rllib/models/tf/visionnet.py>`__ or `Torch <https://github.com/ray-project/ray/blob/master/rllib/models/torch/visionnet.py>`__)
  for observations that have a shape of length larger than 2, for example, ``(84 x 84 x 3)``.
- A fully connected network (`TF <https://github.com/ray-project/ray/blob/master/rllib/models/tf/fcnet.py>`__ or `Torch <https://github.com/ray-project/ray/blob/master/rllib/models/torch/fcnet.py>`__)
  for everything else.

These default model types can further be configured via the ``model`` config key inside your Algorithm config (as discussed above).
Available settings are `listed above <#default-model-config-settings>`__ and also documented in the `model catalog file <https://github.com/ray-project/ray/blob/master/rllib/models/catalog.py>`__.

Note that for the vision network case, you'll probably have to configure ``conv_filters``, if your environment observations
have custom sizes. For example, ``"model": {"dim": 42, "conv_filters": [[16, [4, 4], 2], [32, [4, 4], 2], [512, [11, 11], 1]]}`` for 42x42 observations.
Thereby, always make sure that the last Conv2D output has an output shape of ``[B, 1, 1, X]`` (``[B, X, 1, 1]`` for PyTorch), where B=batch and
X=last Conv2D layer's number of filters, so that RLlib can flatten it. An informative error will be thrown if this isn't the case.


.. _auto_lstm_and_attention:

Built-in auto-LSTM, and auto-Attention Wrappers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In addition, if you set ``"use_lstm": True`` or ``"use_attention": True`` in your model config,
your model's output will be further processed by an LSTM cell
(`TF <https://github.com/ray-project/ray/blob/master/rllib/models/tf/recurrent_net.py>`__ or `Torch <https://github.com/ray-project/ray/blob/master/rllib/models/torch/recurrent_net.py>`__),
or an attention (`GTrXL <https://arxiv.org/abs/1910.06764>`__) network
(`TF <https://github.com/ray-project/ray/blob/master/rllib/models/tf/attention_net.py>`__ or
`Torch <https://github.com/ray-project/ray/blob/master/rllib/models/torch/attention_net.py>`__), respectively.
More generally, RLlib supports the use of recurrent/attention models for all
its policy-gradient algorithms (A3C, PPO, PG, IMPALA), and the necessary sequence processing support
is built into its policy evaluation utilities.

See above for which additional config keys to use to configure in more detail these two auto-wrappers
(e.g. you can specify the size of the LSTM layer by ``lstm_cell_size`` or the attention dim by ``attention_dim``).

For fully customized RNN/LSTM/Attention-Net setups see the `Recurrent Models <#rnns>`_ and
`Attention Networks/Transformers <#attention>`_ sections below.

.. note::
    It isn't possible to use both auto-wrappers (lstm and attention) at the same time. Doing so will create an error.




Variable-length / Complex Observation Spaces
--------------------------------------------

RLlib supports complex and variable-length observation spaces, including ``gym.spaces.Tuple``, ``gym.spaces.Dict``, and ``rllib.utils.spaces.Repeated``. The handling of these spaces is transparent to the user. RLlib internally will insert preprocessors to insert padding for repeated elements, flatten complex observations into a fixed-size vector during transit, and unpack the vector into the structured tensor before sending it to the model. The flattened observation is available to the model as ``input_dict["obs_flat"]``, and the unpacked observation as ``input_dict["obs"]``.

To enable batching of struct observations, RLlib unpacks them in a `StructTensor-like format <https://github.com/tensorflow/community/blob/master/rfcs/20190910-struct-tensor.md>`__. In summary, repeated fields are "pushed down" and become the outer dimensions of tensor batches, as illustrated in this figure from the StructTensor RFC.

.. image:: images/struct-tensor.png

For further information about complex observation spaces, see:
  * A custom environment and model that uses `repeated struct fields <https://github.com/ray-project/ray/blob/master/rllib/examples/complex_struct_space.py>`__.
  * The pydoc of the `Repeated space <https://github.com/ray-project/ray/blob/master/rllib/utils/spaces/repeated.py>`__.
  * The pydoc of the batched `repeated values tensor <https://github.com/ray-project/ray/blob/master/rllib/models/repeated_values.py>`__.
  * The `unit tests <https://github.com/ray-project/ray/blob/master/rllib/tests/test_nested_observation_spaces.py>`__ for Tuple and Dict spaces.

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


Depending on your use case it may make sense to use |just the masking|_, |just action embeddings|_, or |both|_.  For a runnable example of "just action embeddings" in code,
check out `examples/parametric_actions_cartpole.py <https://github.com/ray-project/ray/blob/master/rllib/examples/parametric_actions_cartpole.py>`__.

.. |just the masking| replace:: just the **masking**
.. _just the masking: https://github.com/ray-project/ray/blob/master/rllib/examples/_old_api_stack/models/action_mask_model.py
.. |just action embeddings| replace:: just action **embeddings**
.. _just action embeddings: https://github.com/ray-project/ray/blob/master/rllib/examples/parametric_actions_cartpole.py
.. |both| replace:: **both**
.. _both: https://github.com/ray-project/ray/blob/master/rllib/examples/_old_api_stack/models/parametric_actions_model.py

Note that since masking introduces ``tf.float32.min`` values into the model output, this technique might not work with all algorithm options. For example, algorithms might crash if they incorrectly process the ``tf.float32.min`` values. The cartpole example has working configurations for DQN (must set ``hiddens=[]``), PPO (must disable running mean and set ``model.vf_share_layers=True``), and several other algorithms. Not all algorithms support parametric actions; see the `algorithm overview <rllib-algorithms.html#available-algorithms-overview>`__.
