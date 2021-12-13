.. _sample-batch-reference-docs:

Sample Batches
==============

SampleBatch (ray.rllib.policy.sample_batch.SampleBatch)
-------------------------------------------------------

Whether running in a single process or `large cluster <rllib-training.html#specifying-resources>`__,
all data interchange in RLlib happens in the form of :py:class:`~ray.rllib.policy.sample_batch.SampleBatch`es.
They encode one or more fragments of a trajectory. Typically, one :py:class:`~ray.rllib.evaluation.rollout_worker.RolloutWorker` collects batches of size
``rollout_fragment_length``, and RLlib then concatenates one or more of these batches (across different
:py:class:`~ray.rllib.evaluation.rollout_worker.RolloutWorker`s or
from the same :py:class:`~ray.rllib.evaluation.rollout_worker.RolloutWorker` in subsequent sampling steps) into a batch of size
``train_batch_size``, which then serves as the input to a :py:class:`~ray.rllib.policy.policy.Policy`'s ``learn_on_batch()`` method.

A typical sample batch looks something like the following when summarized.
Since all values are kept in arrays, this allows for efficient encoding and transmission
across the network:

.. code-block:: python

 { 'action_logp': np.ndarray((200,), dtype=float32, min=-0.701, max=-0.685, mean=-0.694),
   'actions': np.ndarray((200,), dtype=int64, min=0.0, max=1.0, mean=0.495),
   'dones': np.ndarray((200,), dtype=bool, min=0.0, max=1.0, mean=0.055),
   'infos': np.ndarray((200,), dtype=object, head={}),
   'new_obs': np.ndarray((200, 4), dtype=float32, min=-2.46, max=2.259, mean=0.018),
   'obs': np.ndarray((200, 4), dtype=float32, min=-2.46, max=2.259, mean=0.016),
   'rewards': np.ndarray((200,), dtype=float32, min=1.0, max=1.0, mean=1.0),
   't': np.ndarray((200,), dtype=int64, min=0.0, max=34.0, mean=9.14)}


.. autoclass:: ray.rllib.policy.sample_batch.SampleBatch
    :members:


MultiAgentBatch (ray.rllib.policy.sample_batch.MultiAgentBatch)
---------------------------------------------------------------

In `multi-agent mode <rllib-concepts.html#policies-in-multi-agent>`__, several sample batches may be
collected separately for each individual policy and are placed in a container object of type :py:class:`~ray.rllib.policy.sample_batch.MultiAgentBatch`:

.. autoclass:: ray.rllib.policy.sample_batch.MultiAgentBatch
    :members:
