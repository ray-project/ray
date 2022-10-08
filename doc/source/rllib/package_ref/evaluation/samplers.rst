.. _sampler-docs:

Environment Samplers
====================

When a simulator (environment) is available, :py:class:`~ray.rllib.evaluation.sampler.Sampler`s - child classes
of :py:class:`~ray.rllib.offline.input_reader.InputReader` - are used to collect and return experiences from the envs.
For more details on :py:class:`~ray.rllib.offline.input_reader.InputReader` used for offline RL (e.g. reading files of
pre-recorded data), see the :ref:`offline RL API reference here <offline-reference-docs>`.

The base sampler API (SamplerInput) is defined as follows:

Base Sampler class (ray.rllib.evaluation.sampler.SamplerInput)
--------------------------------------------------------------

.. autoclass:: ray.rllib.evaluation.sampler.SamplerInput
    :members:

SyncSampler (ray.rllib.evaluation.sampler.SyncSampler)
------------------------------------------------------

The synchronous sampler starts stepping through and collecting samples from an
environment only when its ``next()`` method is called. Calling this method blocks
until a :py:class:`~ray.rllib.policy.sample_batch.SampleBatch` has been built and is returned.

.. autoclass:: ray.rllib.evaluation.sampler.SyncSampler
    :special-members: __init__
    :members:

AsyncSampler (ray.rllib.evaluation.sampler.AsyncSampler)
--------------------------------------------------------

The asynchronous sampler has a separate thread that keeps stepping through and
collecting samples from an environment in the background. Calling its ``next()`` method
gets the next enqueued :py:class:`~ray.rllib.policy.sample_batch.SampleBatch` from a queue and returns it immediately.

.. autoclass:: ray.rllib.evaluation.sampler.AsyncSampler
    :special-members: __init__
    :members:
