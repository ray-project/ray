.. _evaluation-docs:


RLlib's Evaluation/Rollout APIs
===============================


RolloutWorker
+++++++++++++

RolloutWorkers are used as ``@ray.remote`` actors to collect and return samples
from environments or offline files in parallel. An RLlib Trainer usually has
``num_workers`` RolloutWorkers plus a single "local" RolloutWorker (not ``@ray.remote``) in
its WorkerSet (see below) under ``self.workers``.
Depending on its evaluation config settings, an additional ``WorkerSet`` with
RolloutWorkers for evaluation may be present in the Trainer under ``self.evaluation_workers``.

.. autoclass:: ray.rllib.evaluation.rollout_worker.RolloutWorker
    :members:


WorkerSet
+++++++++

A set of RolloutWorkers containing n ``@ray.remote`` workers as well as a single "local"
RolloutWorker. Exposes some convenience methods to call methods on its individual workers in
parallel using e.g. ``ray.get()``.

.. autoclass:: ray.rllib.evaluation.worker_set.WorkerSet
    :members:


InputReaders
++++++++++++

The InputReader API is used by individual RolloutWorkers to produce batches of experiences
from an env or an offline source (e.g. a file):

.. autoclass:: ray.rllib.offline.input_reader.InputReader
    :members:

Base Sampler class (ray.rllib.evaluation.sampler.SamplerInput)
--------------------------------------------------------------

When a simulator (environment) is available, ``Samplers`` - child classes
of ``InputReader`` - are used to collect and return experiences from the envs.

.. autoclass:: ray.rllib.evaluation.sampler.SamplerInput
    :members:

SyncSampler (ray.rllib.evaluation.sampler.SyncSampler)
------------------------------------------------------

The synchronous sampler starts stepping through and collecting samples from an
environment only when its ``next()`` method is called. Calling this method blocks
until a ``SampleBatch`` has been built and is returned.

.. autoclass:: ray.rllib.evaluation.sampler.SyncSampler
    :members:

AsyncSampler (ray.rllib.evaluation.sampler.AsyncSampler)
--------------------------------------------------------

The asynchronous sampler has a separate thread that keeps stepping through and
collecting samples from an environment in the background. Calling its ``next()`` method
gets the next enqueued SampleBatch from a queue and returns it immediately.

.. autoclass:: ray.rllib.evaluation.sampler.AsyncSampler
    :members:

JsonReader (ray.rllib.offline.json_reader.JsonReader)
-----------------------------------------------------

For reading data from offline files (for example when no simulator/environment is available),
you can use the built-in JsonReader class. You will have to change the "input" config setting
from "sampler" (default) to a JSON file name (str) or a list of JSON files.
Alternatively, you can specify a callable that returns a new InputReader object.

.. autoclass:: ray.rllib.offline.json_reader.JsonReader
    :members:

MixedInput  (ray.rllib.offline.mixed_input.MixedInput)
------------------------------------------------------

In order to mix different input readers with each other in different custom ratios, you can use
the MixedInput reader. This reader is chosen automatically by RLlib when you provide a dict under
the "input" config key that maps input reader specifiers to probabilities, e.g.:

.. code-block:: python

    "input": {
       "sampler": 0.4,  # 40% of samples will come from environment
       "/tmp/experiences/*.json": 0.4,  # the rest from different JSON files
       "s3://bucket/expert.json": 0.2,
    }

D4RLReader (ray.rllib.offline.d4rl_reader.D4RLReader)
-----------------------------------------------------

.. autoclass:: ray.rllib.offline.d4rl_reader.D4RLReader
    :members:
