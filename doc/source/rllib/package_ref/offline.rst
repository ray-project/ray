.. _offline-reference-docs:

Offline RL
==========

InputReaders
++++++++++++

The InputReader API is used by individual RolloutWorkers to produce batches of experiences
either from an simulator/environment or from an offline source (e.g. a file).
Here, we introduce the generic API and its children used for reading offline data (for offline RL).
For details on RLlib's sampler implementations (when collecting data
from simulators/environments), see the :ref:`Sampler docs here<sampler-docs>`.

.. autoclass:: ray.rllib.offline.input_reader.InputReader
    :members:


JsonReader (ray.rllib.offline.json_reader.JsonReader)
-----------------------------------------------------

For reading data from offline files (for example when no simulator/environment is available),
you can use the built-in JsonReader class. You will have to change the "input" config setting
from "sampler" (default) to a JSON file name (str) or a list of JSON files.
Alternatively, you can specify a callable that returns a new InputReader object.

.. autoclass:: ray.rllib.offline.json_reader.JsonReader
    :special-members: __init__
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

.. autoclass:: ray.rllib.offline.mixed_input.MixedInput
    :special-members: __init__
    :members:


D4RLReader (ray.rllib.offline.d4rl_reader.D4RLReader)
-----------------------------------------------------

.. autoclass:: ray.rllib.offline.d4rl_reader.D4RLReader
    :special-members: __init__
    :members:
