RLlib Sample Collection
=======================

Sample Collector API
--------------------

When using a RolloutWorker with the `sampler` property set (i.e. the RolloutWorker
has to collect trajectories from a live environment), the Sampler object will use
a child class of `SampleCollector` to store and manage all collected data (from the env,
from the model, and the additional collection stats such as timesteps, agent index, etc..).



~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

    Some note

test text

.. code-block:: bash

    $ rllib train
        --run=PG \
        --env=CartPole-v0 \
        --config='{"output": "/tmp/cartpole-out", "output_max_file_size": 5000000}' \
        --stop='{"timesteps_total": 100000}'


**bold text**


Trajectory View API
-------------------

The trajectory view API is a common format by which Models and Policies communicate
to each other as well as to the SampleCollector, which data to store and
how to present it as input to the Policy's different method. In particular, these
are a) compute_actions, b) postprocess_trajectory, and c) learn_on_batch
(loss functions).
The data can stem from either the environment (observations, rewards, and infos),
the model itself (actions, state outputs, action-probs, etc..) or the Sampler (e.g.
agent index, env ID, episode ID, timestep, etc..).
The idea is to allow more flexibility in how a model can get a view on the ongoing
or past episode trajectories and the introduction of such a concept is helpful with
more complex model setups like RNNs, attention nets, observation image framestacking,
and multi-agent communication channels.
