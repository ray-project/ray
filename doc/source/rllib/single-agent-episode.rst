.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst

.. include:: /_includes/rllib/new_api_stack_component.rst


Episodes
========

RLlib stores and transports all trajectory data in the form of episode objects, which come in two
different flavors: :py:class:`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` for single-agent setups
and :py:class:`~ray.rllib.env.multi_agent_episode.MultiAgentEpisode` for multi-agent setups.
RLlib translates from Episodes to tensor batches (including a possible move to the GPU)
only immediately before a neural network forward pass.

.. figure:: images/episodes/sa_episode.svg
    :width: 750

    **(Single-agent) Episode**: The episode starts with a single observation (the "reset observation"), then
    continues with each timestep with a 3-tuple of: observation, action, reward. Note that because of the reset observation,
    every episode - at each timestep - always contains one more observation than it contains actions or rewards.
    Important additional properties of an Episode are its `id_` (a string) and `terminated/truncated` (bool) flags.
    See further below for a detailed description of the different APIs the :py:class:`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` class
    exposes to the user.

The main advantage of collecting and moving around data in such a trajectory-as-a-whole format
as opposed to keeping data in tensor batches is that episodes offer 360° visibility and full access
to the RL environment's history. This means users can extract arbitrary pieces of information from episodes to be further
processed by their custom pipelines and models. Think of an attention model requiring not
only the current observation, but the whole sequence of the last n observations or an
(IMPALA-style) LSTM requiring the last reward and action in addition to the current observation.

Another advantage of episodes over batches is the more efficient memory footprint.
For example, an algorithm like DQN needs to have both observations and
next observations (to compute the TD error-based loss) in the train batch, thereby duplicating an
already large observation tensor, whereas episode objects only store
a single observation-track containing all observations from reset to terminal.

This page explains in detail what working with RLlib's Episode APIs looks like.


SingleAgentEpisode
==================

Creating a SingleAgentEpisode
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Even in heavily customized user setups, RLlib usually takes care of creating :py:class:`~ray.rllib.env.single_agent_episode.SingleAgentEpisode`
instances and moving them around, for example from :py:class:`~ray.rllib.env.env_runner.EnvRunner` to
:py:class:`~ray.rllib.core.learner.learner.Learner`.

Here is how to generate and fill an initially empty episode with dummy data:

.. literalinclude:: doc_code/sa_episode.py
    :language: python
    :start-after: rllib-sa-episode-01-begin
    :end-before: rllib-sa-episode-01-end

The :py:class:`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` should now look roughly like the
one in the figure above.


Using the getter APIs of SingleAgentEpisode
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Now that there is a :py:class:`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` to work with, one can explore
and extract information from this episode using different "getter" APIs:

.. figure:: images/episodes/sa_episode_getters.svg
    :width: 750

    **SingleAgentEpisode getter APIs**: "getter" methods exist for all of the Episode's fields, which are "observations",
    "actions", "rewards", "infos", and "extra_model_outputs". For simplicity, only the getters for observations, actions, and rewards
    are shown here. Their behavior is intuitive, returning a single item when provided with a single index and a list of items
    (in the non-finalized case; see further below) when provided with a list of indices or a slice (range) of indices.


.. literalinclude:: doc_code/sa_episode.py
    :language: python
    :start-after: rllib-sa-episode-02-begin
    :end-before: rllib-sa-episode-02-end



Finalized and Non-Finalized Episodes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The data in a :py:class:`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` can exist in two states:
non-finalized and finalized. A non-finalized episode stores its observations and other data items in
plain python lists and appends new timestep data to these. On the other hand, a finalized episode
has converted these lists into (possibly complex) structures that have numpy arrays as their leafs.
Note that a "finalized" episode doesn't necessarily have to be terminated or truncated
in the sense that the underlying RL environment declared the episode to be over (or has reached some
maximum number of timesteps).

.. figure:: images/episodes/sa_episode_non_finalized_vs_finalized.svg
    :width: 800

:py:class:`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` objects start in the non-finalized
state (data stored in lists), making it very fast to further add data to the episode, while for example
sampling from an RL environment. Once an episode is no longer expected to

