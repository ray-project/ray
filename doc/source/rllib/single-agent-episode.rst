.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst

.. include:: /_includes/rllib/new_api_stack_component.rst


SingleAgentEpisode
==================

RLlib stores and transports all trajectory data in the form of
:py:class:`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` (single-agent case) or
:py:class:`~ray.rllib.env.multi_agent_episode.MultiAgentEpisode` (multi-agent case) objects.
Only immediately before a model forward pass does RLlib translate Episodes into a tensor batch
format (possibly moving the batch to the GPU).

The main advantage of collecting and moving around data in such a trajectory-as-a-whole format
as opposed to keeping data in batches of tensors is that episodes offer 360° visibility and full access
to the RL environment's history.
This means users can extract arbitrary pieces of information from these episodes to be further
processed by their custom pipelines and models.

Another advantage the more efficient memory footprint of episodes. For example, using tensor batch
format for an algorithm like DQN, one would have to store both observations and
next observations (to compute the TD error-based loss) in your batch, thereby duplicating an
already large observation tensor. On the other hand, in episode objects one only has to
store a single observation-track containing all observations from reset to terminal.

This page explains in detail what working with RLlib's Episode APIs looks like.



Creating a SingleAgentEpisode
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Even in heavily customized user setups, RLlib usually takes care of creating :py:class:`~ray.rllib.env.single_agent_episode.SingleAgentEpisode`
and moving them around (for example from :py:class:`~ray.rllib.env.env_runner.EnvRunner` to :py:class:`~ray.rllib.core.learner.learner.Learner`).

Nevertheless, here is how to generate and fill an initially empty episode with dummy data:

.. literalinclude:: doc_code/sa_episode.py
    :language: python
    :start-after: rllib-sa-episode-01-begin
    :end-before: rllib-sa-episode-01-end

The :py:class:`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` should now look like this:

.. figure:: images/episodes/sa_episode.svg
    :width: 650

    **SingleAgentEpisode**: The episode starts with a single observation (the "reset observation"), then
    continues with each timestep with a 3-tuple of: observation, action, reward. Note that because of the reset observation,
    every episode - at each timestep - always contains one more observation than it contains actions or rewards.


Using the getter APIs
~~~~~~~~~~~~~~~~~~~~~

Now that there is a :py:class:`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` to work with, one can explore
and extract information from this episode using different "getter" APIs:

.. figure:: images/episodes/sa_episode_getters.svg
    :width: 650

    **SingleAgentEpisode getter APIs**: "getter" methods exist for all of the Episode's fields, which are "observations",
    "actions", "rewards", "infos", and "extra_model_outputs". For simplicity, only the getters for observations, actions, and rewards
    are shown here. Their behavior is intuitive, returning a single item when provided with a single index and a list of items
    when provided with a list of indices or a slice (range) of indices.


.. literalinclude:: doc_code/sa_episode.py
    :language: python
    :start-after: rllib-sa-episode-02-begin
    :end-before: rllib-sa-episode-02-end
