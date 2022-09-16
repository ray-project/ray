.. _checkpoints-and-exports-docs:

#################
Using Checkpoints
#################


What's a Checkpoint?
====================

A checkpoint is a set of information, located inside a directory (which may contain
further subdirectories) and used to restore either an Algorithm- or a single Policy instance.
The Algorithm- or Policy instances that were used to create the checkpoint in the first place
may or may not have been trained prior to this.

RLlib uses the new Ray AIR :py:class:`~ray.air.checkpoint.Checkpoint` class to create checkpoints and
restore objects from them.


Algorithm- vs Policy Checkpoints vs Model exports
=================================================

Algorithm Checkpoints
---------------------

An Algorithm checkpoint contains all of the Algorithm's state, including its configuration,
its actual Algorithm subclass, all of its Policies' weights, its current counters, etc..

Restoring a new Algorithm from such a Checkpoint leaves you in a state, where you can continue
working with that new Algorithm exactly like you would have continued working with the
old Algorithm (from which the checkpoint as taken).

How do I create an Algorithm Checkpoint?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` ``save()`` method creates a new checkpoint
(directory with files in it) and returns the path to that directory.

Let's take a look at a simple example on how to create such an Algorithm checkpoint:

.. code-block:: python

    # Create a PPO algorithm object ..
    from ray.rllib.algorithms.ppo import PPOConfig
    my_ppo = PPOConfig().environment(env="CartPole-v0").build()
    # .. train one iteration ..
    my_ppo.train()
    # .. and call `save()` to create a checkpoint.
    path_to_checkpoint = my_ppo.save()

    # Let's terminate the algo for demonstration purposes.
    my_ppo.stop()
    # Doing this will lead to an error.
    # my_ppo.train()


We have created


How do I restore from an Algorithm Checkpoint?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


Which Algorithm Checkpoint versions can I use?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


Multi-agent Algorithm Checkpoints
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

TODO: Restoring a multi-agent Algorithm, but only with some subset of the original policies


Policy Checkpoints
------------------

How do I create a Policy Checkpoint?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


How do I restore from a Policy Checkpoint?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~




Model Exports
-------------

How do I export my NN Model?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
