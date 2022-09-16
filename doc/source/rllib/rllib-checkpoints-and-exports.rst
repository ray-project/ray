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

Let's take a look at a simple example on how to create such an
Algorithm checkpoint:

.. literalinclude:: ../../../rllib/examples/documentation/checkpoints_and_exports.py
    :language: python
    :start-after: __create-algo-checkpoint-begin__
    :end-before: __create-algo-checkpoint-end__

If you take a look at the directory returned by the ``save()`` call, you should see something
like this:


.. code-block:: shell

    $ ls -la
      .
      ..
      .is_checkpoint
      .tune_metadata
      policies
      state.pkl

As you can see, there is a `policies` sub-directory created for us (more on that
later) and a ``state.pkl`` file. The ``state.pkl`` file contains all state information
of the Algorithm that is **not** Policy-specific, such as the algo's counters and
other important variables to persistently keep track of.

Let's check out the `policies/` sub-directory:

.. code-block:: shell

    $ cd policies
    $ ls -la
      .
      ..
      default_policy

We can see yet another sub-directory, called ``default_policy``. RLlib creates
exactly one sub-directory inside the ``policies/`` dir per Policy instance that
the Algorithm uses. In the standard single-agent case, this will be the
"default_policy". Note here, that "default_policy" is the so-called PolicyID.
In the multi-agent case, depending on your particular setup and environment,
you might see multiple sub-directories here with different names (the PolicyIDs of
the different policies trained). For example, if you are training 2 Policies
with the IDs "policy_1" and "policy_2", you should see the sub-directories:

.. code-block:: shell

    $ ls -la
      .
      ..
      policy_1
      policy_2

Lastly, let's quickly take a look at our ``default_policy`` sub-directory:

.. code-block:: shell

    $ cd default_policy
    $ ls -la
      .
      ..
      policy_state.pkl

Similar to the algorithm's state (saved within ``state.pkl``),
a Policy's state is stored under the ``policy_state.pkl`` file. We'll cover more
details on the contents of this file when talking about Policy checkpoints below.


How do I restore from an Algorithm Checkpoint?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Given our checkpoint path (returned by ``Algorithm.save()``), we can now
create a completely new Algorithm instance and make it the exact same as the one we
had stopped (and could thus no longer use) in the example above:

.. literalinclude:: ../../../rllib/examples/documentation/checkpoints_and_exports.py
    :language: python
    :start-after: __restore-from-algo-checkpoint-begin__
    :end-before: __restore-from-algo-checkpoint-end__


Alternatively, you could also first create a new Algorithm instance using the
same config that you used for the original algo, and only then call the new
Algorithm's ``restore()`` method, passing it the checkpoint directory:

.. literalinclude:: ../../../rllib/examples/documentation/checkpoints_and_exports.py
    :language: python
    :start-after: __restore-from-algo-checkpoint-2-begin__
    :end-before: __restore-from-algo-checkpoint-2-end__

The above procedure used to be the only way of restoring an algo, however, it is more tedious
than using the ``from_checkpoint()`` utility as it requires an extra step and you will
have to keep your original config stored somewhere.


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
