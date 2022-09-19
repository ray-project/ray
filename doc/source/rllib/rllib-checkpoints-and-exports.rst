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
      policies/
      state.pkl
      checkpoint_version.txt

As you can see, there is a `policies` sub-directory created for us (more on that
later), a ``state.pkl`` file, and a ``checkpoint_version.txt`` file.
The ``state.pkl`` file contains all state information
of the Algorithm that is **not** Policy-specific, such as the algo's counters and
other important variables to persistently keep track of.
The ``checkpoint_version.txt`` file contains the checkpoint version used for the user's
convenience. From Ray RLlib 2.0 and up, all checkpoint versions will be
backward compatible, meaning an RLlib version ``V`` will be able to
handle any checkpoints created with 2.0 or any version up to ``V``.

.. code-block:: shell

    $ mode checkpoint_version.txt
    v1

Now, let's check out the `policies/` sub-directory:

.. code-block:: shell

    $ cd policies
    $ ls -la
      .
      ..
      default_policy/

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
      policy_1/
      policy_2/


Lastly, let's quickly take a look at our ``default_policy`` sub-directory:

.. code-block:: shell

    $ cd default_policy
    $ ls -la
      .
      ..
      checkpoint_version.txt
      policy_state.pkl

Similar to the algorithm's state (saved within ``state.pkl``),
a Policy's state is stored under the ``policy_state.pkl`` file. We'll cover more
details on the contents of this file when talking about Policy checkpoints below.
Note that Policy checkpoint also have a version information
(in ``checkpoint_version.txt``), which is always identical to the enclosing
algorithm checkpoint version.


How do I restore an Algorithm from a Checkpoint?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

RLlib uses simple checkpoint versions (for example "v0" or "v1") to figure
out how to restore an Algorithm (or a Policy; see below) from a given
checkpoint directory.

From Ray 2.1 on, you can find the checkpoint version written in the
``checkpoint_version.txt`` file at the top-level of your checkpoint directory.
RLlib does not use this file or information therein, it solely exists for the
user's convenience.

From Ray RLlib 2.0 and up, all checkpoint versions will be
backward compatible, meaning some RLlib version 2.x will be able to
handle any checkpoints created by RLlib 2.0 or any version up to 2.x.


Multi-agent Algorithm Checkpoints
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In case you are working with a multi-agent setup and have more than one
Policy to train inside your Algorithm,

.. literalinclude:: ../../../rllib/examples/documentation/checkpoints_and_exports.py
    :language: python
    :start-after: __multi-agent-checkpoints-begin__
    :end-before: __multi-agent-checkpoints-end__

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
