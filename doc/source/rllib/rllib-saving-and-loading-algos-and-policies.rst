.. include:: /_includes/rllib/announcement.rst

.. include:: /_includes/rllib/we_are_hiring.rst

.. _rllib-saving-and-loading-algos-and-policies-docs:

##################################################
Saving and Loading your RL Algorithms and Policies
##################################################


You can use :py:class:`~ray.air.checkpoint.Checkpoint` objects to store
and load the current state of your :py:class:`~ray.rllib.algorithms.algorithm.Algorithm`
or :py:class:`~ray.rllib.policy.policy.Policy` and the neural networks (weights)
within these structures. In the following, we will cover how you can create these
checkpoints (and hence save your Algos and Policies) to disk, where you can find them,
and how you can recover (load) your :py:class:`~ray.rllib.algorithms.algorithm.Algorithm`
or :py:class:`~ray.rllib.policy.policy.Policy` from such a given checkpoint.


What's a checkpoint?
====================

A checkpoint is a set of information, located inside a directory (which may contain
further subdirectories) and used to restore either an :py:class:`~ray.rllib.algorithms.algorithm.Algorithm`
or a single :py:class:`~ray.rllib.policy.policy.Policy` instance.
The Algorithm- or Policy instances that were used to create the checkpoint in the first place
may or may not have been trained prior to this.

RLlib uses the new Ray AIR :py:class:`~ray.air.checkpoint.Checkpoint` class to create checkpoints and
restore objects from them.


Algorithm checkpoints
=====================

An Algorithm checkpoint contains all of the Algorithm's state, including its configuration,
its actual Algorithm subclass, all of its Policies' weights, its current counters, etc..

Restoring a new Algorithm from such a Checkpoint leaves you in a state, where you can continue
working with that new Algorithm exactly like you would have continued working with the
old Algorithm (from which the checkpoint as taken).

How do I create an Algorithm checkpoint?
----------------------------------------

The :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` ``save()`` method creates a new checkpoint
(directory with files in it) and returns the path to that directory.

Let's take a look at a simple example on how to create such an
Algorithm checkpoint:

.. literalinclude:: ../../../rllib/examples/documentation/saving_and_loading_algos_and_policies.py
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
      algorithm_state.pkl
      rllib_checkpoint.json

As you can see, there is a `policies` sub-directory created for us (more on that
later), a ``algorithm_state.pkl`` file, and a ``rllib_checkpoint.json`` file.
The ``algorithm_state.pkl`` file contains all state information
of the Algorithm that is **not** Policy-specific, such as the algo's counters and
other important variables to persistently keep track of.
The ``rllib_checkpoint.json`` file contains the checkpoint version used for the user's
convenience. From Ray RLlib 2.0 and up, all checkpoint versions will be
backward compatible, meaning an RLlib version ``V`` will be able to
handle any checkpoints created with Ray 2.0 or any version up to ``V``.

.. code-block:: shell

    $ more rllib_checkpoint.json
    {"type": "Algorithm", "checkpoint_version": "1.0"}

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
      rllib_checkpoint.json
      policy_state.pkl

Similar to the algorithm's state (saved within ``algorithm_state.pkl``),
a Policy's state is stored under the ``policy_state.pkl`` file. We'll cover more
details on the contents of this file when talking about :py:class:`~ray.rllib.policy.policy.Policy` checkpoints below.
Note that :py:class:`~ray.rllib.policy.policy.Policy` checkpoint also have a
info file (``rllib_checkpoint.json``), which is always identical to the enclosing
algorithm checkpoint version.


How do I restore an Algorithm from a checkpoint?
------------------------------------------------

Given our checkpoint path (returned by ``Algorithm.save()``), we can now
create a completely new Algorithm instance and make it the exact same as the one we
had stopped (and could thus no longer use) in the example above:

.. literalinclude:: ../../../rllib/examples/documentation/saving_and_loading_algos_and_policies.py
    :language: python
    :start-after: __restore-from-algo-checkpoint-begin__
    :end-before: __restore-from-algo-checkpoint-end__


Alternatively, you could also first create a new Algorithm instance using the
same config that you used for the original algo, and only then call the new
Algorithm's ``restore()`` method, passing it the checkpoint directory:

.. literalinclude:: ../../../rllib/examples/documentation/saving_and_loading_algos_and_policies.py
    :language: python
    :start-after: __restore-from-algo-checkpoint-2-begin__
    :end-before: __restore-from-algo-checkpoint-2-end__

The above procedure used to be the only way of restoring an algo, however, it is more tedious
than using the ``from_checkpoint()`` utility as it requires an extra step and you will
have to keep your original config stored somewhere.


Which Algorithm checkpoint versions can I use?
----------------------------------------------

RLlib uses simple checkpoint versions (for example v0.1 or v1.0) to figure
out how to restore an Algorithm (or a :py:class:`~ray.rllib.policy.policy.Policy`;
see below) from a given
checkpoint directory.

From Ray 2.1 on, you can find the checkpoint version written in the
``rllib_checkpoint.json`` file at the top-level of your checkpoint directory.
RLlib does not use this file or information therein, it solely exists for the
user's convenience.

From Ray RLlib 2.0 and up, all checkpoint versions will be
backward compatible, meaning some RLlib version 2.x will be able to
handle any checkpoints created by RLlib 2.0 or any version up to 2.x.


Multi-agent Algorithm checkpoints
---------------------------------

In case you are working with a multi-agent setup and have more than one
:py:class:`~ray.rllib.policy.policy.Policy` to train inside your
:py:class:`~ray.rllib.algorithms.algorithm.Algorithm`, you
can create an :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` checkpoint in the
exact same way as described above and will find your individual
:py:class:`~ray.rllib.policy.policy.Policy` checkpoints
inside the sub-directory ``policies/``.

For example:

.. literalinclude:: ../../../rllib/examples/documentation/saving_and_loading_algos_and_policies.py
    :language: python
    :start-after: __multi-agent-checkpoints-begin__
    :end-before: __multi-agent-checkpoints-end__


Assuming you would like to restore all policies within the checkpoint, you would
do so just as described above in the single-agent case
(via ``algo = Algorithm.from_checkpoint([path to your multi-agent checkpoint])``).

However, there may be a situation where you have so many policies in your algorithm
(e.g. you are doing league-based training) and would like to restore a new Algorithm
instance from your checkpoint, but only include some of the original policies in this
new Algorithm object. In this case, you can also do:

.. literalinclude:: ../../../rllib/examples/documentation/saving_and_loading_algos_and_policies.py
    :language: python
    :start-after: __multi-agent-checkpoints-restore-policy-sub-set-begin__
    :end-before: __multi-agent-checkpoints-restore-policy-sub-set-end__


Policy checkpoints
------------------

We have already looked at the ``policies/`` sub-directory inside an
:py:class:`~ray.rllib.algorithms.algorithm.Algorithm` checkpoint dir
and learned that individual policies inside the
:py:class:`~ray.rllib.algorithms.algorithm.Algorithm` store all their state
information under their policy ID inside that sub-directory.
Thus, we now have the entire picture of a checkpoint:

.. code-block::

  .
  ..
  .is_checkpoint
  .tune_metadata

  algorithm_state.pkl        # <- state of the Algorithm (excluding Policy states)
  rllib_checkpoint.json      # <- checkpoint info, such as checkpoint version, e.g. "1.0"

  policies/
    policy_A/
      policy_state.pkl       # <- state of policy_A
      rllib_checkpoint.json  # <- checkpoint info, such as checkpoint version, e.g. "1.0"

    policy_B/
      policy_state.pkl       # <- state of policy_B
      rllib_checkpoint.json  # <- checkpoint info, such as checkpoint version, e.g. "1.0"


How do I create a Policy checkpoint?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can create a :py:class:`~ray.rllib.policy.policy.Policy` checkpoint by
either calling ``save()`` on your :py:class:`~ray.rllib.algorithms.algorithm.Algorithm`, which
will save each individual Policy's checkpoint under the ``policies/`` sub-directory as
described above or - if you need more fine-grained control - by doing the following:



.. literalinclude:: ../../../rllib/examples/documentation/saving_and_loading_algos_and_policies.py
    :language: python
    :start-after: __create-policy-checkpoint-begin__
    :end-before: __create-policy-checkpoint-end__

If you now check out the provided directory (``/tmp/my_policy_checkpoint/``), you
should see the following files in there:

.. code-block::

    .
    ..
    rllib_checkpoint.json   # <- checkpoint info, such as checkpoint version, e.g. "1.0"
    policy_state.pkl        # <- state of "pol1"


How do I restore from a Policy checkpoint?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Assume you would like to serve your trained policy(ies) in production and would therefore
like to use only the RLlib :py:class:`~ray.rllib.policy.policy.Policy` instance,
without all the other functionality that
normally comes with the :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` object, like different ``RolloutWorkers`` for collecting
training samples or for evaluation (both of which include RL environment copies), etc..

In this case, it would be quite useful if you had a way to restore just the
:py:class:`~ray.rllib.policy.policy.Policy`
from either a :py:class:`~ray.rllib.policy.policy.Policy` checkpoint or an
:py:class:`~ray.rllib.algorithms.algorithm.Algorithm` checkpoint, which - as we learned above -
contains all its Policies' checkpoints.

Here is how you can do this:

.. literalinclude:: ../../../rllib/examples/documentation/saving_and_loading_algos_and_policies.py
    :language: python
    :start-after: __restore-policy-begin__
    :end-before: __restore-policy-end__


How do I restore a multi-agent Algorithm with a subset of the original policies?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Imagine you have trained a multi-agent :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` with e.g. 100 different Policies and created
a checkpoint from this :py:class:`~ray.rllib.algorithms.algorithm.Algorithm`.
The checkpoint now includes 100 sub-directories in the
``policies/`` dir, named after the different policy IDs.

After careful evaluation of the different policies, you would like to restore the
:py:class:`~ray.rllib.algorithms.algorithm.Algorithm`
and continue training it, but only with a subset of the original 100 policies,
for example only with the policies, whose IDs are "polA" and "polB".

You can use the original checkpoint (with the 100 policies in it) and the
``Algorithm.from_checkpoint()`` utility to achieve this in an efficient way.

This example here shows this for five original policies that you would like reduce to
two policies:

.. literalinclude:: ../../../rllib/examples/documentation/saving_and_loading_algos_and_policies.py
    :language: python
    :start-after: __restore-algorithm-from-checkpoint-with-fewer-policies-begin__
    :end-before: __restore-algorithm-from-checkpoint-with-fewer-policies-end__

Note that we had to change our original ``policy_mapping_fn`` from one that maps
"agent0" to "pol0", "agent1" to "pol1", etc..
to a new function that maps our five agents to only the two remaining policies:
"agent0" and "agent1" to "pol0", all other agents to "pol1".


Model Exports
-------------

Apart from creating checkpoints for your RLlib objects (such as an RLlib
:py:class:`~ray.rllib.algorithms.algorithm.Algorithm` or
an individual RLlib :py:class:`~ray.rllib.policy.policy.Policy`), it may also be very useful
to only export your NN models in their native (non-RLlib dependent) format, for example
as a keras- or PyTorch model.
You could then use the trained NN models outside
of RLlib, e.g. for serving purposes in your production environments.

How do I export my NN Model?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

There are several ways of creating Keras- or PyTorch native model "exports".

Here is the example code that illustrates these:

.. literalinclude:: ../../../rllib/examples/documentation/saving_and_loading_algos_and_policies.py
    :language: python
    :start-after: __export-models-begin__
    :end-before: __export-models-end__

We can now export the Keras NN model (that our PPOTF1Policy inside the PPO Algorithm uses)
to disk ...

1) Using the Policy object:

.. literalinclude:: ../../../rllib/examples/documentation/saving_and_loading_algos_and_policies.py
    :language: python
    :start-after: __export-models-1-begin__
    :end-before: __export-models-1-end__

2) Via the Policy's checkpointing method:

.. literalinclude:: ../../../rllib/examples/documentation/saving_and_loading_algos_and_policies.py
    :language: python
    :start-after: __export-models-2-begin__
    :end-before: __export-models-2-end__

3) Via the Algorithm (Policy) checkpoint:

.. literalinclude:: ../../../rllib/examples/documentation/saving_and_loading_algos_and_policies.py
    :language: python
    :start-after: __export-models-3-begin__
    :end-before: __export-models-3-end__


And what about exporting my NN Models in ONNX format?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

RLlib also supports exporting your NN models in the ONNX format. For that, use the
:py:class:`~ray.rllib.policy.policy.Policy` ``export_model`` method, but provide the
extra ``onnx`` arg as follows:


.. literalinclude:: ../../../rllib/examples/documentation/saving_and_loading_algos_and_policies.py
    :language: python
    :start-after: __export-models-as-onnx-begin__
    :end-before: __export-models-as-onnx-end__
