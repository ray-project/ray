
.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst


.. _rllib-checkpointing-docs:

Checkpointing
=============

Overview
--------

RLlib offers a powerful checkpointing system for all its major classes, allowing you to save the
states of :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` instances and their subcomponents
to disk and loading previously run experiment states and individual subcomponents back from disk.
This enables continuing to train your models from a previous state or deploying bare-bones PyTorch
models into production.

.. figure:: images/checkpointing/save_and_restore.svg
    :width: 500
    :align: left

    **Saving to and restoring from disk**: Use the :py:meth:`~ray.rllib.utils.checkpoints.Checkpointable.save_to_path` method
    to write the current state of any :py:meth:`~ray.rllib.utils.checkpoints.Checkpointable` component or your entire Algorithm to disk.
    If you would like to load a saved state back into a running component or into your Algorithm, use
    the :py:meth:`~ray.rllib.utils.checkpoints.Checkpointable.restore_from_path` method.

A checkpoint is a directory. It contains meta data, such as the class and the constructor arguments for creating a new instance,
a pickle state file, and a human readable ``.json`` file with information about the Ray version and git commit of the checkpoint.

You can generate a new :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` instance or other subcomponent,
like an :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`, from an existing checkpoint using
the :py:meth:`~ray.rllib.utils.checkpoints.Checkpointable.from_checkpoint` method,
for example if you want to deploy a previously trained :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` - without
any of the other RLlib components - into production.

.. figure:: images/checkpointing/from_checkpoint.svg
    :width: 500
    :align: left

    **Creating a new instance directly from a checkpoint**: Use the classmethod
    :py:meth:`~ray.rllib.utils.checkpoints.Checkpointable.from_checkpoint` to instantiate objects directly
    from a checkpoint. RLlib first uses the saved meta data to create a bare-bones instance of the originally
    checkpointed object, and then restores its state from the state information in the checkpoint dir.

Another possibility is to load only a certain subcomponent's state into the containing
higher-level object. For example, you may want to load from disk only the state of your :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`,
located inside your :py:class:`~ray.rllib.algorithms.algorithm.Algorithm`, but leave all the other components
as-is.


Checkpointable API
------------------

RLlib manages checkpointing through the :py:class:`~ray.rllib.utils.checkpoints.Checkpointable` API,
which exposes the following three main methods:

- :py:meth:`~ray.rllib.utils.checkpoints.Checkpointable.save_to_path` for creating a new checkpoint
- :py:meth:`~ray.rllib.utils.checkpoints.Checkpointable.restore_from_path` for loading a state from a checkpoint into a running object
- :py:meth:`~ray.rllib.utils.checkpoints.Checkpointable.from_checkpoint` for creating a new object from a checkpoint

RLlib classes, which thus far support the :py:class:`~ray.rllib.utils.checkpoints.Checkpointable` API are:

- :py:class:`~ray.rllib.algorithms.algorithm.Algorithm`
- :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` (and :py:class:`~ray.rllib.core.rl_module.multi_rl_module.MultiRLModule`)
- :py:class:`~ray.rllib.env.env_runner.EnvRunner` (thus, also :py:class:`~ray.rllib.env.single_agent_env_runner.SingleAgentEnvRunner` and :py:class:`~ray.rllib.env.multi_agent_env_runner.MultiAgentEnvRunner`)
- :py:class:`~ray.rllib.connectors.connector_v2.ConnectorV2` (thus, also :py:class:`~ray.rllib.connectors.connector_pipeline_v2.ConnectorPipelineV2`)
- :py:class:`~ray.rllib.core.learner.learner_group.LearnerGroup`
- :py:class:`~ray.rllib.core.learner.learner.Learner`


Creating a new checkpoint with `save_to_path()`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Here is an example, using the :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` class, showing
how to create a checkpoint:

.. testcode::

    from ray.rllib.algorithms.ppo import PPOConfig

    # Configure and build an initial algorithm.
    config = (
        PPOConfig()
        .environment("Pendulum-v1")
    )
    ppo = config.build()

    # Train for one iteration, then save to a checkpoint.
    print(ppo.train())
    checkpoint_dir = ppo.save_to_path()
    print(f"saved algo to {checkpoint_dir}")

    # Shut down the algorithm.
    ppo.stop()


.. note::
    When running your experiments with `Ray Tune <https://docs.ray.io/en/latest/tune/index.html>`__,
    Tune calls the :py:meth:`~ray.rllib.utils.checkpoints.Checkpointable.save_to_path`
    method automatically on your :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` instance, whenever the training
    iteration matches the checkpoint frequency configured through Tune. The default location where Tune creates these checkpoints
    is ``~/ray_results/[your experiment name]``.


.. _rllib-structure-of-checkpoint-dir:

Structure of a checkpoint directory
+++++++++++++++++++++++++++++++++++

You now saved your PPO's state in the ``checkpoint_dir`` directory, or somewhere in ``~/ray_results/`` if you use Ray Tune.
Take a look at what the directory now looks like:

.. code-block:: shell

    $ cd [your algo checkpoint dir]
    $ ls -la
        .
        ..
        env_runner/
        learner_group/
        algorithm_state.pkl
        class_and_ctor_args.pkl
        metadata.json

Subdirectories inside a checkpoint dir, like ``env_runner/``, hint at a subcomponent's own checkpoint data.
For example, an :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` always also saves its
:py:class:`~ray.rllib.env.env_runner.EnvRunner` state and :py:class:`~ray.rllib.core.learner.learner_group.LearnerGroup` state.

.. note::
    Each of the subcomponent's directories themselves contain a ``metadata.json`` file, a ``class_and_ctor_args.pkl`` file
    and a ``.._state.pkl`` file, all serving the same purpose than their counterparts in the main algorithm checkpoint directory.
    For example, inside the ``learner_group/`` subdirectory, you would find the :py:class:`~ray.rllib.core.learner.learner_group.LearnerGroup`'s own
    state, construction, and meta information:

    .. code-block:: shell

        $ cd env_runner/
        $ ls -la
        .
        ..
        state.pkl
        class_and_ctor_args.pkl
        metadata.json

See :ref:`here for the complete RLlib component tree <rllib-components-tree>`.

The ``metadata.json`` file exists for your convenience only and RLlib doesn't need it.

.. note::
    The ``metadata.json`` file contains information about the Ray version used to create the checkpoint,
    the Ray commit, the RLlib checkpoint version, and the names of the state- and constructor-information
    files in the same directory.

    .. code-block:: shell

        $ more metadata.json
        {
            "class_and_ctor_args_file": "class_and_ctor_args.pkl",
            "state_file": "state",
            "ray_version": ..,
            "ray_commit": ..,
            "checkpoint_version": "2.1"
        }

The ``class_and_ctor_args.pkl`` file stores meta information needed to construct a "fresh" object, without any particular state.
This information, as the filename suggests, contains the class of the saved object and its constructor arguments and keyword arguments.
RLlib uses this file to create the initial new object when calling :py:meth:`~ray.rllib.utils.checkpoints.Checkpointable.from_checkpoint`.

Finally, the ``.._state.[pkl|msgpack]`` files contain the pickled or messagepacked state dict of the saved object.
RLlib obtains this state dict when saving a checkpoint through calling the object's
:py:meth:`~ray.rllib.utils.checkpoints.Checkpointable.get_state` method.

.. info::
    Support for ``msgpack`` based checkpoints is experimental, but might become the default in the future.
    Unlike ``pickle``, ``msgpack`` has the advantage of being independent of the python-version, thus allowing
    users to recover experiment and model states from old checkpoints that have been generated with an older python
    version.

    The Ray team is working on completely separating state from architecture, where all state information should go into
    the ``state.msgpack`` file and all architecture information should go into the ``class_and_ctor_args.pkl`` file.


.. _rllib-components-tree:

RLlib's components tree
+++++++++++++++++++++++

The following is the structure of RLlib's components tree, showing under which name you can
access a subcomponent's own checkpoint within the higher-level checkpoint. At the highest levet
is the :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` class:

.. code-block:: shell

    algorithm/
        learner_group/
            learner/
                rl_module/
                    default_policy/  # <- single-agent case
                    [module ID 1]/  # <- multi-agent case
                    [module ID 2]/  # ...
        env_runner/
            env_to_module_connector/
            module_to_env_connector/

.. note::
    The ``env_runner/`` subcomponent currently doesn't hold a copy of the RLModule's
    checkpoint because it's already saved under ``learner/``. The Ray team is working on resolving
    this issue, probably through softlinking to avoid duplicate files and unnecessary disk usage.


Creating instances from a checkpoint with `from_checkpoint`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Once you have a checkpoint of either a trained :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` or
any of its :ref:`subcomponents <rllib-components-tree>`, you can now create new objects directly
from this checkpoint.

For example, you could create a new :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` from
an existing algorithm checkpoint:

.. testcode::

    # Import the correct class to create from scratch using the checkpoint.
    from ray.rllib.algorithms.algorithm import Algorithm

    # Use the already existing checkpoint in `checkpoint_dir`.
    new_ppo = Algorithm.from_checkpoint(checkpoint_dir)
    # Confirm the `new_ppo` matches the originally checkpointed one.
    assert new_ppo.config.env == "Pendulum-v1"

    # Continue training
    new_ppo.train()

.. testcode::
    :hide:

    new_ppo.stop()


Using the exact same checkpoint from before and the same ``.from_checkpoint()`` utility,
you could also only reconstruct the RLModule trained by your Algorithm from the algorithm's checkpoint.
This becomes very useful when deploying trained models into production or evaluating them in a separate
process while training is ongoing.


.. testcode::

    from pathlib import Path
    import torch

    # Import the correct class to create from scratch using the checkpoint.
    from ray.rllib.core.rl_module.rl_module import RLModule

    # Use the already existing checkpoint in `checkpoint_dir`, but go further down
    # into its subdirectory for the single RLModule.
    # See the preceding section on "RLlib's components tree" for the various elements in the RLlib
    # components tree.
    rl_module_checkpoint_dir = Path(checkpoint_dir) / "learner_group" / "learner" / "rl_module" / "default_policy"

    # Create the actual RLModule.
    rl_module = RLModule.from_checkpoint(rl_module_checkpoint_dir)

    # Run a forward pass to compute action logits. Use a dummy Pendulum observation
    # tensor (3d) and add a batch dim (B=1).
    results = rl_module.forward_inference(
        {"obs": torch.tensor([0.5, 0.25, -0.3]).unsqueeze(0).float()}
    )
    print(results)


See here for an `example on how to run policy inference after training <https://github.com/ray-project/ray/blob/master/rllib/examples/inference/policy_inference_after_training.py>`__
and another `example on how to run policy inference, but with an LSTM <https://github.com/ray-project/ray/blob/master/rllib/examples/inference/policy_inference_after_training_w_connector.py>`__.


.. hint::

    A few things to note:
    * The checkpoint saves the entire information on how to recreate a new object, identical to the original one.
    *


Restoring state from a checkpoint with `restore_from_path`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~




.. testcode::

    from ray.rllib.algorithms.algorithm import Algorithm

    # Use any Checkpointable's (e.g. Algorithm's) `from_checkpoint()` method to create
    # a new instance that has the exact same state as the one, which created the checkpoint
    # in the first place:
    my_new_ppo = Algorithm.from_checkpoint(checkpoint_dir)

    # Continue training.
    my_new_ppo.train()

    my_new_ppo.stop()



    # Re-build a fresh algorithm.
    my_new_ppo = my_ppo_config.build()

    # Restore the old (checkpointed) state.
    my_new_ppo.restore_from_path(save_result)

    # Continue training.
    my_new_ppo.train()

    my_new_ppo.stop()








Checkpoints are py-version specific, but can be converted to be version independent
-----------------------------------------------------------------------------------

Checkpoints created with the :py:meth:`~ray.rllib.utils.checkpoints.Checkpointable.save_to_path()` method
are based on `cloudpickle <https://github.com/cloudpipe/cloudpickle>`__ and thus depend on the python version used.
This means there is no guarantee that you are able to use a checkpoint created with ``python 3.x`` to restore
an Algorithm in another environment that runs ``python 3.x+1``.

However, we now provide a utility for converting a checkpoint into a python version independent checkpoint based on `msgpack <https://msgpack.org/>`__.
You can then use the newly converted msgpack checkpoint to restore another
Algorithm instance from it. Look at this this short example here on how to do this:

.. literalinclude:: doc_code/checkpointing.py
    :language: python
    :start-after: __rllib-convert-pickle-to-msgpack-checkpoint-begin__
    :end-before: __rllib-convert-pickle-to-msgpack-checkpoint-end__

This way, you can continue to run your algorithms and `save()` them occasionally or
- if you are running trials with Ray Tune - use Tune's integrated checkpointing settings.
As has been, this will produce cloudpickle based checkpoints. Once you need to migrate to
a higher (or lower) python version, use the ``convert_to_msgpack_checkpoint()`` utility,
create a msgpack-based checkpoint and hand that to either ``Algorithm.from_checkpoint()``
or provide this to your Tune config. RLlib is able to recreate Algorithms from both these
formats now.




How do I restore an Algorithm from a checkpoint?
------------------------------------------------

Given our checkpoint path (returned by ``Algorithm.save()``), we can now
create a completely new Algorithm instance and make it the exact same as the one we
had stopped (and could thus no longer use) in the example above:

.. literalinclude:: doc_code/checkpointing.py
    :language: python
    :start-after: __restore-from-algo-checkpoint-begin__
    :end-before: __restore-from-algo-checkpoint-end__


Alternatively, you could also first create a new Algorithm instance using the
same config that you used for the original algo, and only then call the new
Algorithm's ``restore()`` method, passing it the checkpoint directory:

.. literalinclude:: doc_code/checkpointing.py
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

From Ray 2.40 on, you can find the checkpoint version written in the
``metadata.json`` file inside any checkpoint directory.
RLlib doesn't use this file and it solely exists for your convenience.

From `Ray 2.40` and up, all RLlib checkpoints are backward compatible, meaning an RLlib
checkpoint created with Ray `2.x` can be read and handled by `Ray 2.x+n`, as long as `x >= 40`.
The Ray team makes sure of not breaking this guarantee in the future through comprehensive
CI tests on checkpoints taken with each Ray version, making sure every single commit


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

.. literalinclude:: doc_code/checkpointing.py
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

.. literalinclude:: doc_code/checkpointing.py
    :language: python
    :start-after: __multi-agent-checkpoints-restore-policy-sub-set-begin__
    :end-before: __multi-agent-checkpoints-restore-policy-sub-set-end__



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

.. literalinclude:: doc_code/checkpointing.py
    :language: python
    :start-after: __restore-algorithm-from-checkpoint-with-fewer-policies-begin__
    :end-before: __restore-algorithm-from-checkpoint-with-fewer-policies-end__

Note that we had to change our original ``policy_mapping_fn`` from one that maps
"agent0" to "pol0", "agent1" to "pol1", etc..
to a new function that maps our five agents to only the two remaining policies:
"agent0" and "agent1" to "pol0", all other agents to "pol1".



And what about exporting my NN Models in ONNX format?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

RLlib also supports exporting your NN models in the ONNX format. For that, use the
:py:class:`~ray.rllib.policy.policy.Policy` ``export_model`` method, but provide the
extra ``onnx`` arg as follows:


.. literalinclude:: doc_code/checkpointing.py
    :language: python
    :start-after: __export-models-as-onnx-begin__
    :end-before: __export-models-as-onnx-end__
