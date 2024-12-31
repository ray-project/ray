
.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst

.. _utils-reference-docs:

RLlib Utilities
===============

Here is a list of all the utilities available in RLlib.

Scheduler API
-------------

RLlib uses the Scheduler API to set scheduled values for variables, in Python or PyTorch,
dependent on an int timestep input. The type of the schedule is always a ``PiecewiseSchedule``, which defines a list
of increasing time steps, starting at 0, associated with values to be reached at these particular timesteps.
``PiecewiseSchedule`` interpolates values for all intermittent timesteps.
The computed values are usually float32 types.

For example:

.. testcode::

    from ray.rllib.utils.schedules.scheduler import Scheduler

    scheduler = Scheduler([[0, 0.1], [50, 0.05], [60, 0.001]])
    print(scheduler.get_current_value())  # <- expect 0.1

    # Up the timestep.
    schedule.update(timestep=45)
    print(scheduler.get_current_value())  # <- expect 0.055

    # Up the timestep.
    schedule.update(timestep=100)
    print(scheduler.get_current_value())  # <- expect 0.001 (keep final value)


.. currentmodule:: ray.rllib.utils.schedules.scheduler

.. autosummary::
    :nosignatures:
    :toctree: doc/

    Scheduler
    Scheduler.validate
    Scheduler.get_current_value
    Scheduler.update
    Scheduler._create_tensor_variable


Framework Utilities
-------------------

Import utilities
~~~~~~~~~~~~~~~~

.. currentmodule:: ray.rllib.utils.framework

.. autosummary::
   :nosignatures:
   :toctree: doc/

   ~try_import_torch

Torch utilities
~~~~~~~~~~~~~~~

.. currentmodule:: ray.rllib.utils.torch_utils

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~clip_gradients
    ~compute_global_norm
    ~convert_to_torch_tensor
    ~explained_variance
    ~flatten_inputs_to_1d_tensor
    ~global_norm
    ~one_hot
    ~reduce_mean_ignore_inf
    ~sequence_mask
    ~set_torch_seed
    ~softmax_cross_entropy_with_logits
    ~update_target_network

Numpy utilities
~~~~~~~~~~~~~~~

.. currentmodule:: ray.rllib.utils.numpy

.. autosummary::
   :nosignatures:
   :toctree: doc/

   ~aligned_array
   ~concat_aligned
   ~convert_to_numpy
   ~fc
   ~flatten_inputs_to_1d_tensor
   ~make_action_immutable
   ~huber_loss
   ~l2_loss
   ~lstm
   ~one_hot
   ~relu
   ~sigmoid
   ~softmax


Checkpoint utilities
--------------------

.. currentmodule:: ray.rllib.utils.checkpoints

.. autosummary::
   :nosignatures:
   :toctree: doc/

   try_import_msgpack
   Checkpointable
