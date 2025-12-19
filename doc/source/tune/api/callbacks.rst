.. _tune-callbacks-docs:

Tune Callbacks (tune.Callback)
==============================

See :doc:`this user guide </tune/tutorials/tune-metrics>` for more details.

.. seealso::

    :doc:`Tune's built-in loggers </tune/api/logging>` use the ``Callback`` interface.


Callback Interface
------------------

Callback Initialization and Setup
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. currentmodule:: ray.tune
.. autosummary::
    :nosignatures:
    :toctree: doc/

    Callback

.. autosummary::
    :nosignatures:
    :toctree: doc/

    Callback.setup


Callback Hooks
~~~~~~~~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc/

    Callback.on_checkpoint
    Callback.on_experiment_end
    Callback.on_step_begin
    Callback.on_step_end
    Callback.on_trial_complete
    Callback.on_trial_error
    Callback.on_trial_restore
    Callback.on_trial_result
    Callback.on_trial_save
    Callback.on_trial_start


Stateful Callbacks
~~~~~~~~~~~~~~~~~~

The following methods must be overridden for stateful callbacks to be saved/restored
properly by Tune.

.. autosummary::
    :nosignatures:
    :toctree: doc/

    Callback.get_state
    Callback.set_state
