.. _air-session-ref:

Ray AIR Session
===============

.. currentmodule:: ray.air

.. seealso::
    See this :ref:`Ray Train user guide <train-monitoring>` and
    this :ref:`Ray Tune user guide <tune-function-api>` for usage examples
    of ``ray.air.session`` in the respective libraries.


Report Metrics and Save Checkpoints
-----------------------------------

.. autosummary::
    :toctree: doc/

    session.report


Retrieve Checkpoints and Datasets
-----------------------------------

.. autosummary::
    :toctree: doc/

    session.get_checkpoint
    session.get_dataset_shard


AIR Session Metadata
----------------------------

.. autosummary::
    :toctree: doc/

    session.get_experiment_name
    session.get_trial_name
    session.get_trial_id
    session.get_trial_resources
    session.get_trial_dir
    session.get_world_size
    session.get_world_rank
    session.get_local_world_size
    session.get_local_rank
    session.get_node_rank

