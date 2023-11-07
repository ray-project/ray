:orphan:

Configuration Overview
======================

.. _train-run-config:

Run Configuration in Train (``RunConfig``)
------------------------------------------

``RunConfig`` is a configuration object used in Ray Train to define the experiment
spec that corresponds to a call to ``trainer.fit()``.

It includes settings such as the experiment name, storage path for results,
stopping conditions, custom callbacks, checkpoint configuration, verbosity level,
and logging options.

Many of these settings are configured through other config objects and passed through
the ``RunConfig``. The following sub-sections contain descriptions of these configs.

The properties of the run configuration are :ref:`not tunable <tune-search-space-tutorial>`.

.. literalinclude:: ../doc_code/key_concepts.py
    :language: python
    :start-after: __run_config_start__
    :end-before: __run_config_end__

.. seealso::

    See the :class:`~ray.train.RunConfig` API reference.

    See :ref:`persistent-storage-guide` for storage configuration examples (related to ``storage_path``).

