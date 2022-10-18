Function API Checkpointing
~~~~~~~~~~~~~~~~~~~~~~~~~~

Many Tune features rely on checkpointing, including the usage of certain Trial Schedulers and fault tolerance.
You can save and load checkpoints in Ray Tune in the following manner:

.. literalinclude:: /tune/doc_code/trainable.py
    :language: python
    :start-after: __function_api_checkpointing_start__
    :end-before: __function_api_checkpointing_end__

.. note:: ``checkpoint_frequency`` and ``checkpoint_at_end`` will not work with Function API checkpointing.

In this example, checkpoints will be saved by training iteration to ``<local_dir>/<exp_name>/trial_name/checkpoint_<step>``.

Tune also may copy or move checkpoints during the course of tuning. For this purpose,
it is important not to depend on absolute paths in the implementation of ``save``.
