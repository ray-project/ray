:orphan:

Asynchronous HyperBand Example
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This example demonstrates how to use Ray Tune's Asynchronous Successive Halving Algorithm (ASHA) scheduler 
to efficiently optimize hyperparameters for a machine learning model. ASHA is particularly useful for 
large-scale hyperparameter optimization as it can adaptively allocate resources and end 
poorly performing trials early.

Requirements: `pip install "ray[tune]"`

.. literalinclude:: /../../python/ray/tune/examples/async_hyperband_example.py


See Also
--------

- `ASHA Paper <https://arxiv.org/abs/1810.05934>`_