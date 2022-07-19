.. _air-scaling-config:

Configuring scaling
===================

Ths guide describes how you can use the ``ScalingConfig`` object to configure resource utilization at the
per-run level when training models with Ray AIR.

Ray Train Usage
---------------

To use ``ScalingConfig`` when training a model, pass in the ``scaling_config`` parameter to your 
``Trainer``:

.. literalinclude:: doc_code/config_scaling.py
        :language: python
        :start-after: __config_scaling_1__
        :end-before: __config_scaling_1_end__


Ray Tune Usage
--------------

You can also treat some scaling config variables as hyperparameters and optimize them using Ray Tune. 

Rather than passing in the ``scaling_config`` parameter to ``Trainer``, instead set the ``scaling_config``
key of the ``param_space`` dict that is passed to your ``Tuner`` initializer:

.. literalinclude:: doc_code/config_scaling.py
        :language: python
        :start-after: __config_scaling_2__
        :end-before: __config_scaling_2_end__

For details on how Ray Tune resolves search spaces, see 
:ref:`Ray Tune's search space tutorial <tune-search-space-tutorial>`.
