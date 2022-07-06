.. _config-scaling:

Configuring scaling
===================

Ths guide describes how you can use the ``ScalingConfig`` object configure resource utilization at the per-run level when training models with Ray AIR.

Train Usage
-----------

To use ``ScalingConfig`` when training a model, pass in the ``scaling_config`` parameter to your ``Trainer``:

.. literalinclude:: doc_code/config_scaling.py
        :language: python
        :start-after: __config_scaling_1__
        :end-before: __config_scaling_1_end__


Tune Usage
----------

You can also treat some scaling config variables as hyperparameters and optimize them using Tune. All Tune primitives are supported.

If you use ``tune.grid_search``, then Tune will resolve the search space to yield variants for grid search variable. 

For Tune primitives other than ``tune.grid_search``, no new variants will be generated, and the relevant variables will be sampled exactly once. 

The following example demonstrates both concepts above: 

.. literalinclude:: doc_code/config_scaling.py
        :language: python
        :start-after: __config_scaling_2__
        :end-before: __config_scaling_2_end__

Two variants of ``param_space`` will be produced (one for each value of ``num_workers`` in ``scaling_config``). The values of ``eta``, ``subsample``, 
and ``max_depth`` will be sampled from their corresponding Tune domains, but will not produce any new variants.
