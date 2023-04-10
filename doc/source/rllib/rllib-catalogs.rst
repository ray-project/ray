.. include:: /_includes/rllib/announcement.rst

.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/rlmodules_rollout.rst

.. note:: Interacting with Catalogs mainly covers advanced use cases.

Catalog (Alpha)
===============

Catalogs are where `RLModules <rllib-rlmodule.html>`__ primarily get their models and action distributions from.
Each :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` has its own default
:py:class:`~ray.rllib.core.models.catalog.Catalog`. For example,
:py:class:`~ray.rllib.algorithms.ppo.ppo_torch_rl_module.PPOTorchRLModule` has the
:py:class:`~ray.rllib.algorithms.ppo.ppo_catalog.PPOCatalog`.
You can override Catalogs’ methods to alter the behavior of existing RLModules.
This makes Catalogs a means of configuration for RLModules.
You interact with Catalogs when making deeper customization to what :py:class:`~ray.rllib.core.models.Model` and :py:class:`~ray.rllib.models.distributions.Distribution` RLlib creates by default.

.. note::
    If you simply want to modify a :py:class:`~ray.rllib.core.models.Model` by changing its default values,
    have a look at the ``model config dict``:

    .. dropdown:: **``MODEL_DEFAULTS`` dict**
        :animate: fade-in-slide-down

        This dict (or an overriding sub-set) is part of :py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig`
        and therefore also part of any algorithm-specific config.
        You can override its values and pass it to an :py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig`
        to change the behavior RLlib's default models.

        .. literalinclude:: ../../../rllib/models/catalog.py
            :language: python
            :start-after: __sphinx_doc_begin__
            :end-before: __sphinx_doc_end__

While Catalogs have a base class :py:class:`~ray.rllib.core.models.catalog.Catalog`, you mostly interact with
Algorithm-specific Catalogs.
Therefore, this doc also includes examples around PPO from which you can extrapolate to other algorithms.
Prerequisites for this user guide is a rough understanding of `RLModules <rllib-rlmodule.html>`__.
This user guide covers the following topics:

- Basic usage
- What are Catalogs
- Inject your custom models into RLModules
- Inject your custom action distributions into RLModules

.. - Extend RLlib’s selection of Models and Distributions with your own
.. - Write a Catalog from scratch

Catalog and AlgorithmConfig
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Since Catalogs effectively control what ``models`` and ``distributions`` RLlib uses under the hood,
they are also part of RLlib’s configurations. As the primary entry point for configuring RLlib,
:py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig` is the place where you can configure the
Catalogs of the RLModules that are created.
You set the ``catalog class`` by going through the :py:class:`~ray.rllib.core.rl_module.rl_module.SingleAgentRLModuleSpec`
or :py:class:`~ray.rllib.core.rl_module.marl_module.MultiAgentRLModuleSpec` of an AlgorithmConfig.
For example, in heterogeneous multi-agent cases, you modify the MultiAgentRLModuleSpec.

.. image:: images/catalog/catalog_rlmspecs_diagram.svg
    :align: center

The following example shows how to configure the Catalog of an :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`
created by PPO.

.. literalinclude:: doc_code/catalog_guide.py
    :language: python
    :start-after: __sphinx_doc_algo_configs_begin__
    :end-before: __sphinx_doc_algo_configs_end__

Basic usage
~~~~~~~~~~~

The following three examples illustrate three basic usage patterns of Catalogs.

The first example showcases the general API for interacting with Catalogs.

.. literalinclude:: doc_code/catalog_guide.py
   :language: python
   :start-after: __sphinx_doc_basic_interaction_begin__
   :end-before: __sphinx_doc_basic_interaction_end__

The second example showcases how to use the :py:class:`~ray.rllib.algorithms.ppo.ppo_catalog.PPOCatalog`
to create a ``model`` and an ``action distribution``.
This is more similar to what RLlib does internally.

.. dropdown:: **Use catalog-generated models**
    :animate: fade-in-slide-down

    .. literalinclude:: doc_code/catalog_guide.py
       :language: python
       :start-after: __sphinx_doc_ppo_models_begin__
       :end-before: __sphinx_doc_ppo_models_end__

The third example showcases how to use the base :py:class:`~ray.rllib.core.models.catalog.Catalog`
to create an ``encoder`` and an ``action distribution``.
Besides these, we create a ``head network`` that fits these two by hand to show how you can combine RLlib's
:py:class:`~ray.rllib.core.models.base.ModelConfig` API and Catalog.
Extending Catalog to also build this head is how :py:class:`~ray.rllib.core.models.catalog.Catalog` is meant to be
extended, which we cover later in this guide.

.. dropdown:: **Customize a policy head**
    :animate: fade-in-slide-down

    .. literalinclude:: doc_code/catalog_guide.py
       :language: python
       :start-after: __sphinx_doc_modelsworkflow_begin__
       :end-before: __sphinx_doc_modelsworkflow_end__

What are Catalogs
~~~~~~~~~~~~~~~~~

Catalogs have two primary roles: Choosing the right :py:class:`~ray.rllib.core.models.Model` and choosing the right :py:class:`~ray.rllib.models.distributions.Distribution`.
By default, all catalogs implement decision trees that decide model architecture based on a combination of input configurations.
These mainly include the ``observation space`` and ``action space`` of the :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`, the ``model config dict`` and the ``deep learning framework backend``.

The following diagram shows the break down of the information flow towards ``models`` and ``distributions`` within an :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`.
An :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` creates an instance of the Catalog class they receive as part of their constructor.
It then create its internal ``models`` and ``distributions`` with the help of this Catalog.

.. note::
    You can also modify :py:class:`~ray.rllib.core.models.Model` or :py:class:`~ray.rllib.models.distributions.Distribution` in an :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` directly by overriding the RLModule's constructor!

.. image:: images/catalog/catalog_and_rlm_diagram.svg
    :align: center

The following diagram shows a concrete case in more detail.

.. dropdown:: **Example of catalog in a PPORLModule**
    :animate: fade-in-slide-down

    The :py:class:`~ray.rllib.algorithms.ppo.ppo_catalog.PPOCatalog` is fed an ``observation space``, ``action space``,
    a ``model config dict`` and the ``view requirements`` of the :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`.
    The model config dicts and the view requirements are only of interest in special cases, such as
    recurrent networks or attention networks. A PPORLModule has four components that are created by the
    :py:class:`~ray.rllib.algorithms.ppo.ppo_catalog.PPOCatalog`:
    ``Encoder``, ``value function head``, ``policy head``, and ``action distribution``.

    .. image:: images/catalog/ppo_catalog_and_rlm_diagram.svg
        :align: center


Inject your custom model or action distributions into RLModules
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can make a :py:class:`~ray.rllib.core.models.catalog.Catalog` build custom ``models`` by overriding the Catalog’s methods used by RLModules to build ``models``.
Have a look at these lines from the constructor of the :py:class:`~ray.rllib.algorithms.ppo.ppo_torch_rl_module.PPOTorchRLModule` to see how Catalogs are being used by an :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`:

.. literalinclude:: ../../../rllib/algorithms/ppo/ppo_base_rl_module.py
    :language: python
    :start-after: __sphinx_doc_begin__
    :end-before: __sphinx_doc_end__

Consequently, in order to build a custom :py:class:`~ray.rllib.core.models.Model` compatible with a PPORLModule,
you can override methods by inheriting from :py:class:`~ray.rllib.algorithms.ppo.ppo_catalog.PPOCatalog`
or write a :py:class:`~ray.rllib.core.models.catalog.Catalog` that implements them from scratch.
The following showcases such modifications.

This example shows two modifications:

- How to write a custom :py:class:`~ray.rllib.models.distributions.Distribution`
- How to inject a custom action distribution into a :py:class:`~ray.rllib.core.models.catalog.Catalog`

.. literalinclude:: ../../../rllib/examples/catalog/custom_action_distribution.py
   :language: python
   :start-after: __sphinx_doc_begin__
   :end-before: __sphinx_doc_end__



Notable TODOs
-------------

- Add cross references to Model and Distribution API docs
- Add example that shows how to inject own model
- Add more instructions on how to write a catalog from scratch
- Add section "Extend RLlib’s selection of Models and Distributions with your own"
- Add section "Write a Catalog from scratch"