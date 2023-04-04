.. include:: /_includes/rllib/rlmodules_rollout.rst

.. _rllib-catalogs-user-guide:

Catalogs
========

Catalogs are where RL Modules primarily get their models and action distributions from.
Each RLModule has its own default Catalog - PPORLModule has the PPOCatalog.
You can override Catalogs’ methods to alter the behavior of existing RLModules.
This makes Catalogs a means of configuration for RLModules.
You interact with Catalogs when making deeper customization to what models and distributions RLlib creates by default.
Interacting with Catalogs mainly covers advanced use cases.

If you want to simply want to modify the configurations of RLlib’s default models, have a look at the model config:

.. dropdown:: **RLlib Algorithms**
    :animate: fade-in-slide-down

    This dict (or an overriding sub-set) is part of AlgorithmConfig.

    .. literalinclude:: ../../../rllib/models/catalog.py
       :language: python
       :start-after: __sphinx_doc_begin__
       :end-before: __sphinx_doc_end__

Prerequisites for this user guide is a rough understanding of RLModules.
- After reading this user guide you will be able to…
- Know what Catalogs are
- Instantiate and interact with a Catalog
- Inject your custom models into RLModules
- Inject your custom action distributions into RLModules
- Extend RLlib’s selection of Models and distributions with your own
- Write a Catalog from scratch

Instantiate and interact with a Catalog
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following three examples show basic interaction with RLLib’s Catalogs.
While the first one only shows how to get a model or an action distribution out of an encoder,
the other two show how to create complete models to step through an environment.

.. tabbed:: Basic interaction

    In this example, we showcase the general API for interacting with Catalogs.

    .. literalinclude:: ../../../rllib/examples/catalog/basics/basic_interaction.py
       :language: python
       :start-after: __sphinx_doc_begin__
       :end-before: __sphinx_doc_end__

.. tabbed:: CartPole with PPOCatalog

    In this example, we showcase how to use the PPOCatalog to create models and an action distribution.
    This is very similar to what RLlib does internally within the context of RLModules.

    .. literalinclude:: ../../../rllib/examples/catalog/basics/cartpole_models_base_catalog.py
       :language: python
       :start-after: __sphinx_doc_begin__
       :end-before: __sphinx_doc_end__

.. tabbed:: CartPole with base Catalog and custom model

    In this example, we showcase how to use the base Catalog to create an encoder and an action distribution.
    Besides these, we create an MLP that fits these two by hand.

    .. literalinclude:: ../../../rllib/examples/catalog/basics/cartpole_models_ppo_catalog.py
       :language: python
       :start-after: __sphinx_doc_begin__
       :end-before: __sphinx_doc_end__

What are Catalogs
~~~~~~~~~~~~~~~~~

Catalogs have two primary roles: Choosing the right model and choosing the right action distribution.
By default, all catalogs implement decision trees that choose these based on a range of inputs.
These include, but are not limited to, the observation- and action-space of the RLModule,
the model config [link] and the tensor framework.
Catalogs are also similar to what was called ModelCatalog in earlier versions of RLlib, but are more flexible.

RLModules create an instance of the Catalog they are configured with upon instantiation.
They then create their internal models and distributions from this Catalog:
