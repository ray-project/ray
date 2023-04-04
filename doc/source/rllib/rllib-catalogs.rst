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

.. note::
    If you simply want to modify RLlib’s by configuring its default models, have a look at the model config dict:

    .. dropdown:: **MODEL_DEFAULTS dict**
        :animate: fade-in-slide-down

        This dict (or an overriding sub-set) is part of AlgorithmConfig and therefore also part of any
        algorithm-specific config. You can override its values and pass it to an AlgorithmConfig
        to change the behavior RLlib's default models.

        .. literalinclude:: ../../../rllib/models/catalog.py
            :language: python
            :start-after: __sphinx_doc_begin__
            :end-before: __sphinx_doc_end__

While Catalogs have a base class, you mostly interact with Algorithm-specific Catalogs.
Therefore, this doc also includes examples around PPO from which you can extrapolate to other algorithms.
Prerequisites for this user guide is a rough understanding of RLModules.
After reading this user guide you will be able to…

    - Instantiate and interact with a Catalog
    - Inject your custom models into RLModules
    - Inject your custom action distributions into RLModules
    - Extend RLlib’s selection of Models and distributions with your own
    - Write a Catalog from scratch

Instantiate and interact with a Catalog
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following three examples show variations of basic interaction with RLLib’s Catalogs.
We create models as well as distributions and step through an environment with them.

.. tabbed:: Basic interaction

    In this example, we showcase the general API for interacting with Catalogs.

    .. literalinclude:: ../../../rllib/examples/catalog/basics/basic_interaction.py
       :language: python
       :start-after: __sphinx_doc_begin__
       :end-before: __sphinx_doc_end__

.. tabbed:: PPOCatalog + CartPole

    In this example, we showcase how to use the PPOCatalog to create models and an action distribution.
    This is more similar to what RLlib does internally.

    .. literalinclude:: ../../../rllib/examples/catalog/basics/cartpole_models_ppo.py
       :language: python
       :start-after: __sphinx_doc_begin__
       :end-before: __sphinx_doc_end__

.. tabbed:: Catalog + Custom Model + CartPole

    In this example, we showcase how to use the base Catalog to create an encoder and an action distribution.
    Besides these, we create a head network that fits these two by hand to show how you can combine RLLib's
    ModelConfig API and Catalog. Extending Catalog to also build this head is how Catalog is meant to be
    extended, which we cover later in this guide.

    .. literalinclude:: ../../../rllib/examples/catalog/basics/cartpole_models.py
       :language: python
       :start-after: __sphinx_doc_begin__
       :end-before: __sphinx_doc_end__

What are Catalogs
~~~~~~~~~~~~~~~~~

Catalogs have two primary roles: Choosing the right model and choosing the right action distribution.
By default, all catalogs implement decision trees that choose these based on a range of inputs.
These include mainly the observation- and action-space of the RLModule, the model config dict and the tensor backend.
Catalogs are also similar to what was called ModelCatalog in earlier versions of RLlib, but are more flexible.

.. tabbed:: Catalogs vs RLModules

    The following, broken down, diagram shows the general flow of information towards models and distributions.
    RLModules create an instance of the Catalog class they receive as part of their constructor.
    They then create their internal models and action distributions with the help of this Catalog.

    .. note::
      You can also modify the models and distributions in RLModules directly by overriding their constructor!

    .. image:: images/catalog/catalog_and_rlm_diagram.svg
        :align: center

.. tabbed:: PPOCatalog vs PPORLModule

    The following diagram shows a concrete case in more detail.
    Therein, the PPOCatalog is fed an observation space, action space, a model config dict and the view requirements
    of the RLModule. The model config dicts and the view requirements are only of interest in special cases, such as
    recurrent networks or attention networks. The PPORLModule has four components that are created by the PPOCatalog:
    Encoder, value function head, policy head and action distribution. You can find out more about this
    distinction between these components in our section on Models.

    .. image:: images/catalog/ppo_catalog_and_rlm_diagram.svg
        :align: center

Catalogs and AlgorithmConfig
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Since Catalogs effectively control what models and distributions RLlib uses under the hood,
they are also part of RLlib’s configurations. As the primary entry point for configuring RLlib,
AlgorithmConfig is the place where you can configure the Catalogs of the RLModules that are created.
The following example shows how to configure the Catalogs of the RLModules that are created by PPO.

.. literalinclude:: ../../../rllib/examples/catalog/basics/catalogs_in_algo_configs.py
    :language: python
    :start-after: __sphinx_doc_begin__
    :end-before: __sphinx_doc_end__

.. image:: images/catalog/catalog_rlmspecs_diagram.svg
    :align: center


Inject your custom models into RLModules
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can make Catalog build custom models by overriding the Catalog’s methods used by RL Modules to build models.
For example, a PPOCatalog has the following methods for building models:
    - build_actor_critic_encoder
    - build_pi_head
    - build_vf_head
Have a look at these lines from the constructor of the PPORLModules to see how Catalogs are being used by RLModules:

.. literalinclude:: ../../../rllib/algorithms/ppo/ppo_base_rl_module.py
    :language: python
    :start-after: __sphinx_doc_begin__
    :end-before: __sphinx_doc_end__

Consequently, in order to build custom models compatible with the PPORLModule,
you can override these methods by inheriting from PPOCatalog or write a Catalog that implements them from scratch.
The following examples show different such modifications.

.. tabbed:: Custom action distribution

    This example shows two things:
        - How to write a custom action distribution
        - How to inject a custom action distribution into a Catalog

    .. literalinclude:: ../../../rllib/examples/catalog/custom_action_distribution.py
       :language: python
       :start-after: __sphinx_doc_begin__
       :end-before: __sphinx_doc_end__