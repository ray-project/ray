.. include:: /_includes/rllib/announcement.rst

.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/rlmodules_rollout.rst


Catalog (Beta)
===============

Catalogs are where `RLModules <rllib-rlmodule.html>`__ primarily get their models and action distributions from.
Each :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` has its own default
:py:class:`~ray.rllib.core.models.catalog.Catalog`. For example,
:py:class:`~ray.rllib.algorithms.ppo.ppo_torch_rl_module.PPOTorchRLModule` has the
:py:class:`~ray.rllib.algorithms.ppo.ppo_catalog.PPOCatalog`.

.. note::
    Modifying Catalogs signifies advanced use cases so you should only consider this if modifying an RLModule or writing one does not cover your use case.
    We recommend to modify Catalogs only when making deeper customizations to the decision trees that determine what :py:class:`~ray.rllib.core.models.base.Model` and :py:class:`~ray.rllib.models.distributions.Distribution` RLlib creates by default.

.. note::
    If you simply want to modify a Model by changing its default values,
    have a look at the model config dict:

    .. dropdown:: ``MODEL_DEFAULTS``
        :animate: fade-in-slide-down

        This dict (or an overriding sub-set) is part of :py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig`
        and therefore also part of any algorithm-specific config.
        You can override its values and pass it to an AlgorithmConfig
        to change the behavior RLlib's default models.

        .. literalinclude:: ../../../rllib/models/catalog.py
            :language: python
            :start-after: __sphinx_doc_begin__
            :end-before: __sphinx_doc_end__

While Catalogs have a base class Catalog, you mostly interact with
Algorithm-specific Catalogs.
Therefore, this doc also includes examples around PPO from which you can extrapolate to other algorithms.
Prerequisites for this user guide is a rough understanding of `RLModules <rllib-rlmodule.html>`__.
This user guide covers the following topics:

- Catalog and AlgorithmConfig
- Basic usage
- What are Catalogs
- Inject your custom models into RLModules
- Inject your custom action distributions into RLModules
- Design philosophy
- Write a Catalog from scratch

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
Extending Catalog to also build this head is how Catalog is meant to be
extended, which we cover later in this guide.

.. dropdown:: **Customize a policy head**
    :animate: fade-in-slide-down

    .. literalinclude:: doc_code/catalog_guide.py
       :language: python
       :start-after: __sphinx_doc_modelsworkflow_begin__
       :end-before: __sphinx_doc_modelsworkflow_end__

What are Catalogs
~~~~~~~~~~~~~~~~~

Catalogs have two primary roles: Choosing the right :py:class:`~ray.rllib.core.models.base.Model` and choosing the right :py:class:`~ray.rllib.models.distributions.Distribution`.
By default, all catalogs implement decision trees that decide model architecture based on a combination of input configurations.
These mainly include the ``observation space`` and ``action space`` of the :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`, the ``model config dict`` and the ``deep learning framework backend``.

The following diagram shows the break down of the information flow towards ``models`` and ``distributions`` within an RLModule.
An RLModule creates an instance of the Catalog class they receive as part of their constructor.
It then create its internal ``models`` and ``distributions`` with the help of this Catalog.

.. note::
    You can also modify Model or Distribution in an RLModule directly by overriding the RLModule's constructor!

.. image:: images/catalog/catalog_and_rlm_diagram.svg
    :align: center

The following diagram shows a concrete case in more detail.

.. dropdown:: **Example of catalog in a PPORLModule**
    :animate: fade-in-slide-down

    The :py:class:`~ray.rllib.algorithms.ppo.ppo_catalog.PPOCatalog` is fed an ``observation space``, ``action space``,
    a ``model config dict`` and the ``view requirements`` of the :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`.
    The ``model config dicts`` and the ``view requirements`` are only of interest in special cases, such as
    recurrent networks or attention networks. A PPORLModule has four components that are created by the PPOCatalog:
    ``Encoder``, ``value function head``, ``policy head``, and ``action distribution``.

    .. image:: images/catalog/ppo_catalog_and_rlm_diagram.svg
        :align: center


Inject your custom model or action distributions into Catalogs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can make a :py:class:`~ray.rllib.core.models.catalog.Catalog` build custom ``models`` by overriding the Catalog’s methods used by RLModules to build ``models``.
Have a look at these lines from the constructor of the :py:class:`~ray.rllib.algorithms.ppo.ppo_torch_rl_module.PPOTorchRLModule` to see how Catalogs are being used by an :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`:

.. literalinclude:: ../../../rllib/algorithms/ppo/ppo_rl_module.py
    :language: python
    :start-after: __sphinx_doc_begin__
    :end-before: __sphinx_doc_end__

Consequently, in order to build a custom :py:class:`~ray.rllib.core.models.Model` compatible with a PPORLModule,
you can override methods by inheriting from :py:class:`~ray.rllib.algorithms.ppo.ppo_catalog.PPOCatalog`
or write a :py:class:`~ray.rllib.core.models.catalog.Catalog` that implements them from scratch.
The following examples showcase such modifications:


.. tab-set::

    .. tab-item:: Adding a custom Encoder

        This example shows two modifications:

        - How to write a custom :py:class:`~ray.rllib.models.base.Encoder`
        - How to inject the custom Encoder into a :py:class:`~ray.rllib.core.models.catalog.Catalog`

        Note that, if you only want to inject your Encoder into a single :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`, the recommended workflow is to inherit
        from an existing RL Module and place the Encoder there.

        .. literalinclude:: ../../../rllib/examples/catalog/mobilenet_v2_encoder.py
           :language: python
           :start-after: __sphinx_doc_begin__
           :end-before: __sphinx_doc_end__


    .. tab-item:: Adding a custom action distribution

        This example shows two modifications:

        - How to write a custom :py:class:`~ray.rllib.models.distributions.Distribution`
        - How to inject the custom action distribution into a :py:class:`~ray.rllib.core.models.catalog.Catalog`

        .. literalinclude:: ../../../rllib/examples/catalog/custom_action_distribution.py
           :language: python
           :start-after: __sphinx_doc_begin__
           :end-before: __sphinx_doc_end__

Catalog design
~~~~~~~~~~~~~~

In order to facilitate better development on this component, we explain the design and ideas behind Catalogs in this section.

What problems do Catalogs solve?
--------------------------------

RL algorithms need neural network ``models`` and ``distributions``.
Within an algorithm, many different architectures for such sub-components are valid.
Moreover, models and distributions vary with environments.
However, most algorithms require models that have similarities.
The problem is finding sensible sub-components for a wide range of use cases while sharing this functionality
across algorithms.

How do Catalogs solve this?
----------------------------

As states above, Catalogs implement decision-trees for sub-components of RL Modules.
Models and distributions from a Catalog object are meant to fit together.
Since we mostly build RL Modules out of :py:class:`~ray.rllib.core.models.base.Encoder` s, Heads and :py:class:`~ray.rllib.models.distributions.Distribution` s, Catalogs also generally reflect this.
For example, the PPOCatalog will output Encoders that output a latent vector and two Heads that take this latent vector as input.
(That's why Catalogs have a ``latent_dims`` attribute). Heads and distributions behave accordingly.
Whenever you create a Catalog, the decision tree is executed to find suitable configs for models and classes for distributions.
Whenever you build a model, the config is compiled into a model.

API philosophy
--------------

Catalogs attempt to encapsulate most complexity around models inside the :py:class:`~ray.rllib.core.models.base.Encoder`.
This means that recurrency, attention and other special cases are fully handles inside the Encoder and are transparent
to other components.
Encoders are the only components that the Catalog base class builds.
This is because many algorithms require custom heads and distributions but most of them can use the same encoders.
The Catalog API is designed such that interaction usually happens in two stages:

- Instantiate a Catalog. This executes the decision tree.
- Access decided components through methods s.a. :py:meth:`~ray.rllib.core.models.catalog.Catalog.get_action_dist_cls` or :py:meth:`~ray.rllib.core.models.catalog.Catalog.build_encoder`.


Write a Catalog from scratch
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You only need this when you want to write a new Algorithm under RLlib.
Note that writing an Algorithm does not strictly require writing a new Catalog but you can use Catalogs as a tool to create
the fitting default sub-components, such as models or distributions.
The following are typical requirements and steps for writing a new Catalog:

- Does the Algorithm need more that RLlib's standard Encoders? Overwrite :py:meth:`~ray.rllib.core.models.catalog.Catalog._get_encoder_config`.
- Does the Algorithm need an additional network? Write a method to build it. You can use RLlib's model configurations to build models from dimensions.
- Does the Algorithm need a custom distribution? Overwrite :py:meth:`~ray.rllib.core.models.catalog.Catalog._get_dist_cls_from_action_space`.
- Does the Algorithm not need an Encoder? Overwrite :py:meth:`~ray.rllib.core.models.catalog.Catalog.__post_init__`.

The following example shows our implementation of a Catalog for PPO:

.. dropdown:: **Catalog for PPORLModules**

    .. literalinclude:: ../../../rllib/algorithms/ppo/ppo_catalog.py
               :language: python
               :start-after: __sphinx_doc_begin__
               :end-before: __sphinx_doc_end__