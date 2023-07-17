

.. _catalog-reference-docs:

.. include:: /_includes/rllib/rlmodules_rollout.rst

Catalog API
===========

Basic usage
-----------

Use the following basic API to get a default ``encoder`` or ``action distribution``
out of Catalog. You can inherit from Catalog and modify the following methods to hack a Catalog.
Algorithm-specific implementations of Catalog have additional methods,
for example, for building ``heads``.

.. currentmodule:: ray.rllib.core.models.catalog

.. autosummary::
    :toctree: doc/

    Catalog
    Catalog.build_encoder
    Catalog.get_action_dist_cls
    Catalog.get_tokenizer_config


Advanced usage
--------------

The following methods and attributes are used internally by the Catalog to build the default models.
You can override these to enhance Catalogs.

.. autosummary::
    :toctree: doc/

    Catalog.latent_dims
    Catalog._determine_components_hook
    Catalog._get_encoder_config
    Catalog._get_dist_cls_from_action_space
