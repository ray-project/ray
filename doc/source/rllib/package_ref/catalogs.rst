

.. _catalog-reference-docs:

.. include:: /_includes/rllib/rlmodules_rollout.rst

Catalog API
===========

Basic usage
-----------

Use the following basic API to get a default ``encoder`` or ``action distribution``
out of Catalog. To change the catalog behavior, modify the following methods.
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

The following methods and attributes are used internally by the Catalog to build the default models. Only override them when you need more granular control.

.. autosummary::
    :toctree: doc/

    Catalog.latent_dims
    Catalog._determine_components_hook
    Catalog._get_encoder_config
    Catalog._get_dist_cls_from_action_space
