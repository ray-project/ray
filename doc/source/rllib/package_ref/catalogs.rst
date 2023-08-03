

.. _catalog-reference-docs:

.. include:: /_includes/rllib/rlmodules_rollout.rst

Catalog API
===========

Basic usage
-----------

Use the following basic API to get a default ``encoder`` or ``action distribution``
out of Catalog. You can inherit from Catalog and modify the following methods to
directly inject custom components into a given RLModule.
Algorithm-specific implementations of Catalog have additional methods,
for example, for building ``heads``.

.. currentmodule:: ray.rllib.core.models.catalog

.. autosummary::
    :toctree: doc/

    Catalog
    Catalog.build_encoder
    Catalog.get_action_dist_cls
    Catalog.get_preprocessor


Advanced usage
--------------

The following methods are used internally by the Catalog to build the default models.

.. autosummary::
    :toctree: doc/

    Catalog.latent_dims
    Catalog.__post_init__
    Catalog.get_encoder_config
    Catalog.get_tokenizer_config
