"""Experimental actor-only execution backend for Ray Data.

This package mirrors the structure of ``ray.data._internal`` and holds the
backend-specific overrides that are active only when the actor-only backend is
enabled (``RAY_DATA_ACTOR_ONLY_BACKEND``, see
``ray.data._internal.execution.execution_flags``).

Each module here subclasses the corresponding base class and overrides only the
behavior that differs under the actor-only backend. The overrides are wired in
through existing seams (the operator factory in
``ray.data._internal.execution.operators.__init__`` and the physical ruleset in
``ray.data._internal.logical.optimizers``) rather than by monkeypatching.
"""
