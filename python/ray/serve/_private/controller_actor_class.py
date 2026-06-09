from typing import Optional

import ray
from ray._common.constants import HEAD_NODE_RESOURCE_NAME
from ray.serve._private.constants import (
    CONTROLLER_MAX_CONCURRENCY,
    RAY_SERVE_ENABLE_TASK_EVENTS,
    SERVE_CONTROLLER_NAME,
    SERVE_NAMESPACE,
)
from ray.serve._private.controller import ServeController
from ray.serve.config import ControllerOptions

# NOTE: Please read carefully before changing!
#
# This is a common extension point, therefore it should be changed as a
# Developer API, ie the method should not be renamed, have its API modified
# w/o substantial enough justification.
#
# It lives here rather than in default_impl so the controller actor class can
# be imported eagerly: default_impl is imported by the runtime objects (proxy,
# handle, ...), so importing the controller from default_impl would be a
# circular import.


def get_controller_impl(controller_options: Optional[ControllerOptions] = None):
    """Build the Ray actor class for the Serve controller.

    ``controller_options`` is the validated ``ControllerOptions`` model from
    ``serve.start`` / ``serve.run`` / the YAML schema. Today only its
    ``runtime_env`` field is consumed; future fields (num_cpus, resources,
    max_concurrency overrides) slot in here.
    """
    actor_options = dict(
        name=SERVE_CONTROLLER_NAME,
        namespace=SERVE_NAMESPACE,
        num_cpus=0,
        lifetime="detached",
        max_restarts=-1,
        max_task_retries=-1,
        resources={HEAD_NODE_RESOURCE_NAME: 0.001},
        max_concurrency=CONTROLLER_MAX_CONCURRENCY,
        enable_task_events=RAY_SERVE_ENABLE_TASK_EVENTS,
    )
    if controller_options is not None and controller_options.runtime_env:
        # The validator on ControllerOptions guarantees this is a dict
        # containing only the ``env_vars`` key with str->str entries.
        actor_options["runtime_env"] = controller_options.runtime_env

    return ray.remote(**actor_options)(ServeController)
