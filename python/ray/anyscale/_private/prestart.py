from typing import Dict, Optional, Union

import ray
from ray._private.utils import get_runtime_env_info, parse_runtime_env
from ray.runtime_env import RuntimeEnv
from ray.util.placement_group import PlacementGroup, PlacementGroupSchedulingStrategy

"""
Prestart APIs. The prestarted workers are binded to the runtime_env and job_id. A future
task/actor with the same runtime_env and job_id *can* be assigned to these prestarted
workers. Before the prestarted workers are assigned a task/actor, they are kept alive
for `keep_alive_s` seconds.
"""


def prestart_workers_on_local_node(
    runtime_env: Optional[Union[Dict, RuntimeEnv]],
    keep_alive_s: int,
    num_workers: int,
):
    """
    Prestart workers on the local node. This function is async: there's no guarantee
    the workers are started when this function returns.
    """
    runtime_env = parse_runtime_env(runtime_env)
    if runtime_env is not None:
        serialized_runtime_env_info = get_runtime_env_info(
            runtime_env,
            is_job_runtime_env=False,
            serialize=True,
        )
    else:
        serialized_runtime_env_info = "{}"
    core_worker = ray._private.worker.global_worker.core_worker
    core_worker.prestart_workers(serialized_runtime_env_info, keep_alive_s, num_workers)


def prestart_workers_on_pg_bundles(
    pg: PlacementGroup,
    runtime_env: Optional[Union[Dict, RuntimeEnv]],
    keep_alive_s: int,
    num_workers_per_bundle: int,
):
    """
    Makes 1 prestart request to each bundle's raylet by sending 1 Ray task to each
    bundle. Returns a list of ObjectRefs to each bundle's task. However the completion
    of the tasks doesn't guarantee the workers are started.
    """
    objs = []
    for i in range(pg.bundle_count):
        objs.append(
            ray.remote(prestart_workers_on_local_node)
            .options(
                num_cpus=0,
                scheduling_strategy=PlacementGroupSchedulingStrategy(
                    placement_group=pg,
                    placement_group_bundle_index=i,
                ),
            )
            .remote(runtime_env, keep_alive_s, num_workers_per_bundle)
        )
    return objs
