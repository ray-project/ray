from typing import Literal, Optional

import ray
from ray._private.state import state


def actors(
    actor_id: Optional[str] = None,
    job_id: Optional[ray.JobID] = None,
    actor_state_name: Optional[
        Literal[
            "DEPENDENCIES_UNREADY",
            "PENDING_CREATION",
            "ALIVE",
            "RESTARTING",
            "DEAD",
        ]
    ] = None,
):
    """Fetch actor info for one or more actor IDs (for debugging only).

    Args:
        actor_id: A hex string of the actor ID to fetch information about. If
            this is None, then all actor information is fetched.
            If this is not None, `job_id` and `actor_state_name`
            will not take effect.
        job_id: To filter actors by job_id, which is of type `ray.JobID`.
            You can use the `ray.get_runtime_context().job_id` function
            to get the current job ID
        actor_state_name: To filter actors based on actor state,
            which can be one of the following: "DEPENDENCIES_UNREADY",
            "PENDING_CREATION", "ALIVE", "RESTARTING", or "DEAD".
    Returns:
        Information about the actors.
    """
    return state.actor_table(
        actor_id=actor_id, job_id=job_id, actor_state_name=actor_state_name
    )
