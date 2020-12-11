import time
import re
from collections import defaultdict

PYCLASSNAME_RE = re.compile(r"(.+?)\(")


def construct_actor_groups(actors):
    """actors is a dict from actor id to an actor or an
       actor creation task The shared fields currently are
       "actorClass", "actorId", and "state" """
    actor_groups = _group_actors_by_python_class(actors)
    stats_by_group = {
        name: _get_actor_group_stats(group)
        for name, group in actor_groups.items()
    }

    summarized_actor_groups = {}
    for name, group in actor_groups.items():
        summarized_actor_groups[name] = {
            "entries": group,
            "summary": stats_by_group[name]
        }
    return summarized_actor_groups


def actor_classname_from_task_spec(task_spec):
    return task_spec.get("functionDescriptor", {})\
                .get("pythonFunctionDescriptor", {})\
                .get("className", "Unknown actor class")


def _group_actors_by_python_class(actors):
    groups = defaultdict(list)
    for actor in actors.values():
        actor_class = actor["actorClass"]
        groups[actor_class].append(actor)
    return dict(groups)


def _get_actor_group_stats(group):
    state_to_count = defaultdict(lambda: 0)
    executed_tasks = 0
    min_timestamp = None
    num_timestamps = 0
    sum_timestamps = 0
    now = time.time() * 1000  # convert S -> MS
    for actor in group:
        state_to_count[actor["state"]] += 1
        if "timestamp" in actor:
            if not min_timestamp or actor["timestamp"] < min_timestamp:
                min_timestamp = actor["timestamp"]
            num_timestamps += 1
            sum_timestamps += now - actor["timestamp"]
        if "numExecutedTasks" in actor:
            executed_tasks += actor["numExecutedTasks"]
    if num_timestamps > 0:
        avg_lifetime = int((sum_timestamps / num_timestamps) / 1000)
        max_lifetime = int((now - min_timestamp) / 1000)
    else:
        avg_lifetime = 0
        max_lifetime = 0
    return {
        "stateToCount": state_to_count,
        "avgLifetime": avg_lifetime,
        "maxLifetime": max_lifetime,
        "numExecutedTasks": executed_tasks,
    }
