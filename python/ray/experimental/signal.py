from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import defaultdict

import ray
import ray.cloudpickle as cloudpickle


class Signal(object):
    """Base class for Ray signals."""
    pass


class ErrorSignal(Signal):
    """Signal raised if an exception happens in a task or actor method."""
    def __init__(self, error):
        self.error = error
    def get_error(self):
        return self.error


def _get_task_id(source):
    """Return the task id associated to the generic source of the signal.

    Args:
        source: source of the signal, it can be either an object id, task id,
            or actor handle.

    Returns:
        - If source is an object id, return id of task which creted object.
        - If source is an actor handle, return id of actor's task creator.
        - If source is a task id, return same task id.
    """
    if type(source) is ray.actor.ActorHandle:
        return ray._raylet.compute_task_id(
            source._ray_actor_creation_dummy_object_id)
    else:
        if type(source) is ray.TaskID:
            return source
        else:
            return ray._raylet.compute_task_id(source)


def send(signal, source_id=None):
    """Send signal on behalf of source.

    Each signal is identified by (source, index), where index is incremented
    every time a signal is sent, starting from 1.

    Args:
        signal: signal to be sent.
        source: If empty, initialize to the id of the task/actor
                invoking this function.
    """
    if source_id is None:
        if hasattr(ray.worker.global_worker, "actor_creation_task_id"):
            global_worker = ray.worker.global_worker
            source_key = global_worker.actor_creation_task_id.hex()
        else:
            # No actors; this function must have been called from a task
            source_key = ray.worker.global_worker.current_task_id.hex()
    else:
        source_key = source_id.hex()

    encoded_signal = ray.utils.binary_to_hex(cloudpickle.dumps(signal))
    ray.worker.global_worker.redis_client.execute_command(
        "XADD " + source_key + " * signal " + encoded_signal)


def receive(sources, timeout=10**12):
    """Get all signals from each source in sources.

    A source can be an object id or an actor handle.
    For each source S, this function returns all signals
    associated to S since the last receive() or
    forget() were invoked on S. If this is the first call on
    S, this function returns all past signals associaed with S so far.

    Args:
        sources: list of sources from which caller waits for signals.
        timeout: time it waits for new signals to be generated. If none,
            return when timeout expires. Measured in seconds.

    Returns:
        The list of signals generated for each source in sources.
        They are returned as a list of pairs (source, signal). There can be
        more than a signal associated with the same source.
    """
    if not hasattr(ray.worker.global_worker, "signal_counters"):
        ray.worker.global_worker.signal_counters = defaultdict(lambda: b"0")

    signal_counters = ray.worker.global_worker.signal_counters

    # Construct the redis query.
    query = "XREAD BLOCK "
    # Multiply by 1000x since timeout is in sec and redis expects ms.
    query += str(1000 * timeout)
    query += " STREAMS "
    query += " ".join([_get_task_id(source).hex() for source in sources])
    query += " "
    query += " ".join([
        ray.utils.decode(signal_counters[_get_task_id(source)])
        for source in sources
    ])

    answers = ray.worker.global_worker.redis_client.execute_command(query)
    if not answers:
        return []
    # There will be one answer per source. If there is no signal for a given
    # source, redis will return an empty list for that source.
    assert len(answers) == len(sources)

    results = []
    # Decoding is a little bit involved. Iterate through all the sources:
    for i, answer in enumerate(answers):
        # Make sure the answer corresponds to the source
        assert ray.utils.decode(answer[0]) == _get_task_id(sources[i]).hex()
        # The list of results for that source is stored in answer[1]
        for r in answer[1]:
            # Now it gets tricky: r[0] is the redis internal sequence id
            signal_counters[_get_task_id(sources[i])] = r[0]
            # r[1] contains a list with elements (key, value), in our case
            # we only have one key "signal" and the value is the signal.
            signal = cloudpickle.loads(ray.utils.hex_to_binary(r[1][1]))
            results.append((sources[i], signal))

    return results


def forget(sources):
    """Ignore all previous signals associated with each source S.

    The index of the next expected signal from S is set to the last
    signal's index plus 1. This means that the next receive() on S
    will only get the signals generated after this function was invoked.

    Args:
        sources: list of sources whose past signals are forgotten.
    """
    # Just read all signals sent by all sources so far.
    # This will results in ignoring these signals.
    receive(sources, timeout=0)


def reset():
    """
    Reset the worker state associated with any signals that this worker
    has received so far.
    If the worker calls receive() on a source_id next, it will get all the
    signals generated by (or on behalf of) source_id from the beginning.
    """
    ray.worker.global_worker.signal_counters = defaultdict(lambda: b"0")
