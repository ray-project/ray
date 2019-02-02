from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import hashlib
import time

import ray

START_SIGNAL_COUNTER = 10000

class Signal(object):
    """Signal object"""
    pass

class DoneSignal(Signal):
    pass

class ErrorSignal(Signal):
    def __init__(self, error):
        self.error = error

def _get_task_id(source):
    """Return the task id associated to the generic source of the signal.
       Args:
         source: source of the signal, it can be either an object id, task id, or actor handle.
       Returns:
         - If source is an object id, return id of task which creted object.
         - If source is an actor handle, return id of actor's task creator.
         - If source is a task id, return same task id.
    """
    if  type(source) is ray.actor.ActorHandle:
        return ray._raylet.compute_task_id(source._ray_actor_creation_dummy_object_id)
    else:
        # When called from send() via _get_signa_id() source is a task id.
        if type(source) is ray.TaskID:
            return source
        else:
            return ray._raylet.compute_task_id(source)

def _get_signal_id(source, counter):
    return ray._raylet.compute_signal_id(_get_task_id(source), counter)

def send(signal, source_id = None):
    """Send signal on behalf of source.
    Each signal is identified by (source, index), where index is incremented
    every time a signal is sent, starting from 1.
    Args:
        signal: signal to be sent.
        source: If empty, initialize to the id of the task/actor
                invoking this function.
    """
    if source_id == None:
        if hasattr(ray.worker.global_worker, "actor_creation_task_id"):
            source_key = ray.worker.global_worker.actor_creation_task_id.binary()
        else:
            # No actors; this function must have been called from a task
            source_key = ray.worker.global_worker.current_task_id.binary()
    else:
        source_key = source_id.binary()

    index = ray.worker.global_worker.redis_client.incr(source_key)
    if index < START_SIGNAL_COUNTER:
        ray.worker.global_worker.redis_client.set(source_key, START_SIGNAL_COUNTER)
        index = START_SIGNAL_COUNTER

    object_id = _get_signal_id(ray.ObjectID(source_key), index)
    ray.worker.global_worker.store_and_register(object_id, signal)

def receive(sources, timeout=float('inf')):
    """Get all signals from each source in sources.
    A source can be an object id or an actor handle.
    For each source S, this function returns all signals
    associated to S since the last receive() or
    forget() were invoked on S. If this is the first call on
    S, this function returns all past signals associaed with S so far.
    Args:
        sources: list of generic sources from which caller waits for signals.
        timeout: time it waits for new signals to be generated. If none,
                 return when timeout expires. Measured in seconds.
    Returns:
        The list of signals generated for each source in sources.
        They are returned as a list of pairs (source, signal). There can be
        more than a signal associated with the same source.
    """
    if not hasattr(ray.worker.global_worker, "signal_counters"):
        ray.worker.global_worker.signal_counters = dict()

    signal_counters = ray.worker.global_worker.signal_counters
    results = []
    previous_time = time.time()
    remaining_time = timeout

    # Store the mapping between signal's source and the task id associated
    # with tha source in the sources_from_task_id dictionary.
    sources_from_task_id = dict()
    for s in sources:
        task_id = _get_task_id(s)
        if task_id not in sources_from_task_id.keys():
            sources_from_task_id[task_id] = []
        # Multiple sources can map to the same task id. One example is a task
        # returning multiple objects, and these objects are used as sources
        # in receive(). In this case, each returned object will map to the same task.
        sources_from_task_id[task_id].append(s)
        if not task_id in signal_counters:
            signal_counters[task_id] = START_SIGNAL_COUNTER

    # Store the reverse mapping from a signal to the id of the task
    # generating that signal in the task_id_from_signal_id dictionary.
    task_id_from_signal_id = dict()
    for task_id  in sources_from_task_id.keys():
        signal_id = _get_signal_id(task_id , signal_counters[task_id ])
        task_id_from_signal_id[signal_id] = task_id

    while True:
        ready_ids, _ = ray.wait(task_id_from_signal_id .keys(),
            num_returns=len(task_id_from_signal_id .keys()), timeout=0)
        if len(ready_ids) > 0:
            for signal_id in ready_ids:
                signal = ray.get(signal_id)
                task_id  = task_id_from_signal_id [signal_id]
                if isinstance(signal, Signal):
                    for source in sources_from_task_id[task_id ]:
                        results.append((source, signal))
                    if type(signal) == DoneSignal:
                        del signal_counters[task_id ]

                # We read this signal so forget it.
                del task_id_from_signal_id [signal_id]

                if task_id  in signal_counters:
                    # Compute id of the next expected signal for this source id.
                    signal_counters[task_id ] += 1
                    signal_id = _get_signal_id(task_id , signal_counters[task_id ])
                    task_id_from_signal_id [signal_id] = task_id
                else:
                    break
            current_time = time.time()
            remaining_time -= (current_time - previous_time)
            previous_time = current_time
            if remaining_time < 0:
                break
        else:
            break


    if (remaining_time < 0) or (len(results) > 0):
        return results

    # Thee are no past signals for any source passed to this function, and the
    # timeout has not expired yet. Wait for future signals or until timeout expires.
    ready_ids, _ = ray.wait(task_id_from_signal_id .keys(), 1, timeout=remaining_time)

    for ready_id in ready_ids:
        signal_counters[task_id_from_signal_id [ready_id]] += 1
        signal = ray.get(signal_id)
        if isinstance(signal, Signal):
            for source in sources_from_task_id[task_id ]:
                results.append((source, signal))
            if type(signal) == DoneSignal:
                del signal_counters[task_id ]

    return results

def forget(sources):
    """Ignore all previous signals associated with each source S corresponding to
    a generic source in sources. The index of the next expected signal from S is
    set to the last signal's index plus 1. This means that the next receive()
    on S will only get the signals generated after this function was invoked.
    Args:
        sources: list of sources whose past signals are forgotten.
    """
    if not hasattr(ray.worker.global_worker, "signal_counters"):
        ray.worker.global_worker.signal_counters = dict()
    signal_counters = ray.worker.global_worker.signal_counters

    for s in sources:
        source_id = _get_task_id(s)
        source_key = source_id.binary()
        value = ray.worker.global_worker.redis_client.get(source_key)
        if value != None:
            signal_counters[source_id] = int(value) + 1
        else:
            signal_counters[source_id] = START_SIGNAL_COUNTER

def reset():
    """
    Reset the worker state associated with any signals that this worker
    has received so far.
    If the worker calls receive() on a source_id next, it will get all the
    signals generated by (or on behalf of) source_id from the beginning.
    """
    ray.worker.global_worker.signal_counters = dict()
