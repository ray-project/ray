from collections import defaultdict, deque

import numpy as np

import ray
from ray.experimental.serve.utils import get_custom_object_id, logger


class Query:
    def __init__(self, request_body, result_object_id=None):
        self.request_body = request_body
        if result_object_id is None:
            self.result_object_id = get_custom_object_id()
        else:
            self.result_object_id = result_object_id


class WorkIntent:
    def __init__(self, work_object_id=None):
        if work_object_id is None:
            self.work_object_id = get_custom_object_id()
        else:
            self.work_object_id = work_object_id


class CentralizedQueues:
    """A router that routes request to available workers.

    Router aceepts each request from the `enqueue_request` method and enqueues
    it. It also accepts worker request to work (called work_intention in code)
    from workers via the `dequeue_request` method. The traffic policy is used
    to match requests with their corresponding workers.

    Behavior:
        >>> # psuedo-code
        >>> queue = CentralizedQueues()
        >>> queue.enqueue_request('service-name', data)
        # nothing happens, request is queued.
        # returns result ObjectID, which will contains the final result
        >>> queue.dequeue_request('backend-1')
        # nothing happens, work intention is queued.
        # return work ObjectID, which will contains the future request payload
        >>> queue.link('service-name', 'backend-1')
        # here the enqueue_requester is matched with worker, request
        # data is put into work ObjectID, and the worker processes the request
        # and store the result into result ObjectID

    Traffic policy splits the traffic among different workers
    probabilistically:

    1. When all backends are ready to receive traffic, we will randomly
       choose a backend based on the weights assigned by the traffic policy
       dictionary.

    2. When more than 1 but not all backends are ready, we will normalize the
       weights of the ready backends to 1 and choose a backend via sampling.

    3. When there is only 1 backend ready, we will only use that backend.
    """

    def __init__(self):
        # service_name -> request queue
        self.queues = defaultdict(deque)

        # service_name -> traffic_policy
        self.traffic = defaultdict(dict)

        # backend_name -> worker queue
        self.workers = defaultdict(deque)

    def enqueue_request(self, service, request_data):
        query = Query(request_data)
        self.queues[service].append(query)
        self.flush()
        return query.result_object_id.binary()

    def dequeue_request(self, backend):
        intention = WorkIntent()
        self.workers[backend].append(intention)
        self.flush()
        return intention.work_object_id.binary()

    def link(self, service, backend):
        logger.debug("Link %s with %s", service, backend)
        self.traffic[service][backend] = 1.0
        self.flush()

    def set_traffic(self, service, traffic_dict):
        logger.debug("Setting traffic for service %s to %s", service,
                     traffic_dict)
        self.traffic[service] = traffic_dict
        self.flush()

    def flush(self):
        """In the default case, flush calls ._flush.

        When this class is a Ray actor, .flush can be scheduled as a remote
        method invocation.
        """
        self._flush()

    def _get_available_backends(self, service):
        backends_in_policy = set(self.traffic[service].keys())
        available_workers = {
            backend
            for backend, queues in self.workers.items() if len(queues) > 0
        }
        return list(backends_in_policy.intersection(available_workers))

    def _flush(self):
        for service, queue in self.queues.items():
            ready_backends = self._get_available_backends(service)

            while len(queue) and len(ready_backends):
                # Fast path, only one backend available.
                if len(ready_backends) == 1:
                    backend = ready_backends[0]
                    request, work = (queue.popleft(),
                                     self.workers[backend].popleft())
                    ray.worker.global_worker.put_object(
                        work.work_object_id, request)

                # We have more than one backend available.
                # We will roll a dice among the multiple backends.
                else:
                    backend_weights = np.array([
                        self.traffic[service][backend_name]
                        for backend_name in ready_backends
                    ])
                    # Normalize the weights to 1.
                    backend_weights /= backend_weights.sum()
                    chosen_backend = np.random.choice(
                        ready_backends, p=backend_weights).squeeze()

                    request, work = (
                        queue.popleft(),
                        self.workers[chosen_backend].popleft(),
                    )
                    ray.worker.global_worker.put_object(
                        work.work_object_id, request)

                ready_backends = self._get_available_backends(service)


@ray.remote
class CentralizedQueuesActor(CentralizedQueues):
    self_handle = None

    def register_self_handle(self, handle_to_this_actor):
        self.self_handle = handle_to_this_actor

    def flush(self):
        if self.self_handle:
            self.self_handle._flush.remote()
        else:
            self._flush()
