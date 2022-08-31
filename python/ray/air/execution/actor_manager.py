import random
from collections import defaultdict
from typing import List, Tuple, Optional, Union

import ray
from ray.air.execution.action import Action, Continue, Stop
from ray.air.execution.actor_request import ActorRequest, ActorInfo
from ray.air.execution.controller import Controller
from ray.air.execution.resources.resource_manager import ResourceManager
from ray.air.execution.result import ExecutionResult, ExecutionException


class ActorManager:
    def __init__(self, controller: Controller, resource_manager: ResourceManager):
        self._controller = controller
        self._resource_manager = resource_manager

        self._actor_requests: List[ActorRequest] = []
        self._active_actors = set()
        self._actors_to_futures = defaultdict(set)
        self._futures_to_actors = {}

    def is_finished(self) -> bool:
        return self._controller.is_finished()

    def step_until_finished(self):
        while not self.is_finished():
            self.step()

    def step(self):
        self._handle_next_future()

        self._request_actions()

        self._get_new_actor_requests()

        self._start_new_actors()

    def _handle_next_future(self):
        actor_info, future = self._get_next_future()

        if not actor_info:
            return

        result = self._resolve(future)

        self._futures_to_actors.pop(future)
        self._actors_to_futures[actor_info].remove(future)

        if isinstance(result, ExecutionException):
            action = self._controller.actor_failed(
                actor_info, exception=result.exception
            )
            self._act_on_action(actor_info=actor_info, action=action)
        else:
            self._controller.actor_results(actor_infos=[actor_info], results=[result])

    def _get_next_future(self) -> Tuple[Optional[ActorInfo], Optional[ray.ObjectRef]]:
        actor_futures = list(self._futures_to_actors.keys())
        resource_futures = self._resource_manager.get_resource_futures()

        random.shuffle(actor_futures)

        # Prioritize resource futures
        futures = resource_futures + actor_futures

        ready, not_ready = ray.wait(futures, timeout=0, num_returns=1)
        if not ready:
            return None, None

        next_future = ready[0]

        # Todo: handle resource futures
        return self._futures_to_actors.get(next_future), next_future

    def _resolve(
        self, future: Union[ray.ObjectRef, List[ray.ObjectRef]]
    ) -> ExecutionResult:
        try:
            result = ray.get(future)
        except Exception as e:
            result = ExecutionException(e)
        return result

    def _get_new_actor_requests(self):
        for actor_request in self._controller.get_actor_requests():
            self._actor_requests.append(actor_request)
            self._resource_manager.request_resources(actor_request.resources)

    def _start_new_actors(self):
        new_actor_requests = []
        new_actors = []
        for actor_request in self._actor_requests:
            if self._resource_manager.has_resources_ready(actor_request.resources):
                ready_resource = self._resource_manager.acquire_resources(
                    actor_request.resources
                )
                remote_actor_cls = ray.remote(actor_request.cls)
                annotated_actor_cls = ready_resource.annotate_remote_objects(
                    [remote_actor_cls]
                )
                actor = annotated_actor_cls.remote(**actor_request.kwargs)
                actor_info = ActorInfo(
                    request=actor_request, actor=actor, used_resource=ready_resource
                )
                self._active_actors.add(actor_info)
                new_actors.append(actor_info)
            else:
                new_actor_requests.append(actor_request)

        for actor_info in new_actors:
            action = self._controller.actor_started(actor_info)
            self._act_on_action(actor_info=actor_info, action=action)

        self._actor_requests = new_actor_requests

    def _request_actions(self):
        actor_actions = self._controller.get_actions()
        for actor_info, actions in actor_actions.items():
            for action in actions:
                self._act_on_action(actor_info=actor_info, action=action)

    def _act_on_action(self, actor_info: ActorInfo, action: Action):
        if isinstance(action, Stop):
            # remove futures
            futures = self._actors_to_futures.pop(actor_info)
            for future in futures:
                self._futures_to_actors.pop(future)
            # remove actor
            ray.kill(actor_info.actor)
            self._resource_manager.return_resources(actor_info.used_resource)
            self._controller.actor_stopped(actor_info=actor_info)
        elif isinstance(action, Continue):
            for future in action.futures:
                self._actors_to_futures[actor_info].add(future)
                self._futures_to_actors[future] = actor_info
