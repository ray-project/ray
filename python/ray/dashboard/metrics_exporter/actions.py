import logging
import typing

from ray.dashboard.metrics_exporter.schema import ActionType, KillAction

logger = logging.getLogger(__name__)


class ActionHandler:
    def __init__(self, dashboard_controller):
        self.dashboard_controller = dashboard_controller

    def handle_kill_action(self, action: dict):
        kill_action = KillAction.parse_obj(action)
        self.dashboard_controller.kill_actor(
            kill_action.actor_id, kill_action.ip_address, kill_action.port)

    def handle_actions(self, actions: typing.List[dict]):
        for action in actions:
            action_type = action.get("type", None)

            if action_type == ActionType.KILL_ACTOR:
                self.handle_kill_action(action)
            else:
                logger.warning("Action type {} has been received, but "
                               "action handler doesn't know how to handle "
                               "them. It will skip processing the request. "
                               "Plesae raise an issue if you see this problem."
                               .format(action_type))
                continue
