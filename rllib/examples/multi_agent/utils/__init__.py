import sys

from ray.rllib.examples.multi_agent.utils.self_play_callback import SelfPlayCallback
from ray.rllib.examples.multi_agent.utils.self_play_league_based_callback import (
    SelfPlayLeagueBasedCallback,
)
from ray.rllib.examples.multi_agent.utils.self_play_callback_old_api_stack import (
    SelfPlayCallbackOldAPIStack,
)
from ray.rllib.examples.multi_agent.utils.self_play_league_based_callback_old_api_stack import (  # noqa
    SelfPlayLeagueBasedCallbackOldAPIStack,
)


def ask_user_for_action(time_step):
    """Asks the user for a valid action on the command line and returns it.

    Re-queries the user until she picks a valid one.

    Args:
        time_step: The open spiel Environment time-step object.
    """
    pid = time_step.observations["current_player"]
    legal_moves = time_step.observations["legal_actions"][pid]
    choice = -1
    while choice not in legal_moves:
        print("Choose an action from {}:".format(legal_moves))
        sys.stdout.flush()
        choice_str = input()
        try:
            choice = int(choice_str)
        except ValueError:
            continue
    return choice


__all__ = [
    "ask_user_for_action",
    "SelfPlayCallback",
    "SelfPlayLeagueBasedCallback",
    "SelfPlayCallbackOldAPIStack",
    "SelfPlayLeagueBasedCallbackOldAPIStack",
]
