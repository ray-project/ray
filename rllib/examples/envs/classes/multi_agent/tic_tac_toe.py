# __sphinx_doc_1_begin__
import gymnasium as gym
import numpy as np

from ray.rllib.env.multi_agent_env import MultiAgentEnv


class TicTacToe(MultiAgentEnv):
    """A two-player game in which any player tries to complete one row in a 3x3 field.

    The observation space is Box(0.0, 1.0, (9,)), where each index represents a distinct
    field on a 3x3 board and values of 0.0 mean the field is empty, -1.0 means
    the opponend owns the field, and 1.0 means we occupy the field:
    ----------
    | 0| 1| 2|
    ----------
    | 3| 4| 5|
    ----------
    | 6| 7| 8|
    ----------

    The action space is Discrete(9) and actions landing on an already occupied field
    are simply ignored (and thus useless to the player taking these actions).

    Once a player completes a row, they receive +1.0 reward, the losing player receives
    -1.0 reward. In all other cases, both players receive 0.0 reward.
    """

    # __sphinx_doc_1_end__

    # __sphinx_doc_2_begin__
    def __init__(self, config=None):
        super().__init__()

        # Define the agents in the game.
        self.agents = self.possible_agents = ["player1", "player2"]

        # Each agent observes a 9D tensor, representing the 3x3 fields of the board.
        # A 0 means an empty field, a 1 represents a piece of player 1, a -1 a piece of
        # player 2.
        self.observation_spaces = {
            "player1": gym.spaces.Box(-1.0, 1.0, (9,), np.float32),
            "player2": gym.spaces.Box(-1.0, 1.0, (9,), np.float32),
        }
        # Each player has 9 actions, encoding the 9 fields each player can place a piece
        # on during their turn.
        self.action_spaces = {
            "player1": gym.spaces.Discrete(9),
            "player2": gym.spaces.Discrete(9),
        }

        self.board = None
        self.current_player = None

    # __sphinx_doc_2_end__

    # __sphinx_doc_3_begin__
    def reset(self, *, seed=None, options=None):
        self.board = [
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
        ]
        # Pick a random player to start the game.
        self.current_player = np.random.choice(["player1", "player2"])
        # Return observations dict (only with the starting player, which is the one
        # we expect to act next).
        return {
            self.current_player: np.array(self.board, np.float32),
        }, {}

    # __sphinx_doc_3_end__

    # __sphinx_doc_4_begin__
    def step(self, action_dict):
        action = action_dict[self.current_player]

        # Create a rewards-dict (containing the rewards of the agent that just acted).
        rewards = {self.current_player: 0.0}
        # Create a terminateds-dict with the special `__all__` agent ID, indicating that
        # if True, the episode ends for all agents.
        terminateds = {"__all__": False}

        opponent = "player1" if self.current_player == "player2" else "player2"

        # Penalize trying to place a piece on an already occupied field.
        if self.board[action] != 0:
            rewards[self.current_player] -= 5.0
        # Change the board according to the (valid) action taken.
        else:
            self.board[action] = 1 if self.current_player == "player1" else -1

            # After having placed a new piece, figure out whether the current player
            # won or not.
            if self.current_player == "player1":
                win_val = [1, 1, 1]
            else:
                win_val = [-1, -1, -1]
            if (
                # Horizontal win.
                self.board[:3] == win_val
                or self.board[3:6] == win_val
                or self.board[6:] == win_val
                # Vertical win.
                or self.board[0:7:3] == win_val
                or self.board[1:8:3] == win_val
                or self.board[2:9:3] == win_val
                # Diagonal win.
                or self.board[::3] == win_val
                or self.board[2:7:2] == win_val
            ):
                # Final reward is +5 for victory and -5 for a loss.
                rewards[self.current_player] += 5.0
                rewards[opponent] = -5.0

                # Episode is done and needs to be reset for a new game.
                terminateds["__all__"] = True

            # The board might also be full w/o any player having won/lost.
            # In this case, we simply end the episode and none of the players receives
            # +1 or -1 reward.
            elif 0 not in self.board:
                terminateds["__all__"] = True

        # Flip players and return an observations dict with only the next player to
        # make a move in it.
        self.current_player = opponent

        return (
            {self.current_player: np.array(self.board, np.float32)},
            rewards,
            terminateds,
            {},
            {},
        )


# __sphinx_doc_4_end__
