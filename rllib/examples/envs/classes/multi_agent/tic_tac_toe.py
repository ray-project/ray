# __sphinx_doc_1_begin__
import random

import gymnasium as gym
import numpy as np

from ray.rllib.env.multi_agent_env import MultiAgentEnv


class TicTacToe(MultiAgentEnv):
    """A two-player game in which any player tries to complete one row in a 3x3 field.

    The observation space is Box(-1.0, 1.0, (9,)), where each index represents a distinct
    field on a 3x3 board. From the current player's perspective: 1.0 means we occupy the
    field, -1.0 means the opponent owns the field, and 0.0 means the field is empty:
    ----------
    | 0| 1| 2|
    ----------
    | 3| 4| 5|
    ----------
    | 6| 7| 8|
    ----------

    The action space is Discrete(9). Actions landing on an already occupied field
    result in a -1.0 penalty for the player taking the invalid action.

    Once a player completes a row, they receive +1.0 reward, the losing player receives
    -1.0 reward. A draw results in 0.0 reward for both players.
    """

    # __sphinx_doc_1_end__

    # __sphinx_doc_2_begin__
    def __init__(self, config=None):
        super().__init__()

        # Define the agents in the game.
        self.agents = self.possible_agents = ["player1", "player2"]

        # Each agent observes a 9D tensor, representing the 3x3 fields of the board.
        # From the current player's perspective: 1 means our piece, -1 means opponent's
        # piece, 0 means empty. The board is flipped after each turn.
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
        self.max_timesteps = 20

        self.board = None
        self.current_player = None
        self.timestep = 0

    # __sphinx_doc_2_end__

    # __sphinx_doc_3_begin__
    def reset(self, *, seed=None, options=None):
        self.board = [0] * 9

        # Pick a random player to start the game and reset the current timesteps.
        self.current_player = random.choice(self.agents)
        self.timestep = 0

        # Return observations dict (only with the starting player, which is the one
        # we expect to act next).
        return {self.current_player: np.array(self.board, np.float32)}, {}

    # __sphinx_doc_3_end__

    # __sphinx_doc_4_begin__
    def step(self, action_dict):
        action = action_dict[self.current_player]

        opponent = "player1" if self.current_player == "player2" else "player2"

        # Penalize trying to place a piece on an already occupied field.
        if self.board[action] != 0:
            rewards = {self.current_player: -1.0}
            terminations = {"__all__": False}
        else:
            # Change the board according to the (valid) action taken.
            # For the next turn we "flip" the tokens so that the agent is always playing with the 1 vs the -1
            self.board[action] = 1

            # After having placed a new piece, figure out whether the current player won or not.
            win_val = [1, 1, 1]

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
                or self.board[::4] == win_val
                or self.board[2:7:2] == win_val
            ):
                # Final reward is +1 for victory and -1 for a loss.
                rewards = {
                    self.current_player: 1.0,
                    opponent: -1.0,
                }

                # Episode is done and needs to be reset for a new game.
                terminations = {"__all__": True}

            # The board might also be full w/o any player having won/lost.
            # In this case, we simply end the episode and none of the players receives
            # +1 or -1 reward.
            elif 0 not in self.board:
                rewards = {
                    self.current_player: 0.0,
                    opponent: 0.0,
                }
                terminations = {"__all__": True}
            # Truncate the agents after `max_timesteps`
            elif self.timestep >= self.max_timesteps:
                rewards = {
                    self.current_player: -0.5,
                    opponent: -0.5,
                }
                obs = {
                    self.current_player: np.array(self.board, np.float32),
                    opponent: np.array(self.board, np.float32) * -1,
                }
                return (
                    obs,
                    rewards,
                    {"__all__": False},
                    {"__all__": True},
                    {},
                )
            # Standard move with no reward
            else:
                rewards = {self.current_player: 0.0}
                terminations = {"__all__": False}

        # Flip players and board so the next player sees their pieces as 1.
        self.current_player = opponent
        self.timestep += 1
        self.board = [-x for x in self.board]

        return (
            {self.current_player: np.array(self.board, np.float32)},
            rewards,
            terminations,
            {},
            {},
        )

    def render(self) -> str:
        """Render the current board state as an ASCII grid.

        Returns:
            A string representation of the board where:
            - 'X' represents the current player's pieces
            - 'O' represents opponent player's pieces
            - ' ' represents empty fields
        """
        symbols = {0: " ", 1: "X", -1: "O"}
        rows = []
        for i in range(3):
            row_cells = [symbols[self.board[i * 3 + j]] for j in range(3)]
            rows.append(" " + " | ".join(row_cells) + " ")
        separator = "-----------"
        return "\n" + f"\n{separator}\n".join(rows) + "\n"


# __sphinx_doc_4_end__
