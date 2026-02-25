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

    # Winning line indices: rows, columns, and diagonals.
    WIN_LINES = [
        [0, 1, 2],  # rows
        [3, 4, 5],
        [6, 7, 8],
        [0, 3, 6],  # cols
        [1, 4, 7],
        [2, 5, 8],
        [0, 4, 8],  # diagonals
        [2, 4, 6],
    ]

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
        self.max_timesteps = 30

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
        opponent = "player2" if self.current_player == "player1" else "player1"
        self.timestep += 1

        # Invalid move: penalize and return without changing board.
        if self.board[action] != 0:
            truncated = self.timestep >= self.max_timesteps
            board_arr = np.array(self.board, np.float32)
            if truncated:
                return (
                    {self.current_player: board_arr, opponent: board_arr * -1},
                    {self.current_player: -0.5, opponent: 0.0},
                    {"__all__": False},
                    {"__all__": True},
                    {},
                )
            else:
                reward = {self.current_player: -0.5}
                self.current_player = opponent
                return (
                    {opponent: board_arr * -1},
                    reward,
                    {"__all__": False},
                    {"__all__": False},
                    {},
                )

        # Place the piece on the board.
        self.board[action] = 1
        board_arr = np.array(self.board, np.float32)

        # Check for win.
        if any(all(self.board[i] == 1 for i in line) for line in self.WIN_LINES):
            return (
                {self.current_player: board_arr, opponent: board_arr * -1},
                {self.current_player: 1.0, opponent: -1.0},
                {"__all__": True},
                {"__all__": False},
                {},
            )

        # Check for draw (board full, no winner).
        if 0 not in self.board:
            return (
                {self.current_player: board_arr, opponent: board_arr * -1},
                {self.current_player: 0.0, opponent: 0.0},
                {"__all__": True},
                {"__all__": False},
                {},
            )

        # Check for truncation.
        if self.timestep >= self.max_timesteps:
            return (
                {self.current_player: board_arr, opponent: board_arr * -1},
                {self.current_player: 0.0, opponent: 0.0},
                {"__all__": False},
                {"__all__": True},
                {},
            )

        # Continue game: flip board and switch player.
        self.board = [-x for x in self.board]
        reward = {self.current_player: 0.0}
        self.current_player = opponent

        return (
            {opponent: np.array(self.board, np.float32)},
            reward,
            {"__all__": False},
            {"__all__": False},
            {},
        )

    # __sphinx_doc_4_end__

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


# if __name__ == "__main__":
#     from ray.rllib.algorithms import PPOConfig
#     from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner
#     config = (
#         PPOConfig()
#         .environment(TicTacToe)
#         .multi_agent(policies={"P0"}, policy_mapping_fn=lambda _, __: "P0")
#     )
#     env_runner = MultiAgentEnvRunner(config)
#     episode = env_runner.sample(num_episodes=1, random_actions=True)[0]
#     print(f'{episode=}')
#     for agent_id, sa_episode in episode.agent_episodes.items():
#         print(f'{agent_id=}')
#         observations = sa_episode.get_observations()
#         actions = sa_episode.get_actions()
#         rewards = sa_episode.get_rewards()
#         is_terminated = sa_episode.is_terminated
#         is_truncated = sa_episode.is_truncated
#
#         for obs, action, reward, next_obs in zip(observations[:-1], actions, rewards, observations[1:]):
#             print(f'{obs=}, {action=}, {reward=}, {next_obs=}')
#         print(f'{is_terminated=}, {is_truncated=}')
#         print()
#
#     final_obs = episode.agent_episodes["player1"].get_observations(-1)
#     env = TicTacToe()
#     env.board = final_obs.astype(int)
#     print('X is current player, O is the opponent')
#     print(env.render())
