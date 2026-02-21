# __sphinx_doc_1_begin__
import gymnasium as gym

from ray.rllib.env.multi_agent_env import MultiAgentEnv


class RockPaperScissors(MultiAgentEnv):
    """Two-player environment for the famous rock paper scissors game.

    # __sphinx_doc_1_end__
    Optionally, the "Sheldon Cooper extension" can be activated by passing
    `sheldon_cooper_mode=True` into the constructor, in which case two more moves
    are allowed: Spock and Lizard. Spock is poisoned by Lizard, disproven by Paper, but
    crushes Rock and smashes Scissors. Lizard poisons Spock and eats Paper, but is
    decapitated by Scissors and crushed by Rock.

    # __sphinx_doc_2_begin__
    Both players always move simultaneously over a course of 10 timesteps in total.
    The winner of each timestep receives reward of +1, the losing player -1.0.

    The observation of each player is the last opponent action.
    """

    ROCK = 0
    PAPER = 1
    SCISSORS = 2
    LIZARD = 3
    SPOCK = 4

    WIN_MATRIX = {
        (ROCK, ROCK): (0, 0),
        (ROCK, PAPER): (-1, 1),
        (ROCK, SCISSORS): (1, -1),
        (PAPER, ROCK): (1, -1),
        (PAPER, PAPER): (0, 0),
        (PAPER, SCISSORS): (-1, 1),
        (SCISSORS, ROCK): (-1, 1),
        (SCISSORS, PAPER): (1, -1),
        (SCISSORS, SCISSORS): (0, 0),
    }
    # __sphinx_doc_2_end__

    WIN_MATRIX.update(
        {
            # Sheldon Cooper mode:
            (LIZARD, LIZARD): (0, 0),
            (LIZARD, SPOCK): (1, -1),  # Lizard poisons Spock
            (LIZARD, ROCK): (-1, 1),  # Rock crushes lizard
            (LIZARD, PAPER): (1, -1),  # Lizard eats paper
            (LIZARD, SCISSORS): (-1, 1),  # Scissors decapitate lizard
            (ROCK, LIZARD): (1, -1),  # Rock crushes lizard
            (PAPER, LIZARD): (-1, 1),  # Lizard eats paper
            (SCISSORS, LIZARD): (1, -1),  # Scissors decapitate lizard
            (SPOCK, SPOCK): (0, 0),
            (SPOCK, LIZARD): (-1, 1),  # Lizard poisons Spock
            (SPOCK, ROCK): (1, -1),  # Spock vaporizes rock
            (SPOCK, PAPER): (-1, 1),  # Paper disproves Spock
            (SPOCK, SCISSORS): (1, -1),  # Spock smashes scissors
            (ROCK, SPOCK): (-1, 1),  # Spock vaporizes rock
            (PAPER, SPOCK): (1, -1),  # Paper disproves Spock
            (SCISSORS, SPOCK): (-1, 1),  # Spock smashes scissors
        }
    )

    # __sphinx_doc_3_begin__
    def __init__(self, config=None):
        super().__init__()

        self.agents = self.possible_agents = ["player1", "player2"]

        # The observations are always the last taken actions. Hence observation- and
        # action spaces are identical.
        self.observation_spaces = self.action_spaces = {
            "player1": gym.spaces.Discrete(3),
            "player2": gym.spaces.Discrete(3),
        }
        self.last_move = None
        self.num_moves = 0
        # __sphinx_doc_3_end__

        self.sheldon_cooper_mode = False
        if config.get("sheldon_cooper_mode"):
            self.sheldon_cooper_mode = True
            self.action_spaces = self.observation_spaces = {
                "player1": gym.spaces.Discrete(5),
                "player2": gym.spaces.Discrete(5),
            }

    # __sphinx_doc_4_begin__
    def reset(self, *, seed=None, options=None):
        self.num_moves = 0

        # The first observation should not matter (none of the agents has moved yet).
        # Set them to 0.
        return {
            "player1": 0,
            "player2": 0,
        }, {}  # <- empty infos dict

    # __sphinx_doc_4_end__

    # __sphinx_doc_5_begin__
    def step(self, action_dict):
        self.num_moves += 1

        move1 = action_dict["player1"]
        move2 = action_dict["player2"]

        # Set the next observations (simply use the other player's action).
        # Note that because we are publishing both players in the observations dict,
        # we expect both players to act in the next `step()` (simultaneous stepping).
        observations = {"player1": move2, "player2": move1}

        # Compute rewards for each player based on the win-matrix.
        r1, r2 = self.WIN_MATRIX[move1, move2]
        rewards = {"player1": r1, "player2": r2}

        # Terminate the entire episode (for all agents) once 10 moves have been made.
        terminateds = {"__all__": self.num_moves >= 10}

        # Leave truncateds and infos empty.
        return observations, rewards, terminateds, {}, {}


# __sphinx_doc_5_end__
