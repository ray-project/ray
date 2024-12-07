import gymnasium as gym

from ray.rllib.env.multi_agent_env import MultiAgentEnv


class RockPaperScissors(MultiAgentEnv):
    """Two-player environment for the famous rock paper scissors game.

    Optionally, the "Sheldon Cooper extension" can be activated by passing
    `sheldon_cooper_mode=True` into the constructor, in which case two more moves
    are allowed: Spock and Lizard.

    Both players move simultaneously over a course of 10 timesteps in total.
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

    def __init__(self, config=None):
        super().__init__()

        self.sheldon_cooper_mode = config.get("sheldon_cooper_mode", False)
        self.action_space = gym.spaces.Discrete(5 if self.sheldon_cooper_mode else 3)
        self.observation_space = gym.spaces.Discrete(5 if self.sheldon_cooper_mode else 3)
        self.player1 = "player1"
        self.player2 = "player2"
        self.last_move = None
        self.num_moves = 0

        # For test-case inspections (compare both players' scores).
        self.player1_score = self.player2_score = 0

    def reset(self, *, seed=None, options=None):
        self.last_move = (0, 0)
        self.num_moves = 0
        return {
            self.player1: self.last_move[1],
            self.player2: self.last_move[0],
        }

    def step(self, action_dict):
        move1 = action_dict[self.player1]
        move2 = action_dict[self.player2]
        if self.sheldon_cooper_mode is False:
            assert move1 not in [self.LIZARD, self.SPOCK]
            assert move2 not in [self.LIZARD, self.SPOCK]

        self.last_move = (move1, move2)
        obs = {
            self.player1: self.last_move[1],
            self.player2: self.last_move[0],
        }
        r1, r2 = self.WIN_MATRIX[move1, move2]
        rew = {
            self.player1: r1,
            self.player2: r2,
        }
        self.num_moves += 1
        terminated = {
            "__all__": self.num_moves >= 10,
        }

        if rew["player1"] > rew["player2"]:
            self.player1_score += 1
        elif rew["player2"] > rew["player1"]:
            self.player2_score += 1

        return obs, rew, terminated, False, {}