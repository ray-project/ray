from gym.spaces import Discrete

from ray.rllib.env.multi_agent_env import MultiAgentEnv


class RockPaperScissors(MultiAgentEnv):
    """Two-player environment for the famous rock paper scissors game.

    The observation is simply the last opponent action."""

    ROCK = 0
    PAPER = 1
    SCISSORS = 2
    LIZARD = 3
    SPOCK = 4

    def __init__(self, config):
        self.sheldon_cooper = config.get("sheldon_cooper", False)
        self.action_space = Discrete(5 if self.sheldon_cooper else 3)
        self.observation_space = Discrete(5 if self.sheldon_cooper else 3)
        self.player1 = "player1"
        self.player2 = "player2"
        self.last_move = None
        self.num_moves = 0

        # For test-case inspections (compare both players' scores).
        self.player1_score = self.player2_score = 0

    def reset(self):
        self.last_move = (0, 0)
        self.num_moves = 0
        return {
            self.player1: self.last_move[1],
            self.player2: self.last_move[0],
        }

    def step(self, action_dict):
        move1 = action_dict[self.player1]
        move2 = action_dict[self.player2]
        if self.sheldon_cooper is False:
            assert move1 not in [self.LIZARD, self.SPOCK]
            assert move2 not in [self.LIZARD, self.SPOCK]

        self.last_move = (move1, move2)
        obs = {
            self.player1: self.last_move[1],
            self.player2: self.last_move[0],
        }
        r1, r2 = {
            (self.ROCK, self.ROCK): (0, 0),
            (self.ROCK, self.PAPER): (-1, 1),
            (self.ROCK, self.SCISSORS): (1, -1),
            (self.PAPER, self.ROCK): (1, -1),
            (self.PAPER, self.PAPER): (0, 0),
            (self.PAPER, self.SCISSORS): (-1, 1),
            (self.SCISSORS, self.ROCK): (-1, 1),
            (self.SCISSORS, self.PAPER): (1, -1),
            (self.SCISSORS, self.SCISSORS): (0, 0),
            # Sheldon Cooper extension:
            (self.LIZARD, self.LIZARD): (0, 0),
            (self.LIZARD, self.SPOCK): (1, -1),  # Lizard poisons Spock
            (self.LIZARD, self.ROCK): (-1, 1),  # Rock crushes lizard
            (self.LIZARD, self.PAPER): (1, -1),  # Lizard eats paper
            (self.LIZARD, self.SCISSORS): (-1, 1),  # Scissors decapitate Lizrd
            (self.ROCK, self.LIZARD): (1, -1),  # Rock crushes lizard
            (self.PAPER, self.LIZARD): (-1, 1),  # Lizard eats paper
            (self.SCISSORS, self.LIZARD): (1, -1),  # Scissors decapitate Lizrd
            (self.SPOCK, self.SPOCK): (0, 0),
            (self.SPOCK, self.LIZARD): (-1, 1),  # Lizard poisons Spock
            (self.SPOCK, self.ROCK): (1, -1),  # Spock vaporizes rock
            (self.SPOCK, self.PAPER): (-1, 1),  # Paper disproves Spock
            (self.SPOCK, self.SCISSORS): (1, -1),  # Spock smashes scissors
            (self.ROCK, self.SPOCK): (-1, 1),  # Spock vaporizes rock
            (self.PAPER, self.SPOCK): (1, -1),  # Paper disproves Spock
            (self.SCISSORS, self.SPOCK): (-1, 1),  # Spock smashes scissors
        }[move1, move2]
        rew = {
            self.player1: r1,
            self.player2: r2,
        }
        self.num_moves += 1
        done = {
            "__all__": self.num_moves >= 10,
        }

        if rew["player1"] > rew["player2"]:
            self.player1_score += 1
        elif rew["player2"] > rew["player1"]:
            self.player2_score += 1

        return obs, rew, done, {}
