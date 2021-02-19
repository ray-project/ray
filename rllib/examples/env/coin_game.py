##########
# Contribution by the Center on Long-Term Risk: https://github.com/longtermrisk/marltoolbox
# Some parts are originally from:
# https://github.com/julianstastny/openspiel-social-dilemmas/blob/master/games/coin_game_gym.py
##########

import copy
from collections import Iterable
from typing import Dict

import gym
import numpy as np
from gym.spaces import Discrete
from gym.utils import seeding
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.examples.env.utils.interfaces import InfoAccumulationInterface


class CoinGame(InfoAccumulationInterface, MultiAgentEnv, gym.Env):
    """
    Coin Game environment.
    """
    NAME = "CoinGame"
    NUM_AGENTS = 2
    NUM_ACTIONS = 4
    ACTION_SPACE = Discrete(NUM_ACTIONS)
    OBSERVATION_SPACE = None
    MOVES = [
        np.array([0, 1]),
        np.array([0, -1]),
        np.array([1, 0]),
        np.array([-1, 0]),
    ]

    def __init__(self, config: dict):

        if "players_ids" in config:
            assert isinstance(config["players_ids"], Iterable) and len(
                config["players_ids"]) == self.NUM_AGENTS

        # Load config
        self.players_ids = config.get("players_ids",
                                      ["player_red", "player_blue"])
        self.player_red_id, self.player_blue_id = self.players_ids
        self.max_steps = config.get("max_steps", 20)
        self.grid_size = config.get("grid_size", 3)
        self.output_additional_info = config.get("output_additional_info",
                                                 True)
        self.asymmetric = config.get("asymmetric", False)
        self.n_features = self.grid_size**2 * (2 * self.NUM_AGENTS)
        self.OBSERVATION_SPACE = gym.spaces.Box(
            low=0,
            high=1,
            shape=(self.grid_size, self.grid_size, 4),
            dtype="uint8")

        self.step_count_in_current_episode = None
        if self.output_additional_info:
            self._init_info()
        self.seed(seed=config.get("seed", None))

    def seed(self, seed=None):
        """Seed the PRNG of this space. """
        self.np_random, seed = seeding.np_random(seed)
        return [seed]

    def reset(self):
        self.step_count_in_current_episode = 0

        if self.output_additional_info:
            self._reset_info()

        self._randomize_color_and_player_positions()
        self._generate_coin()
        observation = self._generate_observation()

        # observations in the RLLib multi agent format
        return {
            self.player_red_id: observation,
            self.player_blue_id: observation
        }

    def _randomize_color_and_player_positions(self):
        # Reset coin color and the players and coin positions
        self.red_coin = self.np_random.randint(low=0, high=2)
        self.red_pos = self.np_random.randint(
            low=0, high=self.grid_size, size=(2, ))
        self.blue_pos = self.np_random.randint(
            low=0, high=self.grid_size, size=(2, ))
        self.coin_pos = np.zeros(shape=(2, ), dtype=np.int8)

        # Make sure players don't overlap at the start
        while self._same_pos(self.red_pos, self.blue_pos):
            self.blue_pos = self.np_random.randint(self.grid_size, size=2)

    def _generate_coin(self):
        # Switch between red and blue coins
        self.red_coin = 1 - self.red_coin

        # Make sure the coin has a different position than the players
        success = 0
        while success < self.NUM_AGENTS:
            self.coin_pos = self.np_random.randint(self.grid_size, size=2)
            success = 1 - self._same_pos(self.red_pos, self.coin_pos)
            success += 1 - self._same_pos(self.blue_pos, self.coin_pos)

    def _generate_observation(self):
        state = np.zeros((self.grid_size, self.grid_size, 4))
        state[self.red_pos[0], self.red_pos[1], 0] = 1
        state[self.blue_pos[0], self.blue_pos[1], 1] = 1
        if self.red_coin:
            state[self.coin_pos[0], self.coin_pos[1], 2] = 1
        else:
            state[self.coin_pos[0], self.coin_pos[1], 3] = 1
        return state

    def step(self, actions: Dict):
        """
        :param actions: Dict containing both actions for player_1 and player_2
        :return: observations, rewards, done, info
        """
        actions = self._from_RLLib_API_to_list(actions)

        self.step_count_in_current_episode += 1
        self._move_players(actions)
        reward_list, generate_new_coin = self._compute_reward()
        if generate_new_coin:
            self._generate_coin()
        observation = self._generate_observation()
        observations = self._produce_observations_invariant_to_the_player_trained(
            observation)

        return self._to_RLLib_API(observations, reward_list)

    def _same_pos(self, x, y):
        return (x == y).all()

    def _move_players(self, actions):
        self.red_pos = (self.red_pos + self.MOVES[actions[0]]) % self.grid_size
        self.blue_pos = (
            self.blue_pos + self.MOVES[actions[1]]) % self.grid_size

    def _compute_reward(self):

        reward_red = 0.0
        reward_blue = 0.0
        generate_new_coin = False
        red_pick_any, red_pick_red, blue_pick_any, blue_pick_blue = False, False, False, False
        if self.red_coin:
            if self._same_pos(self.red_pos, self.coin_pos):
                generate_new_coin = True
                reward_red += 1
                if self.asymmetric:
                    reward_red += 3
                red_pick_any = True
                red_pick_red = True
            if self._same_pos(self.blue_pos, self.coin_pos):
                generate_new_coin = True
                reward_red += -2
                reward_blue += 1
                blue_pick_any = True
        else:
            if self._same_pos(self.red_pos, self.coin_pos):
                generate_new_coin = True
                reward_red += 1
                reward_blue += -2
                if self.asymmetric:
                    reward_red += 3
                red_pick_any = True
            if self._same_pos(self.blue_pos, self.coin_pos):
                generate_new_coin = True
                reward_blue += 1
                blue_pick_blue = True
                blue_pick_any = True

        reward_list = [reward_red, reward_blue]

        if self.output_additional_info:
            self._accumulate_info(red_pick_any, red_pick_red, blue_pick_any,
                                  blue_pick_blue)

        return reward_list, generate_new_coin

    def _from_RLLib_API_to_list(self, actions):
        """
        Format actions from dict of players to list of lists
        """
        ac_red, ac_blue = actions[self.player_red_id], actions[
            self.player_blue_id]
        actions = [ac_red, ac_blue]
        return actions

    def _produce_observations_invariant_to_the_player_trained(
            self, observation):
        """
        We want to be able to use a policy trained as player 1 for evaluation as player 2 and vice versa.
        """

        # player_red_observation contains [Red pos, Blue pos, Red coin pos, Blue coin pos]
        player_red_observation = observation
        # After modification, player_blue_observation will contain [Blue pos, Red pos, Blue coin pos, Red coin pos]
        player_blue_observation = copy.deepcopy(observation)
        player_blue_observation[..., 0] = observation[..., 1]
        player_blue_observation[..., 1] = observation[..., 0]
        player_blue_observation[..., 2] = observation[..., 3]
        player_blue_observation[..., 3] = observation[..., 2]

        return [player_red_observation, player_blue_observation]

    def _to_RLLib_API(self, observations, rewards):
        state = {
            self.player_red_id: observations[0],
            self.player_blue_id: observations[1],
        }
        rewards = {
            self.player_red_id: rewards[0],
            self.player_blue_id: rewards[1],
        }

        epi_is_done = (self.step_count_in_current_episode == self.max_steps)
        done = {
            self.player_red_id: epi_is_done,
            self.player_blue_id: epi_is_done,
            "__all__": epi_is_done,
        }

        if epi_is_done and self.output_additional_info:
            player_red_info, player_blue_info = self._get_episode_info()
            info = {
                self.player_red_id: player_red_info,
                self.player_blue_id: player_blue_info,
            }
        else:
            info = {}

        return state, rewards, done, info

    def _get_episode_info(self):
        """
        Output the following information:
        - pick_speed is the fraction of steps during which the player picked a coin
        - pick_own_color is the fraction of coins picked by the player which have the same color as the player
        """
        player_red_info, player_blue_info = {}, {}

        if len(self.red_pick) > 0:
            red_pick = sum(self.red_pick)
            player_red_info["pick_speed"] = red_pick / len(self.red_pick)
            if red_pick > 0:
                player_red_info["pick_own_color"] = sum(
                    self.red_pick_own) / red_pick

        if len(self.blue_pick) > 0:
            blue_pick = sum(self.blue_pick)
            player_blue_info["pick_speed"] = blue_pick / len(self.blue_pick)
            if blue_pick > 0:
                player_blue_info["pick_own_color"] = sum(
                    self.blue_pick_own) / blue_pick

        return player_red_info, player_blue_info

    def _reset_info(self):
        self.red_pick.clear()
        self.red_pick_own.clear()
        self.blue_pick.clear()
        self.blue_pick_own.clear()

    def _accumulate_info(self, red_pick_any, red_pick_red, blue_pick_any,
                         blue_pick_blue):
        self.red_pick.append(red_pick_any)
        self.red_pick_own.append(red_pick_red)
        self.blue_pick.append(blue_pick_any)
        self.blue_pick_own.append(blue_pick_blue)

    def _init_info(self):
        self.red_pick = []
        self.red_pick_own = []
        self.blue_pick = []
        self.blue_pick_own = []


class AsymCoinGame(CoinGame):
    NAME = "AsymCoinGame"

    def __init__(self, config={}):
        if "asymmetric" in config:
            assert config["asymmetric"]
        else:
            config["asymmetric"] = True
        super().__init__(config)
