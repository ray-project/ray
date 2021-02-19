##########
# Contribution by the Center on Long-Term Risk: https://github.com/longtermrisk/marltoolbox
# Some parts are originally from:
# https://github.com/julianstastny/openspiel-social-dilemmas/blob/master/games/coin_game_gym.py
##########

import copy
from collections import Iterable

import numpy as np
from numba import jit, prange
from numba.typed import List
from ray.rllib.examples.env.coin_game import CoinGame as NotVectorizedCoinGame


@jit(nopython=True)
def _same_pos(x, y):
    return (x == y).all()


@jit(nopython=True)
def move_players(batch_size, actions, red_pos, blue_pos, grid_size):
    moves = List([
        np.array([0, 1]),
        np.array([0, -1]),
        np.array([1, 0]),
        np.array([-1, 0]),
    ])

    for j in prange(batch_size):
        red_pos[j] = \
            (red_pos[j] + moves[actions[j, 0]]) % grid_size
        blue_pos[j] = \
            (blue_pos[j] + moves[actions[j, 1]]) % grid_size
    return red_pos, blue_pos


@jit(nopython=True)
def compute_reward(batch_size, red_pos, blue_pos, coin_pos, red_coin,
                   asymmetric):
    # Changing something here imply changing something in the analysis of the rewards
    reward_red = np.zeros(batch_size)
    reward_blue = np.zeros(batch_size)
    generate = np.zeros(batch_size, dtype=np.bool_)
    red_pick_any, red_pick_red, blue_pick_any, blue_pick_blue = False, False, False, False
    for i in prange(batch_size):
        if red_coin[i]:
            if _same_pos(red_pos[i], coin_pos[i]):
                generate[i] = True
                reward_red[i] += 1
                if asymmetric:
                    reward_red[i] += 3
                red_pick_any = True
                red_pick_red = True
            if _same_pos(blue_pos[i], coin_pos[i]):
                generate[i] = True
                reward_red[i] += -2
                reward_blue[i] += 1
                blue_pick_any = True
        else:
            if _same_pos(red_pos[i], coin_pos[i]):
                generate[i] = True
                reward_red[i] += 1
                reward_blue[i] += -2
                if asymmetric:
                    reward_red[i] += 3
                red_pick_any = True
            if _same_pos(blue_pos[i], coin_pos[i]):
                generate[i] = True
                reward_blue[i] += 1
                blue_pick_any = True
                blue_pick_blue = True
    reward = [reward_red, reward_blue]
    return reward, generate, red_pick_any, red_pick_red, blue_pick_any, blue_pick_blue


@jit(nopython=True)
def _flatten_index(pos, grid_size):
    y_pos, x_pos = pos
    idx = grid_size * y_pos
    idx += x_pos
    return idx


@jit(nopython=True)
def _unflatten_index(pos, grid_size):
    x_idx = pos % grid_size
    y_idx = pos // grid_size
    return np.array([y_idx, x_idx])


@jit(nopython=True)
def place_coin(red_pos_i, blue_pos_i, grid_size):
    red_pos_flat = _flatten_index(red_pos_i, grid_size)
    blue_pos_flat = _flatten_index(blue_pos_i, grid_size)
    possible_coin_pos = np.array([
        x for x in range(9) if ((x != blue_pos_flat) and (x != red_pos_flat))
    ])
    flat_coin_pos = np.random.choice(possible_coin_pos)
    return _unflatten_index(flat_coin_pos, grid_size)


@jit(nopython=True)
def generate_coin(batch_size, generate, red_coin, red_pos, blue_pos, coin_pos,
                  grid_size):
    red_coin[generate] = 1 - red_coin[generate]
    for i in prange(batch_size):
        if generate[i]:
            coin_pos[i] = place_coin(red_pos[i], blue_pos[i], grid_size)
    return coin_pos


@jit(nopython=True)
def generate_state(batch_size, red_pos, blue_pos, coin_pos, red_coin,
                   step_count_in_current_episode, max_steps, grid_size):
    state = np.zeros((batch_size, grid_size, grid_size, 4))
    for i in prange(batch_size):
        state[i, red_pos[i][0], red_pos[i][1], 0] = 1
        state[i, blue_pos[i][0], blue_pos[i][1], 1] = 1
        if red_coin[i]:
            state[i, coin_pos[i][0], coin_pos[i][1], 2] = 1
        else:
            state[i, coin_pos[i][0], coin_pos[i][1], 3] = 1
    return state


@jit(nopython=True)
def vectorized_step_with_numba_optimization(
        actions, batch_size, red_pos, blue_pos, coin_pos, red_coin,
        grid_size: int, asymmetric: bool, step_count_in_current_episode: int,
        max_steps: int):
    red_pos, blue_pos = move_players(batch_size, actions, red_pos, blue_pos,
                                     grid_size)
    reward, generate, red_pick_any, red_pick_red, blue_pick_any, blue_pick_blue = compute_reward(
        batch_size, red_pos, blue_pos, coin_pos, red_coin, asymmetric)
    coin_pos = generate_coin(batch_size, generate, red_coin, red_pos, blue_pos,
                             coin_pos, grid_size)
    state = generate_state(batch_size, red_pos, blue_pos, coin_pos, red_coin,
                           step_count_in_current_episode, max_steps, grid_size)
    return red_pos, blue_pos, reward, coin_pos, state, red_coin, red_pick_any, red_pick_red, blue_pick_any, blue_pick_blue


class CoinGame(NotVectorizedCoinGame):
    """
    Vectorized Coin Game environment.
    """

    def __init__(self, config={}):

        super().__init__(config)

        self.batch_size = config.get("batch_size", 1)
        self.force_vectorized = config.get("force_vectorize", False)
        assert self.grid_size == 3, "hardcoded in the generate_state function"

    def reset(self):
        self.step_count_in_current_episode = 0
        if self.output_additional_info:
            self._reset_info()

        self.red_coin = np.random.randint(2, size=self.batch_size)
        # Agent and coin positions
        self.red_pos = np.random.randint(
            self.grid_size, size=(self.batch_size, 2))
        self.blue_pos = np.random.randint(
            self.grid_size, size=(self.batch_size, 2))
        self.coin_pos = np.zeros((self.batch_size, 2), dtype=np.int8)
        for i in range(self.batch_size):
            # Make sure players don't overlap
            while _same_pos(self.red_pos[i], self.blue_pos[i]):
                self.blue_pos[i] = np.random.randint(self.grid_size, size=2)

        generate = np.ones(self.batch_size, dtype=bool)
        self.coin_pos = generate_coin(self.batch_size, generate, self.red_coin,
                                      self.red_pos, self.blue_pos,
                                      self.coin_pos, self.grid_size)
        state = generate_state(self.batch_size, self.red_pos, self.blue_pos,
                               self.coin_pos, self.red_coin,
                               self.step_count_in_current_episode,
                               self.max_steps, self.grid_size)

        # Unvectorize if batch_size == 1 (do not return batch of states)
        if self.batch_size == 1 and not self.force_vectorized:
            state = state[0, ...]

        return {self.player_red_id: state, self.player_blue_id: state}

    def step(self, actions: Iterable):
        """
        :param actions: Dict containing both actions for player_1 and player_2
        :return: observations, rewards, done, info
        """
        actions = self._from_RLLib_API_to_list(actions)
        self.step_count_in_current_episode += 1

        (self.red_pos, self.blue_pos, rewards, self.coin_pos, observation,
         self.red_coin, red_pick_any, red_pick_red, blue_pick_any,
         blue_pick_blue) = vectorized_step_with_numba_optimization(
             actions, self.batch_size, self.red_pos, self.blue_pos,
             self.coin_pos, self.red_coin, self.grid_size, self.asymmetric,
             self.step_count_in_current_episode, self.max_steps)

        if self.output_additional_info:
            self._accumulate_info(red_pick_any, red_pick_red, blue_pick_any,
                                  blue_pick_blue)

        observations = self._produce_observations_invariant_to_the_player_trained(
            observation)

        # Unvectorize if batch_size == 1 (do not return batch of states and rewards)
        if self.batch_size == 1 and not self.force_vectorized:
            observations = [obs[0, ...] for obs in observations]
            rewards[0], rewards[1] = rewards[0][0], rewards[1][0]

        return self._to_RLLib_API(observations, rewards)

    def _from_RLLib_API_to_list(self, actions):
        """
        Format actions from dict of players to list of lists
        """
        ac_red, ac_blue = actions[self.player_red_id], actions[
            self.player_blue_id]
        if not isinstance(ac_red, Iterable):
            ac_red, ac_blue = [ac_red], [ac_blue]
        actions = [ac_red, ac_blue]
        actions = np.array(actions).T
        return actions

    def _save_env(self):
        env_save_state = {
            "red_pos": self.red_pos,
            "blue_pos": self.blue_pos,
            "coin_pos": self.coin_pos,
            "red_coin": self.red_coin,
            "grid_size": self.grid_size,
            "asymmetric": self.asymmetric,
            "batch_size": self.batch_size,
            "step_count_in_current_episode": self.
            step_count_in_current_episode,
            "max_steps": self.max_steps,
            "red_pick": self.red_pick,
            "red_pick_own": self.red_pick_own,
            "blue_pick": self.blue_pick,
            "blue_pick_own": self.blue_pick_own,
        }
        return copy.deepcopy(env_save_state)

    def _load_env(self, env_state):
        for k, v in env_state.items():
            self.__setattr__(k, v)


class AsymCoinGame(CoinGame):
    NAME = "AsymCoinGame"

    def __init__(self, config={}):
        if "asymmetric" in config:
            assert config["asymmetric"]
        else:
            config["asymmetric"] = True
        super().__init__(config)
