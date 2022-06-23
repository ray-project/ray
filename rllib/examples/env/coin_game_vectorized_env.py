##########
# Contribution by the Center on Long-Term Risk:
# https://github.com/longtermrisk/marltoolbox
# Some parts are originally from:
# https://github.com/julianstastny/openspiel-social-dilemmas/
# blob/master/games/coin_game_gym.py
##########

import copy
from collections import Iterable

import numpy as np
from numba import jit, prange
from numba.typed import List
from ray.rllib.examples.env.coin_game_non_vectorized_env import CoinGame
from ray.rllib.utils import override


class VectorizedCoinGame(CoinGame):
    """
    Vectorized Coin Game environment.
    """

    def __init__(self, config=None):
        if config is None:
            config = {}

        super().__init__(config)

        self.batch_size = config.get("batch_size", 1)
        self.force_vectorized = config.get("force_vectorize", False)
        assert self.grid_size == 3, "hardcoded in the generate_state function"

    @override(CoinGame)
    def _randomize_color_and_player_positions(self):
        # Reset coin color and the players and coin positions
        self.red_coin = np.random.randint(2, size=self.batch_size)
        self.red_pos = np.random.randint(self.grid_size, size=(self.batch_size, 2))
        self.blue_pos = np.random.randint(self.grid_size, size=(self.batch_size, 2))
        self.coin_pos = np.zeros((self.batch_size, 2), dtype=np.int8)

        self._players_do_not_overlap_at_start()

    @override(CoinGame)
    def _players_do_not_overlap_at_start(self):
        for i in range(self.batch_size):
            while _same_pos(self.red_pos[i], self.blue_pos[i]):
                self.blue_pos[i] = np.random.randint(self.grid_size, size=2)

    @override(CoinGame)
    def _generate_coin(self):
        generate = np.ones(self.batch_size, dtype=bool)
        self.coin_pos = generate_coin(
            self.batch_size,
            generate,
            self.red_coin,
            self.red_pos,
            self.blue_pos,
            self.coin_pos,
            self.grid_size,
        )

    @override(CoinGame)
    def _generate_observation(self):
        obs = generate_observations_wt_numba_optimization(
            self.batch_size,
            self.red_pos,
            self.blue_pos,
            self.coin_pos,
            self.red_coin,
            self.grid_size,
        )

        obs = self._get_obs_invariant_to_the_player_trained(obs)
        obs, _ = self._optional_unvectorize(obs)
        return obs

    def _optional_unvectorize(self, obs, rewards=None):
        if self.batch_size == 1 and not self.force_vectorized:
            obs = [one_obs[0, ...] for one_obs in obs]
            if rewards is not None:
                rewards[0], rewards[1] = rewards[0][0], rewards[1][0]
        return obs, rewards

    @override(CoinGame)
    def step(self, actions: Iterable):

        actions = self._from_RLlib_API_to_list(actions)
        self.step_count_in_current_episode += 1

        (
            self.red_pos,
            self.blue_pos,
            rewards,
            self.coin_pos,
            observation,
            self.red_coin,
            red_pick_any,
            red_pick_red,
            blue_pick_any,
            blue_pick_blue,
        ) = vectorized_step_wt_numba_optimization(
            actions,
            self.batch_size,
            self.red_pos,
            self.blue_pos,
            self.coin_pos,
            self.red_coin,
            self.grid_size,
            self.asymmetric,
            self.max_steps,
            self.both_players_can_pick_the_same_coin,
        )

        if self.output_additional_info:
            self._accumulate_info(
                red_pick_any, red_pick_red, blue_pick_any, blue_pick_blue
            )

        obs = self._get_obs_invariant_to_the_player_trained(observation)

        obs, rewards = self._optional_unvectorize(obs, rewards)

        return self._to_RLlib_API(obs, rewards)

    @override(CoinGame)
    def _get_episode_info(self):

        player_red_info, player_blue_info = {}, {}

        if len(self.red_pick) > 0:
            red_pick = sum(self.red_pick)
            player_red_info["pick_speed"] = red_pick / (
                len(self.red_pick) * self.batch_size
            )
            if red_pick > 0:
                player_red_info["pick_own_color"] = sum(self.red_pick_own) / red_pick

        if len(self.blue_pick) > 0:
            blue_pick = sum(self.blue_pick)
            player_blue_info["pick_speed"] = blue_pick / (
                len(self.blue_pick) * self.batch_size
            )
            if blue_pick > 0:
                player_blue_info["pick_own_color"] = sum(self.blue_pick_own) / blue_pick

        return player_red_info, player_blue_info

    @override(CoinGame)
    def _from_RLlib_API_to_list(self, actions):

        ac_red = actions[self.player_red_id]
        ac_blue = actions[self.player_blue_id]
        if not isinstance(ac_red, Iterable):
            assert not isinstance(ac_blue, Iterable)
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
            "step_count_in_current_episode": self.step_count_in_current_episode,
            "max_steps": self.max_steps,
            "red_pick": self.red_pick,
            "red_pick_own": self.red_pick_own,
            "blue_pick": self.blue_pick,
            "blue_pick_own": self.blue_pick_own,
            "both_players_can_pick_the_same_coin": self.both_players_can_pick_the_same_coin,  # noqa: E501
        }
        return copy.deepcopy(env_save_state)

    def _load_env(self, env_state):
        for k, v in env_state.items():
            self.__setattr__(k, v)


class AsymVectorizedCoinGame(VectorizedCoinGame):
    NAME = "AsymCoinGame"

    def __init__(self, config=None):
        if config is None:
            config = {}

        if "asymmetric" in config:
            assert config["asymmetric"]
        else:
            config["asymmetric"] = True
        super().__init__(config)


@jit(nopython=True)
def move_players(batch_size, actions, red_pos, blue_pos, grid_size):
    moves = List(
        [
            np.array([0, 1]),
            np.array([0, -1]),
            np.array([1, 0]),
            np.array([-1, 0]),
        ]
    )

    for j in prange(batch_size):
        red_pos[j] = (red_pos[j] + moves[actions[j, 0]]) % grid_size
        blue_pos[j] = (blue_pos[j] + moves[actions[j, 1]]) % grid_size
    return red_pos, blue_pos


@jit(nopython=True)
def compute_reward(
    batch_size,
    red_pos,
    blue_pos,
    coin_pos,
    red_coin,
    asymmetric,
    both_players_can_pick_the_same_coin,
):
    reward_red = np.zeros(batch_size)
    reward_blue = np.zeros(batch_size)
    generate = np.zeros(batch_size, dtype=np.bool_)
    red_pick_any, red_pick_red, blue_pick_any, blue_pick_blue = 0, 0, 0, 0

    for i in prange(batch_size):
        red_first_if_both = None
        if not both_players_can_pick_the_same_coin:
            if _same_pos(red_pos[i], coin_pos[i]) and _same_pos(
                blue_pos[i], coin_pos[i]
            ):
                red_first_if_both = bool(np.random.randint(0, 1))

        if red_coin[i]:
            if _same_pos(red_pos[i], coin_pos[i]) and (
                red_first_if_both is None or red_first_if_both
            ):
                generate[i] = True
                reward_red[i] += 1
                if asymmetric:
                    reward_red[i] += 3
                red_pick_any += 1
                red_pick_red += 1
            if _same_pos(blue_pos[i], coin_pos[i]) and (
                red_first_if_both is None or not red_first_if_both
            ):
                generate[i] = True
                reward_red[i] += -2
                reward_blue[i] += 1
                blue_pick_any += 1
        else:
            if _same_pos(red_pos[i], coin_pos[i]) and (
                red_first_if_both is None or red_first_if_both
            ):
                generate[i] = True
                reward_red[i] += 1
                reward_blue[i] += -2
                if asymmetric:
                    reward_red[i] += 3
                red_pick_any += 1
            if _same_pos(blue_pos[i], coin_pos[i]) and (
                red_first_if_both is None or not red_first_if_both
            ):
                generate[i] = True
                reward_blue[i] += 1
                blue_pick_any += 1
                blue_pick_blue += 1
    reward = [reward_red, reward_blue]

    return reward, generate, red_pick_any, red_pick_red, blue_pick_any, blue_pick_blue


@jit(nopython=True)
def _same_pos(x, y):
    return (x == y).all()


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
def generate_coin(
    batch_size, generate, red_coin, red_pos, blue_pos, coin_pos, grid_size
):
    red_coin[generate] = 1 - red_coin[generate]
    for i in prange(batch_size):
        if generate[i]:
            coin_pos[i] = place_coin(red_pos[i], blue_pos[i], grid_size)
    return coin_pos


@jit(nopython=True)
def place_coin(red_pos_i, blue_pos_i, grid_size):
    red_pos_flat = _flatten_index(red_pos_i, grid_size)
    blue_pos_flat = _flatten_index(blue_pos_i, grid_size)
    possible_coin_pos = np.array(
        [x for x in range(9) if ((x != blue_pos_flat) and (x != red_pos_flat))]
    )
    flat_coin_pos = np.random.choice(possible_coin_pos)
    return _unflatten_index(flat_coin_pos, grid_size)


@jit(nopython=True)
def generate_observations_wt_numba_optimization(
    batch_size, red_pos, blue_pos, coin_pos, red_coin, grid_size
):
    obs = np.zeros((batch_size, grid_size, grid_size, 4))
    for i in prange(batch_size):
        obs[i, red_pos[i][0], red_pos[i][1], 0] = 1
        obs[i, blue_pos[i][0], blue_pos[i][1], 1] = 1
        if red_coin[i]:
            obs[i, coin_pos[i][0], coin_pos[i][1], 2] = 1
        else:
            obs[i, coin_pos[i][0], coin_pos[i][1], 3] = 1
    return obs


@jit(nopython=True)
def vectorized_step_wt_numba_optimization(
    actions,
    batch_size,
    red_pos,
    blue_pos,
    coin_pos,
    red_coin,
    grid_size: int,
    asymmetric: bool,
    max_steps: int,
    both_players_can_pick_the_same_coin: bool,
):
    red_pos, blue_pos = move_players(batch_size, actions, red_pos, blue_pos, grid_size)

    (
        reward,
        generate,
        red_pick_any,
        red_pick_red,
        blue_pick_any,
        blue_pick_blue,
    ) = compute_reward(
        batch_size,
        red_pos,
        blue_pos,
        coin_pos,
        red_coin,
        asymmetric,
        both_players_can_pick_the_same_coin,
    )

    coin_pos = generate_coin(
        batch_size, generate, red_coin, red_pos, blue_pos, coin_pos, grid_size
    )

    obs = generate_observations_wt_numba_optimization(
        batch_size, red_pos, blue_pos, coin_pos, red_coin, grid_size
    )

    return (
        red_pos,
        blue_pos,
        reward,
        coin_pos,
        obs,
        red_coin,
        red_pick_any,
        red_pick_red,
        blue_pick_any,
        blue_pick_blue,
    )
