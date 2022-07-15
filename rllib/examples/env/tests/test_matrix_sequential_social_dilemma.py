##########
# Contribution by the Center on Long-Term Risk:
# https://github.com/longtermrisk/marltoolbox
##########
import random

from ray.rllib.examples.env.matrix_sequential_social_dilemma import (
    IteratedPrisonersDilemma,
    IteratedChicken,
    IteratedStagHunt,
    IteratedBoS,
)

ENVS = [IteratedPrisonersDilemma, IteratedChicken, IteratedStagHunt, IteratedBoS]


def test_reset():
    max_steps = 20
    env_all = [init_env(max_steps, env_class) for env_class in ENVS]

    for env in env_all:
        obs = env.reset()
        check_obs(obs, env)
        assert_logger_buffer_size_two_players(env, n_steps=0)


def init_env(max_steps, env_class, seed=None):
    config = {
        "max_steps": max_steps,
    }
    env = env_class(config)
    env.seed(seed)
    return env


def check_obs(obs, env):
    assert len(obs) == 2, "two players"
    for key, player_obs in obs.items():
        assert isinstance(player_obs, int)  # .shape == (env.NUM_STATES)
        assert player_obs < env.NUM_STATES


def assert_logger_buffer_size_two_players(env, n_steps):
    assert len(env.cc_count) == n_steps
    assert len(env.dd_count) == n_steps
    assert len(env.cd_count) == n_steps
    assert len(env.dc_count) == n_steps


def test_step():
    max_steps = 20
    env_all = [init_env(max_steps, env_class) for env_class in ENVS]

    for env in env_all:
        obs = env.reset()
        check_obs(obs, env)
        assert_logger_buffer_size_two_players(env, n_steps=0)

        actions = {
            policy_id: random.randint(0, env.NUM_ACTIONS - 1)
            for policy_id in env.players_ids
        }
        obs, reward, done, info = env.step(actions)
        check_obs(obs, env)
        assert_logger_buffer_size_two_players(env, n_steps=1)
        assert not done["__all__"]


def test_multiple_steps():
    max_steps = 20
    env_all = [init_env(max_steps, env_class) for env_class in ENVS]
    n_steps = int(max_steps * 0.75)

    for env in env_all:
        obs = env.reset()
        check_obs(obs, env)
        assert_logger_buffer_size_two_players(env, n_steps=0)

        for step_i in range(1, n_steps, 1):
            actions = {
                policy_id: random.randint(0, env.NUM_ACTIONS - 1)
                for policy_id in env.players_ids
            }
            obs, reward, done, info = env.step(actions)
            check_obs(obs, env)
            assert_logger_buffer_size_two_players(env, n_steps=step_i)
            assert not done["__all__"]


def test_multiple_episodes():
    max_steps = 20
    env_all = [init_env(max_steps, env_class) for env_class in ENVS]
    n_steps = int(max_steps * 8.25)

    for env in env_all:
        obs = env.reset()
        check_obs(obs, env)
        assert_logger_buffer_size_two_players(env, n_steps=0)

        step_i = 0
        for _ in range(n_steps):
            step_i += 1
            actions = {
                policy_id: random.randint(0, env.NUM_ACTIONS - 1)
                for policy_id in env.players_ids
            }
            obs, reward, done, info = env.step(actions)
            check_obs(obs, env)
            assert_logger_buffer_size_two_players(env, n_steps=step_i)
            assert not done["__all__"] or (step_i == max_steps and done["__all__"])
            if done["__all__"]:
                obs = env.reset()
                check_obs(obs, env)
                step_i = 0


def assert_info(n_steps, p_row_act, p_col_act, env, max_steps, CC, DD, CD, DC):
    step_i = 0
    for _ in range(n_steps):
        step_i += 1
        actions = {
            "player_row": p_row_act[step_i - 1],
            "player_col": p_col_act[step_i - 1],
        }
        obs, reward, done, info = env.step(actions)
        check_obs(obs, env)
        assert_logger_buffer_size_two_players(env, n_steps=step_i)
        assert not done["__all__"] or (step_i == max_steps and done["__all__"])

        if done["__all__"]:
            assert info["player_row"]["CC"] == CC
            assert info["player_col"]["CC"] == CC
            assert info["player_row"]["DD"] == DD
            assert info["player_col"]["DD"] == DD
            assert info["player_row"]["CD"] == CD
            assert info["player_col"]["CD"] == CD
            assert info["player_row"]["DC"] == DC
            assert info["player_col"]["DC"] == DC

            obs = env.reset()
            check_obs(obs, env)
            assert_logger_buffer_size_two_players(env, n_steps=0)
            step_i = 0


def test_logged_info_full_CC():
    p_row_act = [0, 0, 0, 0]
    p_col_act = [0, 0, 0, 0]
    max_steps = 4
    env_all = [init_env(max_steps, env_class) for env_class in ENVS]
    n_steps = int(max_steps * 8.25)

    for env in env_all:
        obs = env.reset()
        check_obs(obs, env)
        assert_logger_buffer_size_two_players(env, n_steps=0)

        assert_info(
            n_steps,
            p_row_act,
            p_col_act,
            env,
            max_steps,
            CC=1.0,
            DD=0.0,
            CD=0.0,
            DC=0.0,
        )


def test_logged_info_full_DD():
    p_row_act = [1, 1, 1, 1]
    p_col_act = [1, 1, 1, 1]
    max_steps = 4
    env_all = [init_env(max_steps, env_class) for env_class in ENVS]
    n_steps = int(max_steps * 8.25)

    for env in env_all:
        obs = env.reset()
        check_obs(obs, env)
        assert_logger_buffer_size_two_players(env, n_steps=0)

        assert_info(
            n_steps,
            p_row_act,
            p_col_act,
            env,
            max_steps,
            CC=0.0,
            DD=1.0,
            CD=0.0,
            DC=0.0,
        )


def test_logged_info_full_CD():
    p_row_act = [0, 0, 0, 0]
    p_col_act = [1, 1, 1, 1]
    max_steps = 4
    env_all = [init_env(max_steps, env_class) for env_class in ENVS]
    n_steps = int(max_steps * 8.25)

    for env in env_all:
        obs = env.reset()
        check_obs(obs, env)
        assert_logger_buffer_size_two_players(env, n_steps=0)

        assert_info(
            n_steps,
            p_row_act,
            p_col_act,
            env,
            max_steps,
            CC=0.0,
            DD=0.0,
            CD=1.0,
            DC=0.0,
        )


def test_logged_info_full_DC():
    p_row_act = [1, 1, 1, 1]
    p_col_act = [0, 0, 0, 0]
    max_steps = 4
    env_all = [init_env(max_steps, env_class) for env_class in ENVS]
    n_steps = int(max_steps * 8.25)

    for env in env_all:
        obs = env.reset()
        check_obs(obs, env)
        assert_logger_buffer_size_two_players(env, n_steps=0)

        assert_info(
            n_steps,
            p_row_act,
            p_col_act,
            env,
            max_steps,
            CC=0.0,
            DD=0.0,
            CD=0.0,
            DC=1.0,
        )


def test_logged_info_mix_CC_DD():
    p_row_act = [0, 1, 1, 1]
    p_col_act = [0, 1, 1, 1]
    max_steps = 4
    env_all = [init_env(max_steps, env_class) for env_class in ENVS]
    n_steps = int(max_steps * 8.25)

    for env in env_all:
        obs = env.reset()
        check_obs(obs, env)
        assert_logger_buffer_size_two_players(env, n_steps=0)

        assert_info(
            n_steps,
            p_row_act,
            p_col_act,
            env,
            max_steps,
            CC=0.25,
            DD=0.75,
            CD=0.0,
            DC=0.0,
        )


def test_logged_info_mix_CD_CD():
    p_row_act = [1, 0, 1, 0]
    p_col_act = [0, 1, 0, 1]
    max_steps = 4
    env_all = [init_env(max_steps, env_class) for env_class in ENVS]
    n_steps = int(max_steps * 8.25)

    for env in env_all:
        obs = env.reset()
        check_obs(obs, env)
        assert_logger_buffer_size_two_players(env, n_steps=0)

        assert_info(
            n_steps,
            p_row_act,
            p_col_act,
            env,
            max_steps,
            CC=0.0,
            DD=0.0,
            CD=0.5,
            DC=0.5,
        )


def test_observations_are_invariant_to_the_player_trained():
    p_row_act = [0, 1, 1, 0]
    p_col_act = [0, 1, 0, 1]
    max_steps = 4
    env_all = [init_env(max_steps, env_class) for env_class in ENVS]
    n_steps = 4

    for env in env_all:
        _ = env.reset()
        step_i = 0
        for _ in range(n_steps):
            step_i += 1
            actions = {
                "player_row": p_row_act[step_i - 1],
                "player_col": p_col_act[step_i - 1],
            }
            obs, reward, done, info = env.step(actions)
            # assert observations are symmetrical respective to the actions
            if step_i == 1:
                assert obs[env.players_ids[0]] == obs[env.players_ids[1]]
            elif step_i == 2:
                assert obs[env.players_ids[0]] == obs[env.players_ids[1]]
            elif step_i == 3:
                obs_step_3 = obs
            elif step_i == 4:
                assert obs[env.players_ids[0]] == obs_step_3[env.players_ids[1]]
                assert obs[env.players_ids[1]] == obs_step_3[env.players_ids[0]]
