from unittest.mock import MagicMock

import ray.rllib.examples.multi_agent.utils.self_play_league_based_callback as callback_module
from ray.rllib.core import COMPONENT_RL_MODULE
from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner
from ray.rllib.examples.multi_agent.utils.self_play_league_based_callback import SelfPlayLeagueBasedCallback
from ray.rllib.utils.metrics import ENV_RUNNER_RESULTS, WEIGHTS_SEQ_NO


def test_new_self_play_module_weights_are_synced_immediately(monkeypatch):
    callback = SelfPlayLeagueBasedCallback(win_rate_threshold=0.5)
    callback.trainable_policies = {"main", "league_exploiter_1"}

    main_module = MagicMock()
    main_module.get_state.return_value = {"weights": [1, 2, 3]}

    algorithm = MagicMock()
    algorithm.iteration = 1
    algorithm.env_runner.module = {"main": main_module}
    monkeypatch.setattr(callback_module.RLModuleSpec, "from_module", lambda module: object())

    state_ref = object()
    captured_state = {}

    def fake_put(state):
        captured_state["value"] = state
        return state_ref

    monkeypatch.setattr(callback_module.ray, "put", fake_put)

    callback.on_train_result(
        algorithm=algorithm,
        result={ENV_RUNNER_RESULTS: {"win_rate_main": 0.9}},
    )

    algorithm.add_module.assert_called_once()
    algorithm.set_state.assert_called_once()
    assert captured_state["value"] == {"main_1": {"weights": [1, 2, 3]}}

    foreach_calls = algorithm.env_runner_group.foreach_env_runner.call_args_list
    assert len(foreach_calls) == 2

    sync_call = foreach_calls[0]
    assert sync_call.kwargs["local_env_runner"] is True

    env_runner = MagicMock()
    sync_fn = sync_call.args[0]
    sync_fn(env_runner)
    env_runner.set_state.assert_called_once_with({COMPONENT_RL_MODULE: state_ref})

    mapping_call = foreach_calls[1]
    assert mapping_call.kwargs["local_env_runner"] is True


def test_new_module_state_bypasses_equal_global_weights_sequence():
    """Test the real EnvRunner state version gate without starting a Ray cluster.

    Existing modules may be at the current global weight sequence while a
    just-created module still contains its default weights. The first update
    of that module must bypass the sequence gate without advancing the global
    version, otherwise the opponent plays with its initialization.
    """
    from unittest.mock import MagicMock

    runner = MagicMock()
    runner._weights_seq_no = 7
    new_module_state = {"main_1": {"weights": [1, 2, 3]}}

    # A normal versioned sync is ignored when global sequence numbers match.
    MultiAgentEnvRunner.set_state(
        runner,
        {COMPONENT_RL_MODULE: new_module_state, WEIGHTS_SEQ_NO: 7},
    )
    runner.module.set_state.assert_not_called()
    assert runner._weights_seq_no == 7

    # A module-specific initial sync must explicitly force the state update,
    # while preserving the global learner/runner sequence number.
    MultiAgentEnvRunner.set_state(
        runner,
        {COMPONENT_RL_MODULE: new_module_state},
    )
    runner.module.set_state.assert_called_once_with(new_module_state)
    assert runner._weights_seq_no == 7
