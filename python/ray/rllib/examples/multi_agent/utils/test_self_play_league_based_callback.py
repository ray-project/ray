from unittest.mock import MagicMock

from ray.rllib.core import COMPONENT_RL_MODULE
from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner
from ray.rllib.examples.multi_agent.utils import (
    self_play_league_based_callback as callback_module,
)
from ray.rllib.examples.multi_agent.utils.self_play_league_based_callback import (
    SelfPlayLeagueBasedCallback,
)
from ray.rllib.utils.metrics import ENV_RUNNER_RESULTS, WEIGHTS_SEQ_NO


def test_new_self_play_module_weights_are_synced_before_mapping(monkeypatch):
    callback = SelfPlayLeagueBasedCallback(win_rate_threshold=0.5)
    callback.trainable_policies = {"main", "league_exploiter_1"}

    main_module = MagicMock()
    main_module.get_state.return_value = {"weights": [1, 2, 3]}

    algorithm = MagicMock()
    algorithm.iteration = 1
    algorithm.env_runner.module = {"main": main_module}

    # Real add_module creates a separate, default-initialized local opponent.
    # The weight installation must instead use the existing source policy.
    new_module = MagicMock()
    new_module.get_state.return_value = {"weights": [-1, -1, -1]}

    def add_uninitialized_module(*, module_id, module_spec):
        assert module_id == "main_1"
        algorithm.env_runner.module[module_id] = new_module

    algorithm.add_module.side_effect = add_uninitialized_module
    monkeypatch.setattr(
        callback_module.RLModuleSpec, "from_module", lambda module: object()
    )

    state_ref = object()
    captured_state = {}
    events = []

    def fake_put(state):
        captured_state["value"] = state
        return state_ref

    def apply_to_local_runner(func, **kwargs):
        # The production API is synchronous by default (timeout_seconds=None).
        # Execute callbacks here so the test verifies actual operation ordering,
        # not only the order in which foreach_env_runner was called.
        assert kwargs["local_env_runner"] is True
        return [func(env_runner)]

    env_runner = MagicMock()
    env_runner.set_state.side_effect = lambda state: events.append(("state", state))
    env_runner.config.multi_agent.side_effect = lambda **kwargs: events.append(
        ("mapping", kwargs)
    )

    monkeypatch.setattr(callback_module.ray, "put", fake_put)
    algorithm.env_runner_group.foreach_env_runner.side_effect = apply_to_local_runner

    callback.on_train_result(
        algorithm=algorithm,
        result={ENV_RUNNER_RESULTS: {"win_rate_main": 0.9}},
    )

    algorithm.add_module.assert_called_once()
    algorithm.set_state.assert_called_once()
    new_module.get_state.assert_not_called()
    assert captured_state["value"] == {"main_1": {"weights": [1, 2, 3]}}
    assert len(algorithm.env_runner_group.foreach_env_runner.call_args_list) == 1

    # The new module's weights must be installed before the mapping can route
    # episodes to it; the module-only state intentionally omits WEIGHTS_SEQ_NO.
    assert [event[0] for event in events] == ["state", "mapping"]
    assert events[0][1] == {COMPONENT_RL_MODULE: state_ref}
    assert "policy_mapping_fn" in events[1][1]


def test_initial_league_clones_are_installed_before_first_mapping(monkeypatch):
    """The first three copied opponents need the same forced-install contract."""
    callback = SelfPlayLeagueBasedCallback(win_rate_threshold=0.5)
    main_state = {"weights": [11, 13, 17]}
    module_ids = ("main", "main_0", "league_exploiter_1", "main_exploiter_1")
    modules = {mid: MagicMock() for mid in module_ids}
    for module in modules.values():
        module.get_state.return_value = main_state

    local_modules = MagicMock()
    local_modules.__getitem__.side_effect = modules.__getitem__
    local_modules.get_state.return_value = {mid: main_state for mid in module_ids}

    algorithm = MagicMock()
    algorithm.iteration = 1
    algorithm.env_runner.module = local_modules

    ref = object()
    published = []
    monkeypatch.setattr(
        callback_module.ray, "put", lambda states: published.append(states) or ref
    )

    events = []
    remote = MagicMock()
    remote.set_state.side_effect = lambda state: events.append(("weights", state))
    remote.config.multi_agent.side_effect = lambda **kwargs: events.append(
        ("mapping", kwargs)
    )

    def run_on_env_runners(func, **kwargs):
        assert kwargs["local_env_runner"] is True
        return [func(remote)]

    algorithm.env_runner_group.foreach_env_runner.side_effect = run_on_env_runners
    callback.on_train_result(
        algorithm=algorithm,
        result={ENV_RUNNER_RESULTS: {"win_rate_main": 0.9}},
    )
    assert published == [
        {
            mid: main_state
            for mid in ("main_0", "league_exploiter_1", "main_exploiter_1")
        }
    ]
    assert algorithm.env_runner_group.foreach_env_runner.call_count == 1
    assert [event[0] for event in events] == ["weights", "mapping"]
    assert events[0][1] == {COMPONENT_RL_MODULE: ref}
    assert "policy_mapping_fn" in events[1][1]
    algorithm.env_runner_group.sync_weights.assert_not_called()
    algorithm.learner_group.set_weights.assert_called_once()
    algorithm.add_module.assert_not_called()
    for mid in ("main_0", "league_exploiter_1", "main_exploiter_1"):
        modules[mid].set_state.assert_called_once_with(main_state)


def test_new_module_state_bypasses_equal_global_weights_sequence():
    """A new module's first state must bypass the global sequence-number gate."""
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

    # A module-specific initial sync forces the state update without changing
    # the global learner/runner sequence number.
    MultiAgentEnvRunner.set_state(
        runner,
        {COMPONENT_RL_MODULE: new_module_state},
    )
    runner.module.set_state.assert_called_once_with(new_module_state)
    assert runner._weights_seq_no == 7
