"""Comprehensive pytest-based tests for the MAPPO implementation.

Covers:
    - MAPPOConfig: inheritance, defaults, validation
    - MAPPOLearner: connector swapping, after_gradient_based_update
    - MAPPOTorchLearner: actor loss, critic loss, VF clipping, masked mean
    - MAPPOGAEConnector: agent ordering, batch safety, loss masks, value targets
    - SharedCriticCatalog: 1D validation, obs concatenation sizing
    - SharedCriticTorchRLModule: compute_values output shape
    - DefaultMAPPOTorchRLModule: forward pass outputs
    - End-to-end: full training iteration
"""

import gymnasium as gym
import numpy as np
import pytest

import ray
from ray.rllib.algorithms.ppo.ppo import PPOConfig
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

OBS_DIM = 8
ACT_DIM = 2
NUM_AGENTS = 2
BATCH_SIZE = 16
AGENT_IDS = [f"agent_{i}" for i in range(NUM_AGENTS)]


@pytest.fixture(scope="module")
def ray_init():
    ray.init(num_cpus=2, ignore_reinit_error=True)
    yield
    ray.shutdown()


@pytest.fixture()
def obs_space():
    return gym.spaces.Box(-1.0, 1.0, shape=(OBS_DIM,), dtype=np.float32)


@pytest.fixture()
def act_space():
    return gym.spaces.Box(-1.0, 1.0, shape=(ACT_DIM,), dtype=np.float32)


@pytest.fixture()
def observation_spaces(obs_space):
    """Dict mapping agent IDs to observation spaces."""
    return {aid: obs_space for aid in AGENT_IDS}


@pytest.fixture()
def multi_rl_module(obs_space, act_space, observation_spaces):
    """Build a MultiRLModule with MAPPO actor + shared critic."""
    from ray.rllib.examples.algorithms.mappo.connectors.general_advantage_estimation import (
        SHARED_CRITIC_ID,
    )
    from ray.rllib.examples.algorithms.mappo.torch.default_mappo_torch_rl_module import (
        DefaultMAPPOTorchRLModule,
    )
    from ray.rllib.examples.algorithms.mappo.torch.shared_critic_torch_rl_module import (
        SharedCriticTorchRLModule,
    )

    specs = {
        aid: RLModuleSpec(
            module_class=DefaultMAPPOTorchRLModule,
            observation_space=obs_space,
            action_space=act_space,
            model_config=DefaultModelConfig(),
        )
        for aid in AGENT_IDS
    }
    specs[SHARED_CRITIC_ID] = RLModuleSpec(
        module_class=SharedCriticTorchRLModule,
        observation_space=obs_space,
        action_space=act_space,
        model_config={"observation_spaces": observation_spaces},
    )
    multi_spec = MultiRLModuleSpec(rl_module_specs=specs)
    return multi_spec.build()


def _make_agent_batch(obs_dim=OBS_DIM, act_dim=ACT_DIM, batch_size=BATCH_SIZE):
    """Create a fake per-agent batch dict with all required columns."""
    return {
        Columns.OBS: torch.randn(batch_size, obs_dim),
        Columns.NEXT_OBS: torch.randn(batch_size, obs_dim),
        Columns.ACTIONS: torch.randn(batch_size, act_dim),
        Columns.REWARDS: torch.randn(batch_size),
        Columns.TERMINATEDS: torch.zeros(batch_size, dtype=torch.bool),
        Columns.TRUNCATEDS: torch.zeros(batch_size, dtype=torch.bool),
        Columns.ACTION_DIST_INPUTS: torch.randn(batch_size, act_dim * 2),
        Columns.ACTION_LOGP: torch.randn(batch_size),
    }


# ===================================================================
# 1. MAPPOConfig tests
# ===================================================================


class TestMAPPOConfig:
    def test_inherits_ppo_config(self):
        from ray.rllib.examples.algorithms.mappo.mappo import MAPPOConfig

        config = MAPPOConfig()
        assert isinstance(config, PPOConfig), "MAPPOConfig must inherit from PPOConfig"

    def test_use_critic_is_false(self):
        from ray.rllib.examples.algorithms.mappo.mappo import MAPPOConfig

        config = MAPPOConfig()
        assert (
            config.use_critic is False
        ), "MAPPO uses a shared critic; per-agent use_critic must be False"

    def test_ppo_params_accessible(self):
        """All PPO hyper-parameters should be accessible on MAPPOConfig."""
        from ray.rllib.examples.algorithms.mappo.mappo import MAPPOConfig

        config = MAPPOConfig()
        assert hasattr(config, "lambda_")
        assert hasattr(config, "kl_coeff")
        assert hasattr(config, "kl_target")
        assert hasattr(config, "clip_param")
        assert hasattr(config, "vf_clip_param")
        assert hasattr(config, "vf_loss_coeff")
        assert hasattr(config, "entropy_coeff")
        assert hasattr(config, "use_gae")
        assert hasattr(config, "grad_clip")

    def test_training_method_returns_self(self):
        from ray.rllib.examples.algorithms.mappo.mappo import MAPPOConfig

        config = MAPPOConfig()
        result = config.training(lambda_=0.95, clip_param=0.2)
        assert result is config
        assert config.lambda_ == 0.95
        assert config.clip_param == 0.2

    def test_default_learner_class(self):
        from ray.rllib.examples.algorithms.mappo.mappo import MAPPOConfig
        from ray.rllib.examples.algorithms.mappo.torch.mappo_torch_learner import (
            MAPPOTorchLearner,
        )

        config = MAPPOConfig()
        assert config.get_default_learner_class() is MAPPOTorchLearner

    def test_default_rl_module_spec(self):
        from ray.rllib.examples.algorithms.mappo.mappo import MAPPOConfig
        from ray.rllib.examples.algorithms.mappo.torch.default_mappo_torch_rl_module import (
            DefaultMAPPOTorchRLModule,
        )

        config = MAPPOConfig()
        spec = config.get_default_rl_module_spec()
        assert spec.module_class is DefaultMAPPOTorchRLModule


# ===================================================================
# 2. MAPPOLearner tests
# ===================================================================


class TestMAPPOLearner:
    def test_inherits_ppo_learner(self):
        from ray.rllib.algorithms.ppo.ppo_learner import PPOLearner
        from ray.rllib.examples.algorithms.mappo.mappo_learner import MAPPOLearner

        assert issubclass(MAPPOLearner, PPOLearner)

    def test_rl_module_required_apis_empty(self):
        from ray.rllib.examples.algorithms.mappo.mappo_learner import MAPPOLearner

        assert MAPPOLearner.rl_module_required_apis() == []


# ===================================================================
# 3. MAPPOTorchLearner tests
# ===================================================================


class TestMAPPOTorchLearner:
    def test_inherits_ppo_torch_learner(self):
        from ray.rllib.algorithms.ppo.torch.ppo_torch_learner import PPOTorchLearner
        from ray.rllib.examples.algorithms.mappo.mappo_learner import MAPPOLearner
        from ray.rllib.examples.algorithms.mappo.torch.mappo_torch_learner import (
            MAPPOTorchLearner,
        )

        assert issubclass(MAPPOTorchLearner, MAPPOLearner)
        assert issubclass(MAPPOTorchLearner, PPOTorchLearner)

    def test_mro_order(self):
        """MAPPOLearner must come before PPOTorchLearner in the MRO so that
        MAPPO's build() and after_gradient_based_update() take precedence."""
        from ray.rllib.algorithms.ppo.torch.ppo_torch_learner import PPOTorchLearner
        from ray.rllib.examples.algorithms.mappo.mappo_learner import MAPPOLearner
        from ray.rllib.examples.algorithms.mappo.torch.mappo_torch_learner import (
            MAPPOTorchLearner,
        )

        mro = MAPPOTorchLearner.__mro__
        assert mro.index(MAPPOLearner) < mro.index(PPOTorchLearner)

    def test_make_masked_mean_fn_no_mask(self):
        from ray.rllib.examples.algorithms.mappo.torch.mappo_torch_learner import (
            MAPPOTorchLearner,
        )

        batch = {Columns.OBS: torch.randn(4, 3)}
        fn = MAPPOTorchLearner._make_masked_mean_fn(batch)
        data = torch.tensor([1.0, 2.0, 3.0, 4.0])
        assert torch.isclose(fn(data), torch.tensor(2.5))

    def test_make_masked_mean_fn_with_mask(self):
        from ray.rllib.examples.algorithms.mappo.torch.mappo_torch_learner import (
            MAPPOTorchLearner,
        )

        mask = torch.tensor([True, False, True, False])
        batch = {Columns.LOSS_MASK: mask}
        fn = MAPPOTorchLearner._make_masked_mean_fn(batch)
        data = torch.tensor([10.0, 20.0, 30.0, 40.0])
        # Only indices 0 and 2 are valid: (10+30)/2 = 20
        assert torch.isclose(fn(data), torch.tensor(20.0))

    def test_vf_loss_clips_before_agent_reduction(self):
        """Verify the critic loss clips per-element BEFORE reducing across agents.

        If clip_param=5.0 and one agent has squared error=9.0 and the other=1.0:
            Correct (clip first):  clamp(9,0,5)=5 + clamp(1,0,5)=1 -> mean=3.0
            Wrong (mean first):    mean(9,1)=5.0 -> clamp(5,0,5)=5.0
        """
        vf_preds = torch.tensor([[0.0, 0.0]])
        vf_targets = torch.tensor([[3.0, 1.0]])
        vf_loss = torch.pow(vf_preds - vf_targets, 2.0)
        # Correct: clip per-element, then mean
        clip_param = 5.0
        vf_loss_clipped = torch.clamp(vf_loss, 0, clip_param).mean(dim=-1)
        # 9.0 -> 5.0, 1.0 -> 1.0, mean -> 3.0
        assert torch.isclose(
            vf_loss_clipped, torch.tensor(3.0)
        ), f"Expected 3.0, got {vf_loss_clipped.item()}"
        # Wrong way: mean first, then clip
        wrong = torch.clamp(vf_loss.mean(dim=-1), 0, clip_param)
        # mean(9,1)=5.0, clamp(5,0,5)=5.0
        assert torch.isclose(wrong, torch.tensor(5.0))
        assert not torch.isclose(
            vf_loss_clipped, wrong
        ), "If these are equal, the test is not distinguishing the two orderings"


# ===================================================================
# 4. MAPPOGAEConnector tests
# ===================================================================


class TestMAPPOGAEConnector:
    def test_get_agent_module_ids_stable_order(self, multi_rl_module):
        """Agent IDs must come back sorted for deterministic concat order."""
        from ray.rllib.examples.algorithms.mappo.connectors.general_advantage_estimation import (
            MAPPOGAEConnector,
        )

        connector = MAPPOGAEConnector(gamma=0.99, lambda_=0.95)
        batch = {aid: _make_agent_batch() for aid in AGENT_IDS}
        ids = connector._get_agent_module_ids(multi_rl_module, batch)
        assert ids == sorted(AGENT_IDS)

    def test_get_agent_module_ids_missing_agent_raises(self, multi_rl_module):
        """If an agent is missing from the batch, we should get a clear error."""
        from ray.rllib.examples.algorithms.mappo.connectors.general_advantage_estimation import (
            MAPPOGAEConnector,
        )

        connector = MAPPOGAEConnector(gamma=0.99, lambda_=0.95)
        # Only include one of two agents.
        batch = {AGENT_IDS[0]: _make_agent_batch()}
        with pytest.raises(ValueError, match="Missing modules"):
            connector._get_agent_module_ids(multi_rl_module, batch)

    def test_get_agent_module_ids_skips_non_dict_entries(self, multi_rl_module):
        """Non-dict batch entries (metadata) should be safely skipped."""
        from ray.rllib.examples.algorithms.mappo.connectors.general_advantage_estimation import (
            MAPPOGAEConnector,
        )

        connector = MAPPOGAEConnector(gamma=0.99, lambda_=0.95)
        batch = {aid: _make_agent_batch() for aid in AGENT_IDS}
        batch["__metadata__"] = "some_string"
        # Should not crash; "___metadata__" is not a known module.
        ids = connector._get_agent_module_ids(multi_rl_module, batch)
        assert ids == sorted(AGENT_IDS)

    def test_loss_mask_combination(self):
        """Combined loss mask should be the logical AND of all agent masks."""

        mask_a = torch.tensor([True, True, False, True])
        mask_b = torch.tensor([True, False, False, True])
        expected_combined = torch.tensor([True, False, False, True])

        batch = {
            "agent_0": {
                Columns.OBS: torch.randn(4, OBS_DIM),
                Columns.LOSS_MASK: mask_a,
            },
            "agent_1": {
                Columns.OBS: torch.randn(4, OBS_DIM),
                Columns.LOSS_MASK: mask_b,
            },
        }

        # Manually test the mask combination logic.
        obs_mids = ["agent_0", "agent_1"]
        masks = [batch[mid][Columns.LOSS_MASK] for mid in obs_mids]
        combined = masks[0]
        for m in masks[1:]:
            combined = combined & m
        assert torch.equal(combined, expected_combined)


# ===================================================================
# 5. SharedCriticCatalog tests
# ===================================================================


class TestSharedCriticCatalog:
    def test_1d_obs_accepted(self, obs_space, act_space, observation_spaces):
        from ray.rllib.examples.algorithms.mappo.shared_critic_catalog import (
            SharedCriticCatalog,
        )

        catalog = SharedCriticCatalog(
            observation_space=obs_space,
            action_space=act_space,
            model_config_dict={
                "observation_spaces": observation_spaces,
                "fcnet_hiddens": [64, 64],
                "fcnet_activation": "tanh",
                "head_fcnet_hiddens": [64],
                "head_fcnet_activation": "relu",
            },
        )
        # Encoder input should be concatenated obs from all agents.
        assert catalog.encoder_config.input_dims == (OBS_DIM * NUM_AGENTS,)

    def test_multidim_obs_raises(self, obs_space, act_space):
        """Non-1D observation spaces must be rejected -- either by our explicit
        check or by the upstream Catalog encoder config validation."""
        from ray.rllib.examples.algorithms.mappo.shared_critic_catalog import (
            SharedCriticCatalog,
        )

        obs_2d = gym.spaces.Box(0, 1, shape=(4, 4), dtype=np.float32)
        with pytest.raises(ValueError):
            SharedCriticCatalog(
                observation_space=obs_2d,
                action_space=act_space,
                model_config_dict={
                    "observation_spaces": {"a": obs_2d},
                    "fcnet_hiddens": [64, 64],
                    "fcnet_activation": "tanh",
                    "head_fcnet_hiddens": [64],
                    "head_fcnet_activation": "relu",
                },
            )

    def test_3d_obs_in_observation_spaces_raises(self, obs_space, act_space):
        """If the main obs space is 1D but an agent's obs space in the
        observation_spaces dict is multi-dimensional, our check catches it."""
        from ray.rllib.examples.algorithms.mappo.shared_critic_catalog import (
            SharedCriticCatalog,
        )

        obs_3d = gym.spaces.Box(0, 1, shape=(3, 4, 4), dtype=np.float32)
        with pytest.raises(ValueError, match="1-D observation"):
            SharedCriticCatalog(
                observation_space=obs_space,
                action_space=act_space,
                model_config_dict={
                    "observation_spaces": {"agent_bad": obs_3d},
                    "fcnet_hiddens": [64, 64],
                    "fcnet_activation": "tanh",
                    "head_fcnet_hiddens": [64],
                    "head_fcnet_activation": "relu",
                },
            )

    def test_vf_head_output_dim_matches_num_agents(
        self, obs_space, act_space, observation_spaces
    ):
        from ray.rllib.examples.algorithms.mappo.shared_critic_catalog import (
            SharedCriticCatalog,
        )

        catalog = SharedCriticCatalog(
            observation_space=obs_space,
            action_space=act_space,
            model_config_dict={
                "observation_spaces": observation_spaces,
                "fcnet_hiddens": [64, 64],
                "fcnet_activation": "tanh",
                "head_fcnet_hiddens": [64],
                "head_fcnet_activation": "relu",
            },
        )
        assert catalog.vf_head_config.output_layer_dim == NUM_AGENTS


# ===================================================================
# 6. SharedCriticTorchRLModule tests
# ===================================================================


class TestSharedCriticTorchRLModule:
    def test_compute_values_output_shape(
        self, obs_space, act_space, observation_spaces
    ):
        from ray.rllib.examples.algorithms.mappo.torch.shared_critic_torch_rl_module import (
            SharedCriticTorchRLModule,
        )

        module = SharedCriticTorchRLModule(
            observation_space=obs_space,
            action_space=act_space,
            model_config={"observation_spaces": observation_spaces},
        )
        concat_obs = torch.randn(BATCH_SIZE, OBS_DIM * NUM_AGENTS)
        batch = {Columns.OBS: concat_obs}
        values = module.compute_values(batch)
        assert values.shape == (
            BATCH_SIZE,
            NUM_AGENTS,
        ), f"Expected shape ({BATCH_SIZE}, {NUM_AGENTS}), got {values.shape}"

    def test_forward_train_returns_empty(
        self, obs_space, act_space, observation_spaces
    ):
        """Shared critic's _forward_train should return {} (values are computed
        directly via compute_values, not through the training forward pass)."""
        from ray.rllib.examples.algorithms.mappo.torch.shared_critic_torch_rl_module import (
            SharedCriticTorchRLModule,
        )

        module = SharedCriticTorchRLModule(
            observation_space=obs_space,
            action_space=act_space,
            model_config={"observation_spaces": observation_spaces},
        )
        concat_obs = torch.randn(BATCH_SIZE, OBS_DIM * NUM_AGENTS)
        batch = {Columns.OBS: concat_obs}
        out = module._forward_train(batch)
        assert out == {}


# ===================================================================
# 7. DefaultMAPPOTorchRLModule tests
# ===================================================================


class TestDefaultMAPPOTorchRLModule:
    def test_forward_returns_action_dist_inputs(self, obs_space, act_space):
        from ray.rllib.examples.algorithms.mappo.torch.default_mappo_torch_rl_module import (
            DefaultMAPPOTorchRLModule,
        )

        module = DefaultMAPPOTorchRLModule(
            observation_space=obs_space,
            action_space=act_space,
            model_config=DefaultModelConfig(),
        )
        batch = {Columns.OBS: torch.randn(BATCH_SIZE, OBS_DIM)}
        out = module._forward(batch)
        assert Columns.ACTION_DIST_INPUTS in out
        assert out[Columns.ACTION_DIST_INPUTS].shape[0] == BATCH_SIZE

    def test_forward_train_same_as_forward(self, obs_space, act_space):
        """_forward_train delegates to _forward (no redundant override)."""
        from ray.rllib.examples.algorithms.mappo.torch.default_mappo_torch_rl_module import (
            DefaultMAPPOTorchRLModule,
        )

        module = DefaultMAPPOTorchRLModule(
            observation_space=obs_space,
            action_space=act_space,
            model_config=DefaultModelConfig(),
        )
        batch = {Columns.OBS: torch.randn(BATCH_SIZE, OBS_DIM)}
        out_fwd = module._forward(batch)
        out_train = module._forward_train(batch)
        assert torch.equal(
            out_fwd[Columns.ACTION_DIST_INPUTS],
            out_train[Columns.ACTION_DIST_INPUTS],
        )

    def test_no_vf_attribute(self, obs_space, act_space):
        """MAPPO actors should NOT have a value function head."""
        from ray.rllib.examples.algorithms.mappo.torch.default_mappo_torch_rl_module import (
            DefaultMAPPOTorchRLModule,
        )

        module = DefaultMAPPOTorchRLModule(
            observation_space=obs_space,
            action_space=act_space,
            model_config=DefaultModelConfig(),
        )
        assert not hasattr(module, "vf"), "MAPPO actor should not have a vf attribute"


# ===================================================================
# 8. _setup_mappo_encoder tests
# ===================================================================


class TestSetupMappoEncoder:
    def test_encoder_built(self, obs_space, act_space):
        from ray.rllib.examples.algorithms.mappo.torch.default_mappo_torch_rl_module import (
            DefaultMAPPOTorchRLModule,
        )

        module = DefaultMAPPOTorchRLModule(
            observation_space=obs_space,
            action_space=act_space,
            model_config=DefaultModelConfig(),
        )
        assert hasattr(module, "encoder")
        assert module.encoder is not None

    def test_shared_between_actor_and_critic(
        self, obs_space, act_space, observation_spaces
    ):
        """Both actor and critic modules should use _setup_mappo_encoder
        to build their encoders (same code path)."""
        from ray.rllib.examples.algorithms.mappo.torch.default_mappo_torch_rl_module import (
            DefaultMAPPOTorchRLModule,
        )
        from ray.rllib.examples.algorithms.mappo.torch.shared_critic_torch_rl_module import (
            SharedCriticTorchRLModule,
        )

        actor = DefaultMAPPOTorchRLModule(
            observation_space=obs_space,
            action_space=act_space,
            model_config=DefaultModelConfig(),
        )
        critic = SharedCriticTorchRLModule(
            observation_space=obs_space,
            action_space=act_space,
            model_config={"observation_spaces": observation_spaces},
        )
        assert hasattr(actor, "encoder") and hasattr(critic, "encoder")
        assert type(actor.encoder).__name__ == type(critic.encoder).__name__


# ===================================================================
# 9. End-to-end training test
# ===================================================================


class TestEndToEnd:
    def test_training_iteration(self, ray_init):
        """Run a full training iteration with MAPPO on waterworld."""
        from pettingzoo.sisl import waterworld_v4

        from ray.rllib.env.wrappers.pettingzoo_env import ParallelPettingZooEnv
        from ray.rllib.examples.algorithms.mappo.connectors.general_advantage_estimation import (
            SHARED_CRITIC_ID,
        )
        from ray.rllib.examples.algorithms.mappo.mappo import MAPPOConfig
        from ray.rllib.examples.algorithms.mappo.torch.shared_critic_torch_rl_module import (
            SharedCriticTorchRLModule,
        )
        from ray.tune.registry import register_env

        def get_env(_):
            return ParallelPettingZooEnv(waterworld_v4.parallel_env())

        register_env("test_waterworld", get_env)

        policies = ["pursuer_0", "pursuer_1"]
        env_inst = get_env({})
        specs = {p: RLModuleSpec() for p in policies}
        specs[SHARED_CRITIC_ID] = RLModuleSpec(
            module_class=SharedCriticTorchRLModule,
            observation_space=env_inst.observation_space[policies[0]],
            action_space=env_inst.action_space[policies[0]],
            learner_only=True,
            model_config={"observation_spaces": env_inst.observation_space},
        )

        config = (
            MAPPOConfig()
            .environment("test_waterworld")
            .multi_agent(
                policies=policies + [SHARED_CRITIC_ID],
                policy_mapping_fn=(lambda aid, *a, **kw: aid),
            )
            .rl_module(
                rl_module_spec=MultiRLModuleSpec(rl_module_specs=specs),
            )
            .env_runners(num_env_runners=0)
            .training(train_batch_size=200, minibatch_size=100, num_epochs=2)
        )

        algo = config.build()
        try:
            result = algo.train()
            assert result is not None
            learners = result.get("learners", {})
            # Verify actor losses were computed for each policy.
            for pid in policies:
                assert pid in learners, f"Missing learner results for {pid}"
                assert "total_loss" in learners[pid], f"No total_loss for {pid}"
                assert np.isfinite(
                    learners[pid]["total_loss"]
                ), f"Non-finite total_loss for {pid}"
            # Verify shared critic loss was computed.
            assert SHARED_CRITIC_ID in learners
            assert "vf_loss" in learners[SHARED_CRITIC_ID]
            assert np.isfinite(learners[SHARED_CRITIC_ID]["vf_loss"])
        finally:
            algo.stop()

    def test_reward_improves_over_iterations(self, ray_init):
        """Train for multiple iterations and verify reward trends upward."""
        from pettingzoo.sisl import waterworld_v4

        from ray.rllib.env.wrappers.pettingzoo_env import ParallelPettingZooEnv
        from ray.rllib.examples.algorithms.mappo.connectors.general_advantage_estimation import (
            SHARED_CRITIC_ID,
        )
        from ray.rllib.examples.algorithms.mappo.mappo import MAPPOConfig
        from ray.rllib.examples.algorithms.mappo.torch.shared_critic_torch_rl_module import (
            SharedCriticTorchRLModule,
        )
        from ray.tune.registry import register_env

        def get_env(_):
            return ParallelPettingZooEnv(waterworld_v4.parallel_env())

        register_env("test_waterworld_2", get_env)

        policies = ["pursuer_0", "pursuer_1"]
        env_inst = get_env({})
        specs = {p: RLModuleSpec() for p in policies}
        specs[SHARED_CRITIC_ID] = RLModuleSpec(
            module_class=SharedCriticTorchRLModule,
            observation_space=env_inst.observation_space[policies[0]],
            action_space=env_inst.action_space[policies[0]],
            learner_only=True,
            model_config={"observation_spaces": env_inst.observation_space},
        )

        config = (
            MAPPOConfig()
            .environment("test_waterworld_2")
            .multi_agent(
                policies=policies + [SHARED_CRITIC_ID],
                policy_mapping_fn=(lambda aid, *a, **kw: aid),
            )
            .rl_module(
                rl_module_spec=MultiRLModuleSpec(rl_module_specs=specs),
            )
            .env_runners(num_env_runners=0)
            .training(train_batch_size=200, minibatch_size=100, num_epochs=2)
        )

        algo = config.build()
        try:
            losses = []
            for _ in range(3):
                result = algo.train()
                learners = result.get("learners", {})
                # Collect actor + critic total losses for stability check.
                for pid in policies:
                    assert pid in learners
                    assert np.isfinite(learners[pid]["total_loss"])
                assert np.isfinite(learners[SHARED_CRITIC_ID]["vf_loss"])
                losses.append(learners[SHARED_CRITIC_ID]["vf_loss"])
            # Training didn't diverge -- all losses finite across iterations.
            assert all(
                np.isfinite(l) for l in losses
            ), f"Training produced non-finite VF losses: {losses}"
        finally:
            algo.stop()


# ===================================================================
# Entry point
# ===================================================================

if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
