import torch

from ray.rllib.algorithms.ppo import PPOConfig


def test_ppo_checkpoint_restore_preserves_optimizer_state(tmp_path):
    config = (
        PPOConfig()
        .environment("CartPole-v1")
        .framework("torch")
        .env_runners(num_env_runners=0)
        .learners(num_learners=0)
        .training(
            train_batch_size=128,
            minibatch_size=32,
            num_epochs=1,
        )
    )

    algo1 = config.build_algo()
    try:
        # Populate optimizer state before checkpointing.
        algo1.train()

        learner1 = algo1.learner_group._learner
        optimizer1 = next(iter(learner1._named_optimizers.values()))
        original_state = optimizer1.state_dict()

        ckpt_dir = algo1.save_to_path(tmp_path / "ckpt")
    finally:
        algo1.stop()

    algo2 = config.build_algo()
    try:
        algo2.restore_from_path(ckpt_dir)

        learner2 = algo2.learner_group._learner
        optimizer2 = next(iter(learner2._named_optimizers.values()))
        restored_state = optimizer2.state_dict()
        param_group = optimizer2.param_groups[0]

        # Param-group metadata must remain Python scalars after restore.
        assert isinstance(param_group["betas"][0], float)
        assert isinstance(param_group["betas"][1], float)
        assert not torch.is_tensor(param_group["betas"][0])
        assert not torch.is_tensor(param_group["betas"][1])

        # Restored optimizer state should match the original optimizer state.
        assert restored_state["param_groups"] == original_state["param_groups"]
        assert restored_state["state"].keys() == original_state["state"].keys()

        for param_id, original_param_state in original_state["state"].items():
            restored_param_state = restored_state["state"][param_id]

            assert torch.equal(
                restored_param_state["step"], original_param_state["step"]
            )
            assert torch.equal(
                restored_param_state["exp_avg"], original_param_state["exp_avg"]
            )
            assert torch.equal(
                restored_param_state["exp_avg_sq"], original_param_state["exp_avg_sq"]
            )

        # Optimizer state buffers must still be tensors.
        state = next(iter(optimizer2.state.values()))
        assert torch.is_tensor(state["step"])
        assert torch.is_tensor(state["exp_avg"])
        assert torch.is_tensor(state["exp_avg_sq"])

        # Training must continue after restore.
        algo2.train()
    finally:
        algo2.stop()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
