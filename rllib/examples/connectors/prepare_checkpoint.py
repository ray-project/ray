import random

from ray.rllib.algorithms.appo import APPOConfig
from ray.rllib.algorithms.sac import SACConfig


def create_appo_cartpole_checkpoint(output_dir):
    # enable_connectors defaults to True. Just trying to be explicit here.
    config = APPOConfig().environment("CartPole-v1").rollouts(enable_connectors=True)
    # Build algorithm object.
    algo = config.build()
    algo.save(checkpoint_dir=output_dir)


def create_open_spiel_checkpoint(output_dir):
    def _policy_mapping_fn(*args, **kwargs):
        random.choice(["main", "opponent"])

    config = (
        SACConfig()
        .environment("open_spiel_env")
        # Intentionally create a TF2 policy to demonstrate that we can restore
        # and use a TF policy in a Torch training stack.
        .framework("tf2")
        .rollouts(
            num_rollout_workers=1,
            num_envs_per_worker=5,
            # We will be restoring a TF2 policy.
            # So tell the RolloutWorkers to enable TF eager exec as well, even if
            # framework is set to torch.
            enable_tf1_exec_eagerly=True,
        )
        .training(model={"fcnet_hiddens": [512, 512]})
        .multi_agent(
            policies={"main", "opponent"},
            policy_mapping_fn=_policy_mapping_fn,
            # Just train the "main" policy.
            policies_to_train=["main"],
        )
    )
    # Build algorithm object.
    algo = config.build()
    algo.save(checkpoint_dir=output_dir)
