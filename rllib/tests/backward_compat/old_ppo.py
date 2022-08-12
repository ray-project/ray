from ray.rllib.agents import with_common_config
from ray.rllib.utils.deprecation import DEPRECATED_VALUE


DEFAULT_CONFIG = with_common_config(
    {
        "use_critic": True,
        "use_gae": True,
        "lambda": 1.0,
        "kl_coeff": 0.2,
        "rollout_fragment_length": 200,
        "train_batch_size": 4000,
        "sgd_minibatch_size": 128,
        "shuffle_sequences": True,
        "num_sgd_iter": 30,
        "lr": 5e-5,
        "lr_schedule": None,
        "vf_loss_coeff": 1.0,
        "model": {
            "vf_share_layers": False,
        },
        "entropy_coeff": 0.0,
        "entropy_coeff_schedule": None,
        "clip_param": 0.3,
        "vf_clip_param": 10.0,
        "grad_clip": None,
        "kl_target": 0.01,
        "batch_mode": "truncate_episodes",
        "observation_filter": "NoFilter",
        "vf_share_layers": DEPRECATED_VALUE,
    }
)
