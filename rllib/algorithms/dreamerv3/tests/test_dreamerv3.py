import unittest

import ray
from ray.rllib.algorithms.dreamerv3 import dreamerv3
from ray.rllib.utils.test_utils import framework_iterator


class TestDreamerV3(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_dreamerv3_compilation(self):
        """Test whether DreamerV3 can be built with all frameworks."""

        # Build a DreamerV3Config object.
        config = (
            dreamerv3.DreamerV3Config()
            .framework(eager_tracing=True)
            .training(
                # TODO (sven): Fix having to provide this.
                #  Should be compiled by AlgorithmConfig?
                model={
                    "batch_size_B": 16,
                    "batch_length_T": 64,
                    "horizon_H": 15,
                    "model_dimension": "nano",  # Use a tiny model for testing.
                    "gamma": 0.997,
                    "training_ratio": 512,
                    "symlog_obs": True,
                },
                _enable_learner_api=True,
            )
            .resources(
                num_learner_workers=0,
                num_cpus_per_learner_worker=1,
                num_gpus_per_learner_worker=0,
                num_gpus=0,
            )
            .debugging(log_level="info")
            .rl_module(_enable_rl_module_api=True)
        )

        num_iterations = 2

        for _ in framework_iterator(config, frameworks="tf2"):
            for env in ["FrozenLake-v1", "CartPole-v1", "ALE/MsPacman-v5"]:
                print("Env={}".format(env))
                config.environment(env)
                algo = config.build()

                for i in range(num_iterations):
                    results = algo.train()
                    print(results)

                algo.stop()

    def test_dreamerv3_dreamer_model(self):
        from IPython.display import display, Image
        from moviepy.editor import ImageSequenceClip
        import time

        from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

        from utils.env_runner import EnvRunner

        B = 1
        T = 64
        burn_in_T = 5

        config = (
            AlgorithmConfig()
            .environment(
                "ALE/MsPacman-v5",
                env_config={
                    "repeat_action_probability": 0.25,
                    "full_action_space": True,
                    "frameskip": 1,
                },
            )
            .rollouts(num_envs_per_worker=16, rollout_fragment_length=burn_in_T + T)
        )
        # The vectorized gymnasium EnvRunner to collect samples of shape (B, T, ...).
        env_runner = EnvRunner(
            model=None, config=config, max_seq_len=None, continuous_episodes=True
        )

        # Our DreamerV3 world model.
        # from_checkpoint = 'C:\Dropbox\Projects\dreamer_v3\examples\checkpoints\mspacman_world_model_170'
        from_checkpoint = "/Users/sven/Dropbox/Projects/dreamer_v3/examples/checkpoints/mspacman_dreamer_model_60"
        dreamer_model = tf.keras.models.load_model(from_checkpoint)
        world_model = dreamer_model.world_model
        # TODO: ugly hack (resulting from the insane fact that you cannot know
        #  an env's spaces prior to actually constructing an instance of it) :(
        env_runner.model = dreamer_model

        # obs = np.random.randint(0, 256, size=(B, burn_in_T, 64, 64, 3), dtype=np.uint8)
        # actions = np.random.randint(0, 2, size=(B, burn_in_T), dtype=np.uint8)
        # initial_h = np.random.random(size=(B, 256)).astype(np.float32)

        sampled_obs, _, sampled_actions, _, _, _, sampled_h = env_runner.sample(
            random_actions=False
        )

        dreamed_trajectory = dreamer_model.dream_trajectory(
            sampled_obs[:, :burn_in_T],
            sampled_actions.astype(
                np.int64
            ),  # use all sampled actions, not random or actor-computed ones
            sampled_h,
            timesteps=T,
            # Use same actions as in the sample such that we can 100% compare
            # predicted vs actual observations.
            use_sampled_actions=True,
        )
        print(dreamed_trajectory)

        # Compute observations using h and z and the decoder net.
        # Note that the last h-state is NOT used here as it's already part of
        # a new trajectory.
        _, dreamed_images_distr = world_model.cnn_transpose_atari(
            tf.reshape(dreamed_trajectory["h_states_t1_to_Hp1"][:, :-1], (B * T, -1)),
            tf.reshape(
                dreamed_trajectory["z_dreamed_t1_to_Hp1"],
                (B * T) + dreamed_trajectory["z_dreamed_t1_to_Hp1"].shape[2:],
            ),
        )
        # Use mean() of the Gaussian, no sample.
        dreamed_images = dreamed_images_distr.mean()
        dreamed_images = tf.reshape(
            tf.cast(
                tf.clip_by_value(inverse_symlog(dreamed_images), 0.0, 255.0),
                tf.uint8,
            ),
            shape=(B, T, 64, 64, 3),
        ).numpy()

        # Stitch dreamed_obs and sampled_obs together for better comparison.
        images = np.concatenate([dreamed_images, sampled_obs[:, burn_in_T:]], axis=2)

        # Save sequence a gif.
        clip = ImageSequenceClip(list(images[0]), fps=2)
        clip.write_gif("test.gif", fps=2)
        Image("test.gif")
        time.sleep(10)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
