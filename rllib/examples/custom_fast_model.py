"""Example of using a custom image env and model.

Both the model and env are trivial (and super-fast), so they are useful
for running perf microbenchmarks.
"""

import ray
from ray.rllib.examples.env.fast_image_env import FastImageEnv
from ray.rllib.models import Model, ModelCatalog
from ray.tune import run_experiments, sample_from
from ray.rllib.utils import try_import_tf

tf = try_import_tf()


class FastModel(Model):
    def _build_layers_v2(self, input_dict, num_outputs, options):
        bias = tf.get_variable(
            dtype=tf.float32,
            name="bias",
            initializer=tf.zeros_initializer,
            shape=())
        output = bias + tf.zeros([tf.shape(input_dict["obs"])[0], num_outputs])
        return output, output


if __name__ == "__main__":
    ray.init()
    ModelCatalog.register_custom_model("fast_model", FastModel)
    run_experiments({
        "demo": {
            "run": "IMPALA",
            "env": FastImageEnv,
            "config": {
                "compress_observations": True,
                "model": {
                    "custom_model": "fast_model"
                },
                "num_gpus": 0,
                "num_workers": 2,
                "num_envs_per_worker": 10,
                "num_data_loader_buffers": 1,
                "num_aggregation_workers": 1,
                "broadcast_interval": 50,
                "rollout_fragment_length": 100,
                "train_batch_size": sample_from(
                    lambda spec: 1000 * max(1, spec.config.num_gpus)),
                "_fake_sampler": True,
            },
        },
    })
