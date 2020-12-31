#!/usr/bin/env python

import argparse
import os
import sys

parser = argparse.ArgumentParser()
parser.add_argument(
    "--framework", choices=["jax", "tf", "torch"], default="tf")

if __name__ == "__main__":
    args = parser.parse_args()

    # Tests, whether everything works w/o TF|PyTorch|JAX being installed.
    # 0=env variable; 1=RLlib framework specifier, 2=python module, 3=full name
    dl_frameworks = {
        "tf": ("TF", "tf", "tensorflow", "TensorFlow"),
        "torch": ("TORCH", "torch", "torch", "PyTorch"),
        "jax": ("JAX", "jax", "jax", "JAX"),
    }

    fw = dl_frameworks[args.framework]

    # Do not import tf for testing purposes.
    env_var = "RLLIB_TEST_NO_{}_IMPORT".format(fw[0])
    os.environ[env_var] = "1"

    from ray.rllib.agents.ppo import PPOTrainer
    assert fw[2] not in sys.modules, \
        "{} initially present, when it shouldn't.".format(fw[3])

    # Note: no ray.init(), to test it works without Ray.
    # Try building an agent with one of the other frameworks.
    installed_framework = "torch" if fw[1] != "torch" else "tf"
    trainer = PPOTrainer(
        env="CartPole-v0", config={
            "framework": installed_framework,
            "num_workers": 0,
            "train_batch_size": 128,
        })
    print(trainer.train())
    assert fw[2] not in sys.modules,\
        "{} should not be imported".format(fw[3])
    # Clean up.
    del os.environ[env_var]

    print("ok")
