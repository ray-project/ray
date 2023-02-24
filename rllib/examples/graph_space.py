"""Example of using Graph obs space and a GNN model.

This example shows:
  - using a custom environment with Graph space observations
  - using a custom GNN model to batch and process Graph space observations

The GNN model is based on tensorflow-gnn library.
"""

import argparse
import os

import ray
from ray import air, tune
from ray.rllib.algorithms.pg import PGConfig, PG
from ray.rllib.models import ModelCatalog
from ray.rllib.examples.env.graph_obs_env import GraphObsEnv
from ray.rllib.examples.models.graph_obs_model import TFGraphObsModel

import tensorflow_gnn as tfgnn

parser = argparse.ArgumentParser()
parser.add_argument(
    "--framework",
    choices=["tf", "tf2"],
    default="tf2",
    help="The DL framework specifier.",
)

if __name__ == "__main__":
    ray.init()
    args = parser.parse_args()

    # custom TF-GNN model
    ModelCatalog.register_custom_model("my_model", TFGraphObsModel)

    # define your graph (node/edge sets, context etc.) using a graph schema (see
    # https://github.com/tensorflow/gnn/blob/main/tensorflow_gnn/docs/guide/schema.md)
    graph_schema = tfgnn.parse_schema(
        """
        node_sets {
            key: "node_set"
            value {
                description: "A node set with name 'node_set'"
                features {
                    key: "default"
                    value {
                        description: "Observed node set features under name 'default'"
                        dtype: DT_FLOAT
                        shape { dim { size: 10 } }
                    }
                }
                metadata {
                    cardinality: 30
                }
            }
        }
        edge_sets {
            key: "edge_set"
            value {
                description: "An edge set with name 'edge_set'"
                source: "node_set"
                target: "node_set"
                features {
                    key: "default"
                    value {
                        description: "Observed edge set features under name 'default'"
                        dtype: DT_FLOAT
                        shape { dim { size: 5 } }
                    }
                }
            }
        }
        context {
            features {
                key: "global_context"
                value {
                    description: "(Global) context features for the hole graph"
                    dtype: DT_FLOAT
                    shape { dim { size: 20 } }
                }
            }
        }
        """
    )

    # Env/graph config
    env_config = {"graph_config": {"graph_schema": graph_schema}}

    config = (
        PGConfig()
        .environment(GraphObsEnv, env_config=env_config)
        .framework(args.framework)
        .rollouts(rollout_fragment_length=1, num_rollout_workers=0)
        .training(train_batch_size=2, model={"custom_model": "my_model"})
        .experimental(_disable_preprocessor_api=False)
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
    )

    # stop = {
    #     "timesteps_total": 1,
    # }

    # tuner = tune.Tuner(
    #     "PG",
    #     param_space=config.to_dict(),
    #     run_config=air.RunConfig(stop=stop, verbose=1),
    # )

    algo = PG(config=config)
    while algo.iteration < 2:
        train_progress_dict = algo.train()
        print(train_progress_dict)
