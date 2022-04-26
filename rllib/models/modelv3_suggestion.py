import gym
import numpy as np
import torch

from ray.rllib.models.catalog import ModelCatalog


MODEL_DEFAULTS = {
    # use as-is

    # plus:
    # By default, the input is the observation from the env.
    # You can also specify a model name here to indicate that the input
    # to this model should be the output of another (named) model.
    "input_source": None,
    # Do we need to do anything with input (e.g. frame-stacking)?
    # Individual models are allowed to override these `input_requirements`
    # in their c'tors.
    "input_requirements": None,
    # e.g. frame-stacking last 4 frames:
    #    "obs": {
    #        "shift": [-3, -2, -1, 0],
    #    },
    #},
    # By default, do NOT add an extra (dense) output layer.
    "output_layer_size": None,
    # But if we do, by default, make it linear.
    "output_layer_activation": None,
}

# SimpleQ/PG
TRAINER_DEFAULT_CONFIG = {
    # use as-is

    # plus:
    "models": {
        "core": None,  # No core.
        # Single policy model with standard action head.
        "policy_model": {
            "output_layer_size": "action_space",
        },
    },
}
# Benefits: Much more transparent as to what happens under the hood.


# Example configs for different algorithms and setups.
# ----------------------------------------------------

# PPO/A[23]C/IMPALA w/ shared value function.
config = {
    "models": {
        # Core model w/o final linear layer (output_layer_size=None)
        "core": {},  # True also ok here, just not None or False, which would mean: no core.
        # Policy model using core's outputs as inputs.
        # Outputting action space-dependent (linear) layer.
        "policy_model": {
            "input_source": "core",
            "fcnet_hiddens": [],
            "output_layer_size": "action_space",  # <- special key (get necessary units from given action-space)
        },
        # Vf model using core's outputs as inputs.
        # Outputting single (linear) value.
        "value_function": {
            "input_source": "core",
            "output_layer_size": 1,
        },
    },
}

# PPO/A[23]C/PG/IMPALA w/ separate value function.
config = {
    "models": {
        # No core!
        "core": None,  # False also ok here.
        # Independent policy model.
        "policy_model": {
            "output_layer_size": "action_space",
        },
        # Independent vf model.
        "value_function": {
            "output_layer_size": 1,
        },
    },
}
# Advantage: Gets rid of all default models' built-in value function
# branch code (up to 50% of our default model's code!!).


# PPO w/ custom model wrapped by an LSTM (shared vf) and prev_actions=True.
config = {
    "models": {
        "core": {
            "custom_model": "my_model",
            # some config passed to the custom model's c'tor
            "custom_model_config": {},
            # Wrap custom model w/ LSTM.
            "use_lstm": True,
            # ModelCatalog will - while doing the wrapping -
            # add the correct `input_requirements` to the final
            # "core" model.
            "lstm_use_prev_actions": True,
        },
        # Use LSTM output as input for policy and vf models.
        "policy_model": {
            "input_source": "core",
            "fcnet_hiddens": [],
            "output_layer_size": "action_space",
        },
        "value_function": {
            "input_source": "core",
            "fcnet_hiddens": [],
            "output_layer_size": 1,
        },
    }
}

# PPO w/ custom model wrapped by an attention net (separate vf).
config = {
    "models": {
        "core": None,
        "policy_model": {
            "custom_model": "my_model",
            # some config passed to the custom model's c'tor
            "custom_model_config": {},
            # Wrap custom model w/ attention.
            "use_attention": True,
            "output_layer_size": "action_space",
        },
        "value_function": {
            # Wrap default model w/ attention.
            "use_attention": True,
            "output_layer_size": 1,
        },
    }
}

# DQN (Dueling).
config = {
    "models": {
        "core": {
            # [some config or a custom model]
        },
        "advantages_model": {
            "input_source": "core",
            "fcnet_hiddens": [128, 128],  # Advantages head.
            "output_layer_size": "action_space",
        },
        "state_value_model": {
            "input_source": "core",
            "fcnet_hiddens": [128, 128],  # State-value head.
            "output_layer_size": 1,
        },
    }
}
# Advantages: Get rid of Q-model class entirely. All the dueling-plumbing
# can now be transparently done via thie config. Users can still plug in
# their custom model (pieces) if required.
# DQN can transparently check for `advantages_model` and `state_value_model`
# to be present and use these for its loss/action calculations.

# DQN (w/ Atari frame-stacking):
config = {
    # Same as above (DQN), but with slightly different "core" setup.
    "models": {
        "core": {
            # [some config or a custom model]
            # Specify frame-stacking details.
            "input_requirements": {
                "obs": {
                    "shift": [-3, -2, -1, 0],  # or "-3:0"
                },
            },
        },
        # `advantages_model` and `state_value_model` stay the same!
    }
}

# SAC (simple 1D vector input, image- or any other complex observations).
config = {
    "models": {
        "core": None,
        # The following already exist in the current SAC implementation.
        # What's confusing in this implementation is that the actual `model`
        # config key is ignored (often, users ask why)!
        "policy_model": {
            "output_layer_size": "action_space",
        },
        "Q_network": {
            #TODO: open question: In the SAC code right now, we
            #  set `input_space` to Tuple(obs-space, action-space).
            "input_requirements": {"obs", "actions"},

            "output_layer_size": 1,
        },
    }
}


# Thinking about getting rid of default preprocessors: What about
# complex observation spaces (e.g. Tuple(Box((84, 84, 3)), Discrete(2))
# Right now, the space seen by the policy will be the flattened one.
# SimpleQ w/ complex observation space (no more default preprocessors!).
config = {
    "models": {
        # Single policy model with standard action head.
        "policy_model": {
            # This indicates that the input directly comes from the env.
            # The env has the Tuple-space, so the default model used
            # here should be able to handle it (which it already does via our
            # ComplexInput default models, which simply flattens everything plus
            # handles images correctly with a default Conv stack).
            "input_source": None,
            "output_layer_size": "action_space",
        },
    },
}


# Specifying global models (shared across all policies):
# Multi-agent communication channels:

# Assuming we have 3 agents, ag1 and ag2 are using pol1 and ag3 is using pol2.
# Each agent should take an action according to:
# comm_output = multi_agent_comm_channel([obs_ag1, obs_ag2, obs_ag3])
# action_ag_x = [agent-specific-policy-model](comm_output)
obs_space = action_space = gym.spaces.Discrete(2)
config = {
    "multiagent": {
        "policies": {
            "pol1": (None, obs_space, action_space, {"models": {
                # This is the shared model. It sits inside that policy, that
                # would be responsible for updating it with its optimizer.
                "multi_agent_comm_channel": {
                    # Specify the multi-agent comm. channel's config here.

                    # Make sure all agents' observations are passed in
                    # (not just agent ag1 or ag2).
                    "input_requirements": {
                        "obs1": {
                            "agents": ["ag1"],
                        },
                    },
                },
                "policy_model": {
                    # Specify that we want our input to be coming from the
                    # shared channel.
                    "input_source": "multi_agent_comm_channel",
                },
            }}),
            "pol2": (None, obs_space, action_space, {"models": {
                "policy_model": {
                    # Specify that we want our input to be coming from the
                    # shared channel.
                    "input_source": "pol1.multi_agent_comm_channel",
                },
            }}),
        },
        "policy_mapping_fn": lambda agent_id, **kwargs: f"pol{agent_id}",
    },
}

# Model-based RL: Transition dynamics model (e.g. VAE):
# Ideal: Use (planned) event API to hook into model creation fn,
#        in which we'll add the VAE model to the policy's models registry
#        (dict).
#        Then do another hook into the exec. plan to execute the VAE update
#        from batches sampled by the rollout workers.

# Curiosity (ICM) module:
# See Model-based RL above.





# Algo default model building fn:
def make_model(policy, observation_space, action_space, config):
    #TODO: complete this default building/testing logic.
    #  which would also be used then for action computations and in
    #  loss functions?

    # New model building API.
    if config.get("models") is not None:
        policy.models = {}
        for model_name, model_config in config["models"].items():
            policy.models[model_name] = ModelCatalog.get_model_v2(
                obs_space=observation_space,
                action_space=action_space,
                framework=config["framework"],
                model_config=model_config,
            )

        # Test the entire model pipeline.
        test_outputs = {None: torch.from_numpy(np.array([observation_space.sample()]))}
        while len(test_outputs) < len(policy.models):
            for model_name, model in policy.models.items():
                # Skip if we already have this model's test output.
                if model_name in test_outputs:
                    continue
                in_source = config["models"][model_name].get("input_source")
                # Input is known already -> generate test output.
                if in_source in test_outputs:
                    out, _ = model({
                        "obs": test_outputs[in_source],
                    })
                    assert out.shape[0] == 1

    # ModelV2 API.
    else:
        dist_class, logit_dim = ModelCatalog.get_action_dist(
            action_space, config["model"], framework=config["framework"])
        policy.model = ModelCatalog.get_model_v2(
            obs_space=observation_space,
            action_space=action_space,
            num_outputs=logit_dim,
            model_config=config["model"],
            framework=config["framework"])

