"""Examples for RecSim envs ready to be used by RLlib Trainers

RecSim is a configurable recommender systems simulation platform.
Source: https://github.com/google-research/recsim
"""

import gymnasium as gym
from recsim import choice_model
from recsim.environments import (
    long_term_satisfaction as lts,
    interest_evolution as iev,
    interest_exploration as iex,
)

from ray.rllib.env.wrappers.recsim import make_recsim_env

# Some built-in RecSim envs to test with.
# ---------------------------------------

# Long-term satisfaction env: User has to pick from items that are either
# a) unhealthy, but taste good, or b) healthy, but have bad taste.
# Best strategy is to pick a mix of both to ensure long-term
# engagement.


def lts_user_model_creator(env_ctx):
    return lts.LTSUserModel(
        env_ctx["slate_size"],
        user_state_ctor=lts.LTSUserState,
        response_model_ctor=lts.LTSResponse,
    )


def lts_document_sampler_creator(env_ctx):
    return lts.LTSDocumentSampler()


LongTermSatisfactionRecSimEnv = make_recsim_env(
    recsim_user_model_creator=lts_user_model_creator,
    recsim_document_sampler_creator=lts_document_sampler_creator,
    reward_aggregator=lts.clicked_engagement_reward,
)


# Interest exploration env: Models the problem of active exploration
# of user interests. It is meant to illustrate popularity bias in
# recommender systems, where myopic maximization of engagement leads
# to bias towards documents that have wider appeal,
# whereas niche user interests remain unexplored.
def iex_user_model_creator(env_ctx):
    return iex.IEUserModel(
        env_ctx["slate_size"],
        user_state_ctor=iex.IEUserState,
        response_model_ctor=iex.IEResponse,
        seed=env_ctx["seed"],
    )


def iex_document_sampler_creator(env_ctx):
    return iex.IETopicDocumentSampler(seed=env_ctx["seed"])


InterestExplorationRecSimEnv = make_recsim_env(
    recsim_user_model_creator=iex_user_model_creator,
    recsim_document_sampler_creator=iex_document_sampler_creator,
    reward_aggregator=iex.total_clicks_reward,
)


# Interest evolution env: See https://github.com/google-research/recsim
# for more information.
def iev_user_model_creator(env_ctx):
    return iev.IEvUserModel(
        env_ctx["slate_size"],
        choice_model_ctor=choice_model.MultinomialProportionalChoiceModel,
        response_model_ctor=iev.IEvResponse,
        user_state_ctor=iev.IEvUserState,
        seed=env_ctx["seed"],
    )


# Extend IEvVideo to fix a bug caused by None cluster_ids.
class SingleClusterIEvVideo(iev.IEvVideo):
    def __init__(self, doc_id, features, video_length=None, quality=None):
        super(SingleClusterIEvVideo, self).__init__(
            doc_id=doc_id,
            features=features,
            cluster_id=0,  # single cluster.
            video_length=video_length,
            quality=quality,
        )


def iev_document_sampler_creator(env_ctx):
    return iev.UtilityModelVideoSampler(doc_ctor=iev.IEvVideo, seed=env_ctx["seed"])


InterestEvolutionRecSimEnv = make_recsim_env(
    recsim_user_model_creator=iev_user_model_creator,
    recsim_document_sampler_creator=iev_document_sampler_creator,
    reward_aggregator=iev.clicked_watchtime_reward,
)


# Backward compatibility.
gym.register(
    name="RecSim-v1", env_creator=lambda env_ctx: InterestEvolutionRecSimEnv(env_ctx)
)
