from ray.rllib.algorithms.leela_chess_zero import LeelaChessZeroConfig
from ray.rllib.algorithms.leela_chess_zero.leela_chess_zero_model import LeelaChessZeroModel
from ray.rllib.examples.env.pettingzoo_chess import MultiAgentChess
from ray.rllib.policy.policy import PolicySpec
import numpy as np


p0 = LeelaChessZeroConfig().training(mcts_config={"num_simulations":20,"turn_based_flip":True,"argmax_tree_policy":True,"argmax_child_value":True}).environment(MultiAgentChess)
random = LeelaChessZeroConfig().training(mcts_config={"num_simulations":3,"epsilon":1}).environment(MultiAgentChess)


config = (
    LeelaChessZeroConfig()
    .environment(MultiAgentChess)
    .rollouts(num_rollout_workers=11)
    .resources(num_gpus=1,num_cpus_per_worker = 1,num_gpus_per_worker=0.0)
    .framework("torch")
    .multi_agent(
        # 2 agents per env.
        # p_0 represent a trainable policy that should get better with training
        # p_1 represents a policy which avoids and/or pursues most trivial checkmates in 1, but doesnt learn
        policies={"p_0": PolicySpec(config=p0)
        ,"p_1": PolicySpec(config = random)},
        # Train only the first policy.
        policies_to_train=["p_0"],

        policy_mapping_fn=(
            lambda aid, eps, worker, **kw: "p_"
            + str(
                "0"
                if int(aid.split("_")[-1]) % 2 == 0
                else "1"
            )
        ),
    )
)

#this stops when 
stop = {"policy_reward_mean/p_0": 0.5,
        "timesteps_total": 2000000,
        "time_total_s":180000}