import numpy as np

from ray.rllib.policy.policy import Policy
from ray.rllib.policy.torch_policy import TorchPolicy
from ray.rllib.algorithms.leela_chess_zero.mcts import Node, RootParentNode
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.metrics.learner_info import LEARNER_STATS_KEY


torch, _ = try_import_torch()


class LeelaChessZeroPolicy(TorchPolicy):
    def __init__(
        self,
        observation_space,
        action_space,
        config,
        model,
        loss,
        action_distribution_class,
        mcts_creator,
        env_creator,
        **kwargs
    ):
        super().__init__(
            observation_space,
            action_space,
            config,
            model=model,
            loss=loss,
            action_distribution_class=action_distribution_class,
        )
        # we maintain an env copy in the policy that is used during mcts
        # simulations
        self.env_creator = env_creator
        self.mcts = mcts_creator()
        self.env = self.env_creator()
        self.obs_space = observation_space
        # only used in multi policy competitive environments
        self.elo = 400

    @override(TorchPolicy)
    def compute_actions(
        self,
        obs_batch,
        state_batches=None,
        prev_action_batch=None,
        prev_reward_batch=None,
        info_batch=None,
        episodes=None,
        **kwargs
    ):
        input_dict = {"obs": obs_batch}
        if prev_action_batch is not None:
            input_dict["prev_actions"] = prev_action_batch
        if prev_reward_batch is not None:
            input_dict["prev_rewards"] = prev_reward_batch

        return self.compute_actions_from_input_dict(
            input_dict=input_dict,
            episodes=episodes,
            state_batches=state_batches,
        )

    @override(Policy)
    def compute_actions_from_input_dict(
        self, input_dict, explore=None, timestep=None, episodes=None, **kwargs
    ):
        with torch.no_grad():
            actions = []
            for i, episode in enumerate(episodes):
                env_state = episode.user_data["current_state"][-1]
                # create tree root node
                obs = self.env.set_state(env_state)
                tree_node = Node(
                    state=env_state,
                    obs=obs,
                    reward=0,
                    done=False,
                    action=None,
                    parent=RootParentNode(env=self.env),
                    mcts=self.mcts,
                )

                # run monte carlo simulations to compute the actions
                # and record the tree
                mcts_policy, action, tree_node = self.mcts.compute_action(tree_node)

                # record action
                actions.append(action)
                # store new node
                episode.user_data["tree_node"] = tree_node

                # store mcts policies vectors and current tree root node
                if episode.length == 0:
                    episode.user_data["mcts_policies"] = [mcts_policy]
                else:
                    episode.user_data["mcts_policies"].append(mcts_policy)
                break
            return (
                np.array(actions),
                [],
                self.extra_action_out(
                    input_dict, kwargs.get("state_batches", []), self.model, None
                ),
            )

    @override(Policy)
    def postprocess_trajectory(
        self, sample_batch, other_agent_batches=None, episode=None
    ):
        # add mcts policies to sample batch
        sample_batch["mcts_policies"] = np.array(episode.user_data["mcts_policies"])[
            sample_batch["t"]
        ]
        # final episode reward corresponds to the value (if not discounted)
        # for all transitions in episode
        final_reward = sample_batch["rewards"][-1]
        sample_batch["value_label"] = final_reward * np.ones_like(sample_batch["t"])
        return sample_batch

    @override(TorchPolicy)
    def learn_on_batch(self, postprocessed_batch):
        train_batch = self._lazy_tensor_dict(postprocessed_batch)

        loss_out, policy_loss, value_loss = self._loss(
            self, self.model, self.dist_class, train_batch
        )
        self._optimizers[0].zero_grad()
        loss_out.backward()

        grad_process_info = self.extra_grad_process(self._optimizers[0], loss_out)
        self._optimizers[0].step()

        grad_info = self.extra_grad_info(train_batch)
        grad_info.update(grad_process_info)
        grad_info.update(
            {
                "total_loss": loss_out.detach().cpu().numpy(),
                "policy_loss": policy_loss.detach().cpu().numpy(),
                "value_loss": value_loss.detach().cpu().numpy(),
            }
        )

        return {LEARNER_STATS_KEY: grad_info}
