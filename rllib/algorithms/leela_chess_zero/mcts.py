"""
Mcts implementation modified from
https://github.com/brilee/python_uct/blob/master/numpy_impl.py
"""
import collections
import math

import numpy as np
import copy


class Node:
    def __init__(
        self, action, obs, done, reward, state, mcts, parent=None, multi_agent=False
    ):
        self.env = parent.env
        self.action = action  # Action used to go to this state

        self.is_expanded = False
        self.parent = parent
        self.children = {}

        self.action_space_size = self.env.action_space.n
        self.child_total_value = np.zeros(
            [self.action_space_size], dtype=np.float32
        )  # Q
        self.child_priors = np.zeros([self.action_space_size], dtype=np.float32)  # P
        self.child_number_visits = np.zeros(
            [self.action_space_size], dtype=np.float32
        )  # N

        self.reward = reward
        self.done = done
        self.state = state
        self.obs = obs

        current_agents = list(obs.keys())
        current_agent = current_agents[0]
        for key in current_agents:
            if "player_" in key:
                multi_agent = True
        if multi_agent:
            current_agent = self.state.agent_selection
            if type(self.reward) == dict:
                self.reward = self.reward[current_agent]
            if type(self.done) == dict:
                self.done = self.done[current_agent]
            if type(self.obs) == dict:
                self.valid_actions = obs[current_agent]["action_mask"].astype(bool)
                self.obs = obs[current_agent]
        else:
            self.valid_actions = obs["action_mask"].astype(bool)
            self.obs = obs

        self.mcts = mcts

        self.multi_agent = multi_agent

    @property
    def number_visits(self):
        return self.parent.child_number_visits[self.action]

    @number_visits.setter
    def number_visits(self, value):
        self.parent.child_number_visits[self.action] = value

    @property
    def total_value(self):
        return self.parent.child_total_value[self.action]

    @total_value.setter
    def total_value(self, value):
        self.parent.child_total_value[self.action] = value

    def child_Q(self):
        # TODO (weak todo) add "softmax" version of the Q-value
        return self.child_total_value / (1 + self.child_number_visits)

    def child_U(self):
        return (
            math.sqrt(self.number_visits)
            * self.child_priors
            / (1 + self.child_number_visits)
        )

    def best_action(self):
        """
        :return: action
        """
        child_score = self.child_Q() + self.mcts.c_puct * self.child_U()
        masked_child_score = child_score
        if self.mcts.exploit_child_value:
            masked_child_score[~self.valid_actions] = -1e22
            action = np.argmax(masked_child_score)
            assert self.valid_actions[action] == 1
            return action
        else:
            masked_child_score[~self.valid_actions] = 0
            masked_child_score[self.valid_actions] += 1 + abs(
                np.min(masked_child_score)
            )
            p = masked_child_score / np.sum(masked_child_score)
            action = np.random.choice(
                np.arange(len(masked_child_score)),
                p=p,
            )
            assert self.valid_actions[action] == 1
            return action

    def select(self):
        current_node = self
        while current_node.is_expanded:
            best_action = current_node.best_action()
            current_node = current_node.get_child(best_action)
        return current_node

    def expand(self, child_priors):
        self.is_expanded = True
        self.total_value = 0
        self.parent.child_total_value[self.action] = 0
        self.child_priors = child_priors

    def get_child(self, action):
        if action not in self.children:
            self.env.set_state(self.state)
            obs, reward, done, _ = self.env.step(action)
            next_state = self.env.get_state()
            self.children[action] = Node(
                state=next_state,
                action=action,
                parent=self,
                reward=reward,
                done=done,
                obs=obs,
                mcts=self.mcts,
            )
        return self.children[action]

    def backup(self, value):
        current = self

        while current.parent is not None:
            if self.mcts.turn_based_flip:
                value = -value
            current.number_visits += 1
            current.total_value += value
            current = current.parent


class RootParentNode:
    def __init__(self, env, state=None):
        self.parent = None
        self.child_total_value = collections.defaultdict(float)
        self.child_number_visits = collections.defaultdict(float)
        self.env = env
        if state is None:
            self.state = env.get_state()
        else:
            self.state = state


class MCTS:
    def __init__(self, model, mcts_param):
        self.model = model
        self.temperature = mcts_param["temperature"]
        self.dir_epsilon = mcts_param["dirichlet_epsilon"]
        self.dir_noise = mcts_param["dirichlet_noise"]
        self.num_sims = mcts_param["num_simulations"]
        self.exploit = mcts_param["argmax_tree_policy"]
        self.add_dirichlet_noise = mcts_param["add_dirichlet_noise"]
        self.c_puct = mcts_param["puct_coefficient"]
        self.epsilon = mcts_param["epsilon"]
        self.turn_based_flip = mcts_param["turn_based_flip"]
        self.exploit_child_value = mcts_param["argmax_child_value"]

    def compute_action(self, node):
        initial_state = copy.deepcopy(node.state)
        for _ in range(self.num_sims):
            node.env.set_state(copy.deepcopy(initial_state))
            leaf = node.select()
            if leaf.done:
                value = -leaf.reward * 10
            else:
                child_priors, value = self.model.compute_priors_and_value(leaf.obs)
                if self.add_dirichlet_noise:
                    child_priors = (1 - self.dir_epsilon) * child_priors
                    child_priors += self.dir_epsilon * np.random.dirichlet(
                        [self.dir_noise] * child_priors.size
                    )

                leaf.expand(child_priors)
            leaf.backup(value)

        # Tree policy target (TPT)
        tree_policy = node.child_number_visits / node.number_visits
        tree_policy = tree_policy / np.max(
            tree_policy
        )  # to avoid overflows when computing softmax
        tree_policy = np.power(tree_policy, self.temperature)
        tree_policy *= node.valid_actions
        tree_policy = tree_policy / np.sum(tree_policy)
        epsilon_exploration = np.random.choice(
            [True, False], p=[self.epsilon, 1 - self.epsilon]
        )
        if self.exploit and not epsilon_exploration:
            # if exploit then choose action that has the maximum
            # tree policy probability
            action = np.argmax(tree_policy)
        else:
            # otherwise sample an action according to tree policy probabilities
            action = np.random.choice(np.arange(node.action_space_size), p=tree_policy)
        assert node.valid_actions[action] == 1
        node.env.set_state(initial_state)
        return tree_policy, action, node.children[action]
