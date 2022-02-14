"""Examples for recommender system simulating envs ready to be used by RLlib Trainers
"""
import gym
import numpy as np
from typing import List, Optional

from ray.rllib.utils.numpy import softmax


class RecommSys001(gym.Env):
    def __init__(self,
                 num_categories: int,
                 num_docs_to_select_from: int,
                 slate_size: int,
                 num_docs_in_db: Optional[int] = None,
                 num_users_in_db: Optional[int] = None,
                 user_time_budget: float = 60.0,
                 ):
        """Initializes a RecommSys001 instance.

        Args:
            num_categories: Number of topics a user could be interested in and a
                document may be classified with. This is the embedding size for
                both users and docs. Each category in the user/doc embeddings
                can have values between 0.0 and 1.0.
            num_docs_to_select_from: The number of documents to present to the agent
                each timestep. The agent will then have to pick a slate out of these.
            slate_size: The size of the slate to recommend to the user at each
                timestep.
            num_docs_in_db: The total number of documents in the DB. Set this to None,
                in case you would like to resample docs from an infinite pool.
            num_users_in_db: The total number of users in the DB. Set this to None,
                in case you would like to resample users from an infinite pool.
            user_time_budget: The total time budget a user has throughout an episode.
                Once this time budget is used up (through engagements with
                clicked/selected documents), the episode ends.
        """
        self.num_categories = num_categories
        self.num_docs_to_select_from = num_docs_to_select_from
        self.slate_size = slate_size

        self.num_docs_in_db = num_docs_in_db
        self.docs_db = None
        self.num_users_in_db = num_users_in_db
        self.users_db = None

        self.user_time_budget = user_time_budget

        self.observation_space = gym.spaces.Dict({
            # The D docs our agent sees at each timestep. It has to select a k-slate
            # out of these.
            "doc": gym.spaces.Dict({
                str(idx): gym.spaces.Box(0.0, 1.0, shape=(self.num_categories,), dtype=np.float32) for idx in range(self.num_docs_to_select_from)
            }),
            # The user engaging in this timestep/episode.
            "user": gym.spaces.Box(0.0, 1.0, shape=(self.num_categories,), dtype=np.float32),
            # For each item in the previous slate, was it clicked? If yes, how
            # long was it being engaged with (e.g. watched)?
            "response": gym.spaces.Tuple([
                gym.spaces.Dict({
                    # Clicked or not?
                    "click": gym.spaces.Discrete(2),
                    # Engagement time (how many minutes watched?).
                    "engagement": gym.spaces.Box(0.0, 100.0, shape=(), dtype=np.float32),
                }) for _ in range(self.slate_size)
            ]),
        })
        # Our action space is
        self.action_space = gym.spaces.MultiDiscrete([
            self.num_docs_to_select_from for _ in range(self.slate_size)
        ])

    def reset(self):
        # Reset the current user's time budget.
        self.current_user_budget = self.user_time_budget

        # Sample a user for the next episode/session.
        # Pick from a only-once-sampled user DB.
        if self.num_users_in_db is not None:
            if self.users_db is None:
                self.users_db = [softmax(np.random.normal(scale=2.0, size=(self.num_categories,))) for _ in range(self.num_users_in_db)]
            self.current_user = self.users_db[np.random.choice(self.num_users_in_db)]
        # Pick from an infinite pool of users.
        else:
            self.current_user = softmax(np.random.normal(scale=2.0, size=(self.num_categories,)))

        return self._get_obs()

    def step(self, action):
        # Action is the suggested slate (indices of the docs in the suggested ones).

        # User choice model: User picks a doc stochastically,
        # where probs are dot products between user- and doc feature
        # (categories) vectors. There is also a no-click doc for which all features
        # are 0.5.
        user_doc_overlaps = [
            np.dot(self.current_user, self.currently_suggested_docs[str(doc_idx)])
            for doc_idx in action
        ] + [np.dot(self.current_user, np.array([1.0 / self.num_categories for _ in range(self.num_categories)]))]

        which_clicked = np.random.choice(
            np.arange(self.slate_size + 1),
            p=softmax(np.array(user_doc_overlaps) * 10.0),  # TODO explain why *x -> lower temperature of distribution
        )

        # Reward is the overlap, if clicked. -1.0 if nothing clicked.
        reward = -1.0

        # If anything clicked, deduct from the current user's time budget and compute
        # reward.
        if which_clicked < self.slate_size:
            reward = user_doc_overlaps[which_clicked]
            self.current_user_budget -= 1.0
        done = self.current_user_budget <= 0.0

        # Compile response.
        response = tuple({
            "click": int(idx == which_clicked),
            "engagement": reward if idx == which_clicked else 0.0,
        } for idx in range(len(user_doc_overlaps) - 1))

        return self._get_obs(response=response), reward, done, {}

    def _get_obs(self, response=None):
        # Sample D docs from infinity or our pre-existing docs.
        # Pick from a only-once-sampled docs DB.
        if self.num_docs_in_db is not None:
            if self.docs_db is None:
                self.docs_db = [softmax(np.random.normal(scale=2.0, size=(self.num_categories,))) for _ in range(self.num_docs_in_db)]
            self.currently_suggested_docs = {
                str(i): self.docs_db[doc_idx].astype(np.float32) for i, doc_idx in enumerate(np.random.choice(self.num_docs_in_db, size=(self.num_docs_to_select_from,), replace=False))
            }
        # Pick from an infinite pool of docs.
        else:
            self.currently_suggested_docs = {
                str(idx): softmax(np.random.normal(scale=2.0, size=(self.num_categories,))).astype(np.float32) for idx in range(self.num_docs_to_select_from)
            }

        return {
            "user": self.current_user.astype(np.float32),
            "doc": self.currently_suggested_docs,
            "response": response if response else self.observation_space["response"].sample()
        }


def get_smart_action(obs):
    user = obs["user"]
    docs = obs["doc"]
    response = obs["response"]
    slate_size = len(response)

    scores = [0.0 for _ in range(len(docs))]
    for i, doc in docs.items():
        scores[int(i)] = np.dot(doc, user)
    best_doc_idx = np.argmax(scores)
    action = [best_doc_idx] + [np.random.randint(0, len(docs)) for _ in range(slate_size - 1)]
    np.random.shuffle(action)
    return np.array(action)


if __name__ == "__main__":
    env = RecommSys001(
        num_categories=5,
        num_docs_to_select_from=50,
        slate_size=2,
        num_docs_in_db=1000,
        num_users_in_db=1000,
    )
    obs = env.reset()
    num_episodes = 0
    episode_rewards = []
    episode_reward = 0.0

    while num_episodes < 200:
        #action = get_smart_action(obs)
        action = env.action_space.sample()
        obs, reward, done, _ = env.step(action)

        episode_reward += reward
        if done:
            #print(f"episode reward = {episode_reward}")
            obs = env.reset()
            num_episodes += 1
            episode_rewards.append(episode_reward)
            episode_reward = 0.0

    print(f"Avg reward={np.mean(episode_rewards)}")
