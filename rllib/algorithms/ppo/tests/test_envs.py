import gymnasium as gym
import math
import numpy as np
import tree
from typing import List, Optional
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.typing import (
    TensorStructType,
    ActionConnectorDataType,
    AgentConnectorDataType,
    AgentConnectorsOutput,
)


class OneActionZeroObsOneRewardEnv(gym.Env):
    """
    Isolates the value network.

    If agent cannot learn that the value of the only observation
    it observes is 1.0 there must be a problem with the value loss
    calculation or the optimization.

    Assert that value ~ 1.0.
    Set `horizon=1`, `vf_share_layers=False', `vf_clip_param`=float("inf").
    """

    def __init__(self, config: dict):
        self.observation_space = gym.spaces.Box(-1.0, 1.0, (1,), dtype=np.float32)
        self.action_space = gym.spaces.Box(0.0, 0.0, (), dtype=np.float32)
        self.check_states = [np.array([0.0], dtype=np.float32)]
        self.theoretical_value = 1.0

    def reset(self, *, seed: int = None, options: dict = None):
        state = np.array([0.0])
        return state, {"cost": 0.0}

    def step(self, action: np.ndarray):
        state = np.array([0.0])
        reward = 1.0
        terminated = True
        truncated = False

        return state, reward, terminated, truncated, {"cost": 0.0}

    def assertion(
        self,
        value,
        cost: Optional[float] = None,
        action: Optional[np.ndarray] = None,
        obs: Optional[np.ndarray] = None,
        abs_tol: Optional[float] = None,
        rel_tol: Optional[float] = 0.01,
    ):
        assert math.isclose(self.theoretical_value, value, rel_tol=rel_tol), (
            f"Value should be around {self.theoretical_value} but is {value}. "
            "There might be a problem with your algorithm's value loss calculation "
            "or optimizer."
        )


class OneActionRandomObsTwoRewardEnv(gym.Env):
    """
    Isolates the value network.

    If the agent can learn the value in the `OneActionZeroObsOneRewardEnv`
    environment, but not in this one, i.e. it can learn
    a constant reward, but not a predictable one, backpropagation must
    be broken.

    Assert value > 0 for obs > 0, value < 0 for obs < 0.
    Set `horizon=1`, `vf_share_layers=False`, `vf_clip_param`=float("inf").
    """

    def __init__(self, config: dict):
        # This has to be a 1-dimensional space as RLlib's Encoder cannot
        # be defined for a zero-dimensional space.
        self.observation_space = gym.spaces.Box(-1.0, 1.0, (1,), dtype=np.float32)
        self.action_space = gym.spaces.Box(0.0, 0.0, (), dtype=np.float32)
        self.check_states = [
            np.array([-1.0], dtype=np.float32),
            np.array([1.0], dtype=np.float32),
        ]
        self.theoretical_value = [-1.0, 1.0]

    def reset(self, *, seed: int = None, options: dict = None):
        state = np.random.choice([-1.0, 1.0], (1,))
        self.reward = 1.0 if state > 0.0 else -1.0

        return state, {"cost": 0.0}

    def step(self, action: np.ndarray):
        # TODO: Check, if important that you sample a new state and reward.
        state = np.random.choice([-1.0, 1.0], (1,))
        # reward = 1.0 if state > 0.0 else -1.0
        terminated = True
        truncated = False

        return state, self.reward, terminated, truncated, {"cost": 0.0}

    def assertion(
        self,
        value: Optional[float] = None,
        cost: Optional[float] = None,
        action: Optional[np.ndarray] = None,
        obs: Optional[np.ndarray] = None,
        abs_tol: Optional[float] = None,
        rel_tol: Optional[float] = 0.01,
    ):
        theoretical_value = self.theoretical_value[self.check_states.index(obs)]
        assert math.isclose(theoretical_value, value, rel_tol=rel_tol), (
            f"Value for obs {obs} should be around {np.sign(obs)}, but is {value}. "
            f"There might be a problem with reward discounting."
        )


class OneActionTwoObsOneRewardEnv(gym.Env):
    """
    Isolates reward discounting.

    If an agent can learn the value of the `OneActionZeroObsOneRewardEnv`
    and the one of the `OneActionRandomObsTwoRewardEnv`, but not this
    one, the discounting must be broken.

    Set `horizon=2`, set `vf_share_layers=False`.
    """

    def __init__(self, config: Optional[dict] = None):
        self.observation_space = gym.spaces.Box(0.0, 1.0, (1,), dtype=np.float32)
        self.action_space = gym.spaces.Box(0.0, 0.0, (1,), dtype=np.float32)
        self.count: int = 0
        self.check_states = [
            np.array([0.0], dtype=np.float32),
            np.array([1.0], dtype=np.float32),
        ]
        self.theoretical_value = [config.get("gamma", 0.99), 1.0]

    def reset(self, *, seed: int = None, options: dict = None):
        self.count: int = 0
        state = np.array([0.0], dtype=np.float32)

        return state, {"cost": 0.0}

    def step(self, action: np.ndarray):
        self.count += 1
        state = np.array([1.0], dtype=np.float32)
        reward = 0.0
        terminated = False
        truncated = False
        if self.count == 2:
            terminated = True
            reward = 1.0
        return state, reward, terminated, truncated, {"cost": 0.0}

    def assertion(
        self,
        value: Optional[float] = None,
        cost: Optional[float] = None,
        action: Optional[np.ndarray] = None,
        obs: Optional[np.ndarray] = None,
        abs_tol: Optional[float] = None,
        rel_tol: Optional[float] = 0.01,
    ):
        theoretical_value = self.theoretical_value[self.check_states.index(obs)]
        print(f" - Obs: {obs}")
        print(f" - Theoretical value: {theoretical_value}")
        print(f" - Observed value: {value}")
        assert math.isclose(theoretical_value, value, rel_tol=rel_tol), (
            f"Value for obs {obs} should be around {theoretical_value}, but is "
            f"{value}. "
            f"There might be a problem with reward discounting."
        )


class TwoActionOneObsTwoRewardEnv(gym.Env):
    """
    Tests the policy network.

    If the agent cannot learn to pick the better action(s),
    advantage calculation, policy loss, or policy update
    must be off.

    Assert action > 0.0.
    Set `vf_share_layers=False`, `vf_clip_param=float("inf")`, and
    `hoirzon=1`.
    """

    def __init__(self, config: Optional[dict]):
        self.observation_space = gym.spaces.Box(
            1.0, 1.0, shape=(1,), dtype=np.float32
        )  # gym.spaces.Discrete(1)
        self.action_space = gym.spaces.Box(-1.0, 1.0, (), dtype=np.float32)
        self.check_states = [np.array([1.0], dtype=np.float32)]
        self.theoretical_value = [1.0]
        self.theoretical_actions = [1.0]

    def reset(self, *, seed: int = None, options: dict = None):
        state = np.array([1.0], dtype=np.float32)

        return state, {"cost": 0.0}

    def step(self, action):
        state = np.array([1.0], dtype=np.float32)
        terminated = True
        truncated = False
        reward = 1.0 if action >= 0.0 else -1.0
        return state, reward, terminated, truncated, {"cost": 0.0}

    def assertion(
        self,
        value: Optional[float] = None,
        cost: Optional[float] = None,
        action: Optional[np.ndarray] = None,
        obs: Optional[np.ndarray] = None,
        abs_tol: Optional[float] = None,
        rel_tol: Optional[float] = 0.01,
    ):
        # TODO: Calculate theortical values for advantage, policy loss and
        # and policy update.
        if action:
            assert action > 0.0, (
                f"Action should be > 0.0 but is {action}. There might be a problem "
                f"with "
                f"either the algorithm's advantage calculation, the policy loss, "
                f"or the policy update."
            )
        if value:
            theoretical_value = self.theoretical_value[self.check_states.index(obs)]
            assert math.isclose(theoretical_value, value, rel_tol=rel_tol), (
                f"Value for obs {obs} should be around {theoretical_value}, but is "
                f"{value}. "
                f"There might be a problem with backpropagation in your "
                f"value network."
            )


class TwoActionRandomObsTwoRewardEnv(gym.Env):
    """
    Tests interdependence between policy and value network.

    If an agent learns the values and actions in the environments
    before, but not this one, there might be something wrong with the
    interdepence between policy and value network. If, e.g. the value
    network fails to learn here, we might feed the value network stale
    experiences in batches.

    Assert value == +1.0 for both states, action > 0.0 if state == 1,
    action < 0.0 if state == 0.
    """

    def __init__(self, config):
        self.observation_space = gym.spaces.Discrete(2)
        self.action_space = gym.spaces.Box(-1.0, 1.0, (), dtype=np.float32)
        self.check_states = [0, 1]
        self.theoretical_value = 1.0
        self.theoretical_actions = [-1.0, 1.0]

    def reset(self, *, seed: int = None, options: dict = None):
        self.state = self.observation_space.sample()

        return self.state, {}

    def step(self, action: np.ndarray):
        state = self.observation_space.sample()
        reward = (
            1.0
            if (action > 0.0 and self.state == 1) or (action < 0.0 and self.state == 0)
            else -1.0
        )
        terminated = True
        truncated = False

        return state, reward, terminated, truncated, {"cost": 0.0}

    def assertion(
        self,
        value: Optional[float] = None,
        cost: Optional[float] = None,
        action: Optional[np.ndarray] = None,
        obs: Optional[np.ndarray] = None,
        abs_tol: Optional[float] = 0.2,
        rel_tol: Optional[float] = None,
    ):
        theoretical_action = self.theoretical_actions[self.check_states.index(obs)]
        obs = -1.0 if obs == 0 else obs
        assert (action * obs) > 0.0, (
            f"Action should be around {theoretical_action} for state {obs}, but is "
            f"{action}. "
            f"There might be something wrong with your algorithm's interdependence "
            f"between policy and value network."
        )
        assert math.isclose(value, self.theoretical_value, abs_tol=abs_tol), (
            f"Value should be {self.theoretical_value} for both states but is {value}. "
            "There might be some stale experiences in batches."
        )


def local_policy_inference(
    policy: Policy,
    env_id: str,
    agent_id: str,
    obs: TensorStructType,
    reward: Optional[float],
    explore: bool = None,
    timestep: Optional[int] = None,
) -> TensorStructType:
    assert (
        policy.agent_connectors
    ), "`policy_inference` only works with connector enabled policies."

    policy.agent_connectors.in_eval()
    policy.action_connectors.in_eval()

    input_dict = {SampleBatch.NEXT_OBS: obs}
    # The following two keys are important for the default
    # view requirements of the policies.
    if reward is not None:
        input_dict[SampleBatch.REWARDS] = reward
    if agent_id is not None:
        input_dict[SampleBatch.AGENT_INDEX] = agent_id

    acd_list: List[AgentConnectorDataType] = [
        AgentConnectorDataType(env_id, agent_id, input_dict)
    ]
    ac_outputs: List[AgentConnectorsOutput] = policy.agent_connectors(acd_list)
    outputs = []
    for ac in ac_outputs:
        policy_output = policy.compute_actions_from_input_dict(
            ac.data.sample_batch,
            explore=explore,
            timestep=timestep,
        )

        policy_output = tree.map_structure(lambda x: x[0], policy_output)

        action_connector_data = ActionConnectorDataType(
            env_id,
            agent_id,
            ac.data.raw_dict,
            policy_output,
        )

        if policy.action_connectors:
            acd = policy.action_connectors(action_connector_data)
            actions = acd.output
        else:
            actions = policy_output[0]

        outputs.append(actions)

        policy.agent_connectors.on_policy_output(action_connector_data)

    return outputs
