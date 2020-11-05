import numpy as np
import scipy
from typing import Union

from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils.exploration.exploration import Exploration
from ray.rllib.utils.framework import try_import_tf, try_import_torch, \
    TensorType

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()


class LinearFeatureBaseline():
    def __init__(self, reg_coeff=1e-5):
        self._coeffs = None
        self._reg_coeff = reg_coeff

    def get_param_values(self, **tags):
        return self._coeffs

    def set_param_values(self, val, **tags):
        self._coeffs = val

    def _features(self, path):
        o = np.clip(path["observations"], -10, 10)
        ll = len(path["rewards"])
        al = np.arange(ll).reshape(-1, 1) / 100.0
        return np.concatenate(
            [o, o**2, al, al**2, al**3,
             np.ones((ll, 1))], axis=1)

    def fit(self, paths):
        featmat = np.concatenate([self._features(path) for path in paths])
        returns = np.concatenate([path["returns"] for path in paths])
        reg_coeff = self._reg_coeff
        for _ in range(5):
            self._coeffs = np.linalg.lstsq(
                featmat.T.dot(featmat) +
                reg_coeff * np.identity(featmat.shape[1]),
                featmat.T.dot(returns))[0]
            if not np.any(np.isnan(self._coeffs)):
                break
            reg_coeff *= 10

    def predict(self, path):
        if self._coeffs is None:
            return np.zeros(len(path["rewards"]))
        return self._features(path).dot(self._coeffs)


def calculate_gae_advantages(paths, discount, gae_lambda):
    baseline = LinearFeatureBaseline()

    for idx, path in enumerate(paths):
        path["returns"] = discount_cumsum(path["rewards"], discount)

    baseline.fit(paths)
    all_path_baselines = [baseline.predict(path) for path in paths]

    for idx, path in enumerate(paths):
        path_baselines = np.append(all_path_baselines[idx], 0)
        deltas = path["rewards"] + \
            discount * path_baselines[1:] - \
            path_baselines[:-1]
        path["advantages"] = discount_cumsum(deltas, discount * gae_lambda)
    return paths


def discount_cumsum(x, discount):
    """
        Returns:
            (float) : y[t] - discount*y[t+1] = x[t] or rev(y)[t]
            - discount*rev(y)[t-1] = rev(x)[t]
        """
    return scipy.signal.lfilter(
        [1], [1, float(-discount)], x[::-1], axis=0)[::-1]


class MBMPOExploration(Exploration):
    """An exploration that simply samples from a distribution.

    The sampling can be made deterministic by passing explore=False into
    the call to `get_exploration_action`.
    Also allows for scheduled parameters for the distributions, such as
    lowering stddev, temperature, etc.. over time.
    """

    def __init__(self, action_space, *, framework: str, model: ModelV2,
                 **kwargs):
        """Initializes a StochasticSampling Exploration object.

        Args:
            action_space (Space): The gym action space used by the environment.
            framework (str): One of None, "tf", "torch".
        """
        assert framework is not None
        self.timestep = 0
        self.worker_index = kwargs["worker_index"]
        super().__init__(
            action_space, model=model, framework=framework, **kwargs)

    @override(Exploration)
    def get_exploration_action(self,
                               *,
                               action_distribution: ActionDistribution,
                               timestep: Union[int, TensorType],
                               explore: bool = True):
        assert self.framework == "torch"
        return self._get_torch_exploration_action(action_distribution, explore)

    def _get_torch_exploration_action(self, action_dist, explore):
        action = action_dist.sample()
        logp = action_dist.sampled_action_logp()

        batch_size = action.size()[0]

        # Initial Random Exploration for Real Env Interaction
        if self.worker_index == 0 and self.timestep < 8000:
            print("Using Random")
            action = [self.action_space.sample() for _ in range(batch_size)]
            logp = [0.0 for _ in range(batch_size)]
        self.timestep += batch_size
        return action, logp
