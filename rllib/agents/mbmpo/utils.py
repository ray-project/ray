import numpy as np

from ray.rllib.evaluation.postprocessing import discount_cumsum
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.utils.exploration.stochastic_sampling import StochasticSampling
from ray.rllib.utils.framework import try_import_tf, try_import_torch

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()


class LinearFeatureBaseline:
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
            [o, o ** 2, al, al ** 2, al ** 3, np.ones((ll, 1))], axis=1
        )

    def fit(self, paths):
        featmat = np.concatenate([self._features(path) for path in paths])
        returns = np.concatenate([path["returns"] for path in paths])
        reg_coeff = self._reg_coeff
        for _ in range(5):
            self._coeffs = np.linalg.lstsq(
                featmat.T.dot(featmat) + reg_coeff * np.identity(featmat.shape[1]),
                featmat.T.dot(returns),
            )[0]
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
        deltas = path["rewards"] + discount * path_baselines[1:] - path_baselines[:-1]
        path["advantages"] = discount_cumsum(deltas, discount * gae_lambda)
    return paths


class MBMPOExploration(StochasticSampling):
    """Like StochasticSampling, but only worker=0 uses Random for n timesteps."""

    def __init__(
        self,
        action_space,
        *,
        framework: str,
        model: ModelV2,
        random_timesteps: int = 8000,
        **kwargs
    ):
        """Initializes a MBMPOExploration instance.

        Args:
            action_space (Space): The gym action space used by the environment.
            framework (str): One of None, "tf", "torch".
            model (ModelV2): The ModelV2 used by the owning Policy.
            random_timesteps (int): The number of timesteps for which to act
                completely randomly. Only after this number of timesteps,
                actual samples will be drawn to get exploration actions.
                NOTE: For MB-MPO, only worker=0 will use this setting. All
                other workers will not use random actions ever.
        """
        super().__init__(
            action_space,
            model=model,
            framework=framework,
            random_timesteps=random_timesteps,
            **kwargs
        )

        assert (
            self.framework == "torch"
        ), "MBMPOExploration currently only supports torch!"

        # Switch off Random sampling for all non-driver workers.
        if self.worker_index > 0:
            self.random_timesteps = 0
