from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
import logging
from ray.rllib.evaluation.metrics import get_learner_stats
from ray.rllib.optimizers.policy_optimizer import PolicyOptimizer
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.filter import RunningStat
from ray.rllib.utils.timer import TimerStat
from ray.rllib.utils.sgd import standardized
import numpy as np
import scipy

logger = logging.getLogger(__name__)

class MAMLOptimizer(PolicyOptimizer):
	""" MAML Optimizer: Workers are different tasks while 
	Every time MAML Optimizer steps...
	1) Workers are set to the same weights as master...
	2) Tasks are randomly sampled and assigned to each worker...
	3) Inner Adaptation Steps
		-Workers collect their own data, update themselves, and collect more data...
		-All data from all workers from all steps gets aggregated to all_samples
	4) Using the aggregated data, update the meta-objective
	"""

	def __init__(self, workers, config):
		PolicyOptimizer.__init__(self, workers)

		# Each worker represents a different task
		self.baseline = LinearFeatureBaseline()
		self.discount = config["gamma"]
		self.gae_lambda = config["lambda"]
		self.num_tasks = len(self.workers.remote_workers())
		self.update_weights_timer = TimerStat()
		self.set_tasks_timer = TimerStat()
		self.sample_timer = TimerStat()
		self.meta_grad_timer = TimerStat()
		self.inner_adaptation_steps = config["inner_adaptation_steps"]
		self.learner_stats = {}
		self.maml_optimizer_steps = config["maml_optimizer_steps"]
		self.config = config

	@override(PolicyOptimizer)
	def step(self):

		# Initialize Workers to have the same weights
		print("Start of Optimizer Loop: Setting Weights")
		with self.update_weights_timer:
			if self.workers.remote_workers():
				weights = ray.put(self.workers.local_worker().get_weights())
				for e in self.workers.remote_workers():
					e.set_weights.remote(weights)

		# Set Tasks for each Worker
		print("Setting Tasks for each Worker")
		with self.set_tasks_timer:
			env_configs = self.workers.local_worker().sample_tasks(self.num_tasks)
			ray.get([e.set_task.remote(env_configs[i]) for i,e in enumerate(self.workers.remote_workers())])

		# Collecting Data from Pre and Post Adaptations
		print("Sampling Data")
		with self.sample_timer:
			meta_split = []

			# Pre Adaptation Sampling from Workers
			samples = ray.get([e.sample.remote(dataset_id="0") for i,e in enumerate(self.workers.remote_workers())])
			samples = self.post_processing(samples)
			#for sample in samples:
				#sample['advantages'] = standardized(sample['advantages'])
			all_samples = SampleBatch.concat_samples(samples)
			meta_split.append([sample['obs'].shape[0] for sample in samples])

			# Data Collection for Meta-Update Step (which will be done on Master Learner)
			for step in range(self.inner_adaptation_steps):
				# Inner Adaptation Gradient Steps
				print("Inner Adaptation")
				for i, e in enumerate(self.workers.remote_workers()):
					e.learn_on_batch.remote(samples[i])
				
				samples = ray.get([e.sample.remote(dataset_id=str(step+1)) for e in self.workers.remote_workers()])
				samples = self.post_processing(samples)
				#for sample in samples:
					#sample['advantages'] = standardized(sample['advantages'])
				all_samples = all_samples.concat(SampleBatch.concat_samples(samples))
				meta_split.append([sample['obs'].shape[0] for sample in samples])

		# Meta gradient Update
		# All Samples should be a list of list of dicts where the dims are (inner_adaptation_steps+1,num_workers,SamplesDict)
		print("Meta Update")
		with self.meta_grad_timer:
			all_samples["split"] = np.array(meta_split)
			for i in range(self.maml_optimizer_steps):
				fetches = self.workers.local_worker().learn_on_batch(all_samples)
			self.learner_stats = get_learner_stats(fetches)

		self.num_steps_sampled += all_samples.count
		self.num_steps_trained += all_samples.count

		return self.learner_stats


	def post_processing(self, samples):
		for sample in samples:
			indexes = np.where(sample['dones']==True)[0]
			indexes = indexes+1

			reward_list = np.split(sample['rewards'], indexes)[:-1]
			observation_list = np.split(sample['obs'], indexes)[:-1]

			temp_list = []
			for i in range(0, len(reward_list)):
				temp_list.append({"rewards": reward_list[i], "observations": observation_list[i]})
			
			advantages = self._compute_samples_data(temp_list)
			sample["advantages"] = advantages
		return samples

	def _compute_samples_data(self, paths):
		assert type(paths) == list

		# 1) compute discounted rewards (returns)
		for idx, path in enumerate(paths):
			path["returns"] = self.discount_cumsum(path["rewards"], self.discount)

		# 2) fit baseline estimator using the path returns and predict the return baselines
		self.baseline.fit(paths, target_key="returns")
		all_path_baselines = [self.baseline.predict(path) for path in paths]

		# 3) compute advantages and adjusted rewards
		paths = self._compute_advantages(paths, all_path_baselines)

		# 4) stack path data
		advantages = np.concatenate([path["advantages"] for path in paths])

		advantages = self.normalize_advantages(advantages)
		return advantages

	def _compute_advantages(self, paths, all_path_baselines):
		assert len(paths) == len(all_path_baselines)

		for idx, path in enumerate(paths):
			path_baselines = np.append(all_path_baselines[idx], 0)
			deltas = path["rewards"] + \
					 self.discount * path_baselines[1:] - \
					 path_baselines[:-1]
			path["advantages"] = self.discount_cumsum(
				deltas, self.discount * self.gae_lambda)

		return paths

	def discount_cumsum(self, x, discount):
		"""
		See https://docs.scipy.org/doc/scipy/reference/tutorial/signal.html#difference-equation-filtering
		Returns:
			(float) : y[t] - discount*y[t+1] = x[t] or rev(y)[t] - discount*rev(y)[t-1] = rev(x)[t]
		"""
		return scipy.signal.lfilter([1], [1, float(-discount)], x[::-1], axis=0)[::-1]

	def normalize_advantages(self, advantages):
		"""
		Args:
			advantages (np.ndarray): np array with the advantages
		Returns:
			(np.ndarray): np array with the advantages normalized
		"""
		return (advantages - np.mean(advantages)) / (advantages.std() + 1e-8)

class LinearBaseline():
    """
    Abstract class providing the functionality for fitting a linear baseline
    Don't instantiate this class. Instead use LinearFeatureBaseline or LinearTimeBaseline
    """

    def __init__(self, reg_coeff=1e-5):
        super(LinearBaseline, self).__init__()
        self._coeffs = None
        self._reg_coeff = reg_coeff

    def predict(self, path):
        """
        Abstract Class for the LinearFeatureBaseline and the LinearTimeBaseline
        Predicts the linear reward baselines estimates for a provided trajectory / path.
        If the baseline is not fitted - returns zero baseline
        Args:
           path (dict): dict of lists/numpy array containing trajectory / path information
                 such as "observations", "rewards", ...
        Returns:
             (np.ndarray): numpy array of the same length as paths["observations"] specifying the reward baseline
        """
        if self._coeffs is None:
            return np.zeros(len(path["observations"]))
        return self._features(path).dot(self._coeffs)

    def get_param_values(self, **tags):
        """
        Returns the parameter values of the baseline object
        Returns:
            numpy array of linear_regression coefficients
        """
        return self._coeffs

    def set_params(self, value, **tags):
        """
        Sets the parameter values of the baseline object
        Args:
            value: numpy array of linear_regression coefficients
        """
        self._coeffs = value

    def fit(self, paths, target_key='returns'):
        """
        Fits the linear baseline model with the provided paths via damped least squares
        Args:
            paths (list): list of paths
            target_key (str): path dictionary key of the target that shall be fitted (e.g. "returns")
        """
        assert all([target_key in path.keys() for path in paths])

        featmat = np.concatenate([self._features(path) for path in paths], axis=0)
        target = np.concatenate([path[target_key] for path in paths], axis=0)
        reg_coeff = self._reg_coeff
        for _ in range(5):
            self._coeffs = np.linalg.lstsq(
                featmat.T.dot(featmat) + reg_coeff * np.identity(featmat.shape[1]),
                featmat.T.dot(target),
                rcond=-1
            )[0]
            if not np.any(np.isnan(self._coeffs)):
                break
            reg_coeff *= 10

    def _features(self, path):
        raise NotImplementedError("this is an abstract class, use either LinearFeatureBaseline or LinearTimeBaseline")


class LinearFeatureBaseline(LinearBaseline):
    """
    Linear (polynomial) time-state dependent return baseline model
    (see. Duan et al. 2016, "Benchmarking Deep Reinforcement Learning for Continuous Control", ICML)
    Fits the following linear model
    reward = b0 + b1*obs + b2*obs^2 + b3*t + b4*t^2+  b5*t^3
    Args:
        reg_coeff: list of paths
    """
    def __init__(self, reg_coeff=1e-5):
        super(LinearFeatureBaseline, self).__init__()
        self._coeffs = None
        self._reg_coeff = reg_coeff

    def _features(self, path):
        obs = np.clip(path["observations"], -10, 10)
        path_length = len(path["observations"])
        time_step = np.arange(path_length).reshape(-1, 1) / 100.0
        return np.concatenate([obs, obs ** 2, time_step, time_step ** 2, time_step ** 3, np.ones((path_length, 1))],
                              axis=1)


class LinearTimeBaseline(LinearBaseline):
    """
    Linear (polynomial) time-dependent reward baseline model
    Fits the following linear model
    reward = b0 + b3*t + b4*t^2+  b5*t^3
    Args:
        reg_coeff: list of paths
    """

    def _features(self, path):
        path_length = len(path["observations"])
        time_step = np.arange(path_length).reshape(-1, 1) / 100.0
        return np.concatenate([time_step, time_step ** 2, time_step ** 3, np.ones((path_length, 1))],
                              axis=1)