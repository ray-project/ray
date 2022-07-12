from collections import namedtuple
import logging
from ray.rllib.policy.sample_batch import MultiAgentBatch, SampleBatch
from ray.rllib.policy import Policy
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.offline.io_context import IOContext
from ray.rllib.utils.annotations import Deprecated
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.typing import TensorType, SampleBatchType
from typing import List

logger = logging.getLogger(__name__)

OffPolicyEstimate = DeveloperAPI(
    namedtuple("OffPolicyEstimate", ["estimator_name", "metrics"])
)


@DeveloperAPI
class OffPolicyEstimator:
    """Interface for an off policy reward estimator."""

    @DeveloperAPI
    def __init__(self, name: str, policy: Policy, gamma: float):
        """Initializes an OffPolicyEstimator instance.

        Args:
            name: string to save OPE results under
            policy: Policy to evaluate.
            gamma: Discount factor of the environment.
        """
        self.name = name
        self.policy = policy
        self.gamma = gamma
        self.new_estimates = []

    @DeveloperAPI
    def estimate(self, batch: SampleBatchType) -> List[OffPolicyEstimate]:
        """Returns a list of off policy estimates for the given batch of episodes.

        Args:
            batch: The batch to calculate the off policy estimates (OPE) on.

        Returns:
            The off-policy estimates (OPE) calculated on the given batch.
        """
        raise NotImplementedError

    @DeveloperAPI
    def train(self, batch: SampleBatchType) -> TensorType:
        """Trains an Off-Policy Estimator on a batch of experiences.
        A model-based estimator should override this and train
        a transition, value, or reward model.

        Args:
            batch: The batch to train the model on

        Returns:
            any optional training/loss metrics from the model
        """
        pass

    @DeveloperAPI
    def action_log_likelihood(self, batch: SampleBatchType) -> TensorType:
        """Returns log likelihood for actions in given batch for policy.

        Computes likelihoods by passing the observations through the current
        policy's `compute_log_likelihoods()` method

        Args:
            batch: The SampleBatch or MultiAgentBatch to calculate action
                log likelihoods from. This batch/batches must contain OBS
                and ACTIONS keys.

        Returns:
            The probabilities of the actions in the batch, given the
            observations and the policy.
        """
        num_state_inputs = 0
        for k in batch.keys():
            if k.startswith("state_in_"):
                num_state_inputs += 1
        state_keys = ["state_in_{}".format(i) for i in range(num_state_inputs)]
        log_likelihoods: TensorType = self.policy.compute_log_likelihoods(
            actions=batch[SampleBatch.ACTIONS],
            obs_batch=batch[SampleBatch.OBS],
            state_batches=[batch[k] for k in state_keys],
            prev_action_batch=batch.get(SampleBatch.PREV_ACTIONS),
            prev_reward_batch=batch.get(SampleBatch.PREV_REWARDS),
            actions_normalized=True,
        )
        log_likelihoods = convert_to_numpy(log_likelihoods)
        return log_likelihoods

    @DeveloperAPI
    def check_can_estimate_for(self, batch: SampleBatchType) -> None:
        """Checks if we support off policy estimation (OPE) on given batch.

        Args:
            batch: The batch to check.

        Raises:
            ValueError: In case `action_prob` key is not in batch OR batch
            is a MultiAgentBatch.
        """

        if isinstance(batch, MultiAgentBatch):
            raise ValueError(
                "Off-Policy Estimation is not implemented for multi-agent batches. "
                "You can set `off_policy_estimation_methods: {}` to resolve this."
            )

        if "action_prob" not in batch:
            raise ValueError(
                "Off-policy estimation is not possible unless the inputs "
                "include action probabilities (i.e., the policy is stochastic "
                "and emits the 'action_prob' key). For DQN this means using "
                "`exploration_config: {type: 'SoftQ'}`. You can also set "
                "`off_policy_estimation_methods: {}` to disable estimation."
            )

    @DeveloperAPI
    def process(self, batch: SampleBatchType) -> None:
        """Computes off policy estimates (OPE) on batch and stores results.
        Thus-far collected results can be retrieved then by calling
        `self.get_metrics` (which flushes the internal results storage).
        Args:
            batch: The batch to process (call `self.estimate()` on) and
                store results (OPEs) for.
        """
        self.new_estimates.extend(self.estimate(batch))

    @DeveloperAPI
    def get_metrics(self, get_losses: bool = False) -> List[OffPolicyEstimate]:
        """Returns list of new episode metric estimates since the last call.

        Args:
            get_losses: If True, also return self.losses for the OPE estimator
        Returns:
            out: List of OffPolicyEstimate objects.
            losses: List of training losses for the estimator.
        """
        out = self.new_estimates
        self.new_estimates = []
        if hasattr(self, "losses"):
            losses = self.losses
            self.losses = []
            if get_losses:
                return out, losses
        return out

    # TODO (rohan): Remove deprecated methods; set to error=True because changing
    # from one episode per SampleBatch to full SampleBatch is a breaking change anyway

    @Deprecated(help="OffPolicyEstimator.__init__(policy, gamma, config)", error=False)
    @classmethod
    @DeveloperAPI
    def create_from_io_context(cls, ioctx: IOContext) -> "OffPolicyEstimator":
        """Creates an off-policy estimator from an IOContext object.
        Extracts Policy and gamma (discount factor) information from the
        IOContext.
        Args:
            ioctx: The IOContext object to create the OffPolicyEstimator
                from.
        Returns:
            The OffPolicyEstimator object created from the IOContext object.
        """
        gamma = ioctx.worker.policy_config["gamma"]
        # Grab a reference to the current model
        keys = list(ioctx.worker.policy_map.keys())
        if len(keys) > 1:
            raise NotImplementedError(
                "Off-policy estimation is not implemented for multi-agent. "
                "You can set `input_evaluation: []` to resolve this."
            )
        policy = ioctx.worker.get_policy(keys[0])
        config = ioctx.input_config.get("estimator_config", {})
        return cls(policy, gamma, config)

    @Deprecated(new="OffPolicyEstimator.create_from_io_context", error=True)
    @DeveloperAPI
    def create(self, *args, **kwargs):
        return self.create_from_io_context(*args, **kwargs)

    @Deprecated(new="OffPolicyEstimator.compute_log_likelihoods", error=False)
    @DeveloperAPI
    def action_prob(self, *args, **kwargs):
        return self.compute_log_likelihoods(*args, **kwargs)
