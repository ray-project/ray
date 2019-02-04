from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import namedtuple
import logging

from ray.rllib.evaluation.sample_batch import MultiAgentBatch

logger = logging.getLogger(__name__)

OffPolicyEstimate = namedtuple("OffPolicyEstimate",
                               ["estimator_name", "metrics"])


class OffPolicyEstimator(object):
    """Interface for an off policy reward estimator (experimental).

    TODO(ekl): implement model-based estimators from literature, e.g., doubly
    robust, MAGIC. This will require adding some way of training a model
    (we should probably piggyback this on RLlib model API).
    """

    def __init__(self, ioctx):
        self.ioctx = ioctx
        self.gamma = ioctx.evaluator.policy_config["gamma"]
        keys = list(ioctx.evaluator.policy_map.keys())
        if len(keys) > 1:
            logger.warning(
                "Offline estimation is not implemented for multi-agent")
            self.policy = None
        else:
            self.policy = ioctx.evaluator.get_policy(keys[0])
        self.estimates = []

    def process(self, batch):
        """Process a new batch of experiences.

        The batch will only contain data from one episode, but it may only be
        a fragment of an episode.
        """
        raise NotImplementedError

    def can_estimate_for(self, batch):
        """Returns whether we can support OPE for this batch."""

        if isinstance(batch, MultiAgentBatch):
            logger.warning(
                "IS-estimation is not implemented for multi-agent batches")
            return False

        if "action_prob" not in batch:
            logger.warning(
                "Off-policy estimation is not possible unless the inputs "
                "include action probabilities (i.e., the 'action_prob' key).")
            return False
        return True

    def action_prob(self, batch):
        """Returns the probs for the batch actions for the current policy."""

        num_state_inputs = 0
        for k in batch.keys():
            if k.startswith("state_in_"):
                num_state_inputs += 1
        state_keys = ["state_in_{}".format(i) for i in range(num_state_inputs)]
        _, _, info = self.policy.compute_actions(
            obs_batch=batch["obs"],
            state_batches=[batch[k] for k in state_keys],
            prev_action_batch=batch.data.get("prev_action"),
            prev_reward_batch=batch.data.get("prev_reward"),
            info_batch=batch.data.get("info"))
        if "action_prob" not in info:
            raise ValueError(
                "Off-policy estimation is not possible unless the policy "
                "returns action probabilities when computing actions (i.e., "
                "the 'action_prob' key is defined).")
        return info["action_prob"]

    def get_metrics(self):
        """Return a list of new episode metric estimates since the last call.

        Returns:
            list of RolloutMetrics objects.
        """
        out = self.estimates
        self.estimates = []
        return out


class ImportanceSamplingEstimator(OffPolicyEstimator):
    """The step-wise IS estimator.

    Step-wise IS estimator described in https://arxiv.org/pdf/1511.03722.pdf"""

    def __init__(self, ioctx):
        OffPolicyEstimator.__init__(self, ioctx)

    def process(self, batch):
        if not self.can_estimate_for(batch):
            return

        rewards, old_prob = batch["rewards"], batch["action_prob"]
        new_prob = self.action_prob(batch)

        # calculate importance ratios
        p = []
        for t in range(batch.count - 1):
            if t == 0:
                pt_prev = 1.0
            else:
                pt_prev = p[t - 1]
            p.append(pt_prev * new_prob[t] / old_prob[t])

        # calculate stepwise IS estimate
        V_prev, V_step_IS = 0.0, 0.0
        for t in range(batch.count - 1):
            V_prev += rewards[t] * self.gamma**t
            V_step_IS += p[t] * rewards[t] * self.gamma**t

        estimation = OffPolicyEstimate(
            "is", {
                "V_prev": V_prev,
                "V_step_IS": V_step_IS,
                "V_gain_est": V_step_IS / V_prev,
            })
        self.estimates.append(estimation)


class WeightedImportanceSamplingEstimator(OffPolicyEstimator):
    """The weighted step-wise IS estimator.

    Step-wise WIS estimator in https://arxiv.org/pdf/1511.03722.pdf"""

    def __init__(self, ioctx):
        OffPolicyEstimator.__init__(self, ioctx)
        # TODO(ekl) consider synchronizing these as MeanStdFilter. This is a
        # bit tricky since we don't know the max episode length here.
        self.filter_values = []
        self.filter_counts = []

    def process(self, batch):
        if not self.can_estimate_for(batch):
            return

        rewards, old_prob = batch["rewards"], batch["action_prob"]
        new_prob = self.action_prob(batch)

        # calculate importance ratios
        p = []
        for t in range(batch.count - 1):
            if t == 0:
                pt_prev = 1.0
            else:
                pt_prev = p[t - 1]
            p.append(pt_prev * new_prob[t] / old_prob[t])
        for t, v in enumerate(p):
            if t >= len(self.filter_values):
                self.filter_values.append(v)
                self.filter_counts.append(1.0)
            else:
                self.filter_values[t] += v
                self.filter_counts[t] += 1.0

        # calculate stepwise IS estimate
        V_prev, V_step_WIS = 0.0, 0.0
        for t in range(batch.count - 1):
            V_prev += rewards[t] * self.gamma**t
            w_t = self.filter_values[t] / self.filter_counts[t]
            V_step_WIS += p[t] / w_t * rewards[t] * self.gamma**t

        estimation = OffPolicyEstimate(
            "wis", {
                "V_prev": V_prev,
                "V_step_WIS": V_step_WIS,
                "V_gain_est": V_step_WIS / V_prev,
            })
        self.estimates.append(estimation)
