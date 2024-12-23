import abc
from typing import Dict

from ray.rllib.utils.typing import TensorType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class QNetAPI(abc.ABC):
    """An API to be implemented by RLModules used for (distributional) Q-learning.

    RLModules implementing this API must override the `compute_q_values` and the
    `compute_advantage_distribution` methods.
    """

    @abc.abstractmethod
    def compute_q_values(
        self,
        batch: Dict[str, TensorType],
    ) -> Dict[str, TensorType]:
        """Computes Q-values, given encoder, q-net and (optionally), advantage net.

        Note, these can be accompanied by logits and probabilities
        in case of distributional Q-learning, i.e. `self.num_atoms > 1`.

        Args:
            batch: The batch received in the forward pass.

        Results:
            A dictionary containing the Q-value predictions ("qf_preds")
            and in case of distributional Q-learning - in addition to the Q-value
            predictions ("qf_preds") - the support atoms ("atoms"), the Q-logits
            ("qf_logits"), and the probabilities ("qf_probs").
        """

    def compute_advantage_distribution(
        self,
        batch: Dict[str, TensorType],
    ) -> Dict[str, TensorType]:
        """Computes the advantage distribution.

        Note this distribution is identical to the Q-distribution in case no dueling
        architecture is used.

        Args:
            batch: A dictionary containing a tensor with the outputs of the
                forward pass of the Q-head or advantage stream head.

        Returns:
            A `dict` containing the support of the discrete distribution for
            either Q-values or advantages (in case of a dueling architecture),
            ("atoms"), the logits per action and atom and the probabilities
            of the discrete distribution (per action and atom of the support).
        """
        # Return the Q-distribution by default.
        return self.compute_q_values(batch)
