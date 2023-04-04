from typing import Mapping, Any

import abc
from ray.rllib.core.rl_module.rl_module import ModuleID
from ray.rllib.core.learner.learner import Learner
from ray.rllib.utils.annotations import override


class PPOBaseLearner(Learner):
    def build(self) -> None:
        super().build()

        # TODO (Kourosh): Move these failures to config.validate() or support them.
        self.entropy_coeff_scheduler = None
        if self.hps.entropy_coeff_schedule:
            raise ValueError("entropy_coeff_schedule is not supported in Learner yet")

        # TODO (Kourosh): This needs to be native tensor variable to be traced.
        self.entropy_coeff = self.hps.entropy_coeff

        # TODO (Kourosh): Create a way on the base class for users to define arbitrary
        # schedulers for learning rates.
        self.lr_scheduler = None
        if self.hps.lr_schedule:
            raise ValueError("lr_schedule is not supported in Learner yet")

        # TODO (Kourosh): We can still use mix-ins in the new design. Do we want that?
        # Most likely not. I rather be specific about everything. kl_coeff is a
        # none-gradient based update which we can define here and add as update with
        # additional_update() method.

        # We need to make sure that the kl_coeff is a framework tensor that is
        # registered as part of the graph so that upon update the graph can be updated
        # (e.g. in TF with eager tracing)
        self.kl_coeff_val = self.hps.kl_coeff
        self.kl_coeff = self._create_kl_variable(self.hps.kl_coeff)

        self.kl_target = self.hps.kl_target

    @override(Learner)
    def additional_update_per_module(
        self, module_id: ModuleID, sampled_kl_values: dict, timestep: int
    ) -> Mapping[str, Any]:
        assert sampled_kl_values, "Sampled KL values are empty."

        sampled_kl = sampled_kl_values[module_id]
        if sampled_kl > 2.0 * self.kl_target:
            # TODO (Kourosh) why not 2?
            self.kl_coeff_val *= 1.5
        elif sampled_kl < 0.5 * self.kl_target:
            self.kl_coeff_val *= 0.5

        self._set_kl_coeff(self.kl_coeff_val)
        results = {"kl_coeff": self.kl_coeff_val}

        # TODO (Kourosh): We may want to index into the schedulers to get the right one
        # for this module
        if self.entropy_coeff_scheduler is not None:
            self.entropy_coeff_scheduler.update(timestep)

        if self.lr_scheduler is not None:
            self.lr_scheduler.update(timestep)

        return results

    @abc.abstractmethod
    def _create_kl_variable(self, value: float) -> Any:
        """Creates the kl_coeff tensor variable.

        This is a framework specific method that should be implemented by the
        framework specific sub-class.

        Args:
            value: The initial value for the kl_coeff variable.
        """

    @abc.abstractmethod
    def _set_kl_coeff(self, value: float) -> None:
        """Sets the value of the kl_coeff variable.

        This is a framework specific method that should be implemented by the
        framework specific sub-class.

        Args:
            value: The new value for the kl_coeff variable.
        """
