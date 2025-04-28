import abc
from typing import List

from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class InferenceOnlyAPI(abc.ABC):
    """An API to be implemented by RLModules that have an inference-only mode.

    Only the `get_non_inference_attributes` method needs to get implemented for
    an RLModule to have the following functionality:
    - On EnvRunners (or when self.inference_only=True), RLlib will remove
    those parts of the model not required for action computation.
    - An RLModule on a Learner (where `self.inference_only=False`) will
    return only those weights from `get_state()` that are part of its inference-only
    version, thus possibly saving network traffic/time.
    """

    @abc.abstractmethod
    def get_non_inference_attributes(self) -> List[str]:
        """Returns a list of attribute names (str) of components NOT used for inference.

        RLlib will use this information to remove those attributes/components from an
        RLModule, whose `config.inference_only` is set to True. This so-called
        "inference-only setup" is activated. Normally, all RLModules located on
        EnvRunners are constructed this way (because they are only used for computing
        actions). Similarly, when deployed into a production environment, users should
        consider building their RLModules with this flag set to True as well.

        For example:

        .. code-block:: python

            from ray.rllib.core.rl_module.rl_module import RLModuleSpec

            spec = RLModuleSpec(module_class=..., inference_only=True)

        If an RLModule has the following setup() implementation:

        .. testcode::
            :skipif: True

            class MyRLModule(RLModule):

                def setup(self):
                    self._policy_head = [some NN component]
                    self._value_function_head = [some NN component]

                    self._encoder = [some NN component with attributes: pol and vf
                                     (policy- and value func. encoder)]

        Then its `get_non_inference_attributes()` should return:
        ["_value_function_head", "_encoder.vf"].

        Note the "." notation to separate attributes and their sub-attributes in case
        you need more fine-grained control over which exact sub-attributes to exclude in
        the inference-only setup.

        Returns:
            A list of names (str) of those attributes (or sub-attributes) that should be
            excluded (deleted) from this RLModule in case it's setup in
            `inference_only` mode.
        """
