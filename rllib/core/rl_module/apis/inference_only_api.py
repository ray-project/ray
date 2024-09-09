import abc
from typing import List


class InferenceOnlyAPI(abc.ABC):
    """An API to be implemented by RLModules that have an inference-only mode.

    Only the `get_non_inference_attributes` method needs to get implemented for
    a RLModule to have the following functionality:
    - On EnvRunners (or when self.config.inference_only=True), RLlib will remove
    those parts of the model not required for action computation.
    - An RLModule on a Learner (where `self.config.inference_only=False`) will return
    only those weights from `get_state()` that are part of its inference-only version,
    thus possibly saving network traffic/time.
    """

    @abc.abstractmethod
    def get_non_inference_attributes(self) -> List[str]:
        """Returns a list of names (str) of attributes of inference-only components.

        The `inference_only` mode is activated by setting `inference_only` to True
        in any RLModule's `RLModuleSpec`.

        For example:

        .. testcode::
            :skipif: True

            from ray.rllib.core.rl_module.rl_module import RLModuleSpec

            spec = RLModuleSpec(module_class=..., inference_only=True)

        If an RLModule has the following `setup()` implementation:

        .. testcode::
            :skipif: True

            def setup(self):
                self._policy_head = [some NN component]
                self._value_function_head = [some NN component]

                self._encoder = [some NN component with attributes
                                 `pol` (policy encoder) and `vf` (value func encoder)]

        Then its `get_non_inference_attributes()` should return:
        `["_value_function_head", "_encoder.vf"]`

        Note the "." notation to separate attributes and their sub-attributes in case
        you need more fine-grained control over which sub-attributes to exclude in an
        inference-only setup.

        Returns:
            A list of names (str) of those attributes (or sub-attributes) that should be
            excluded (deleted) from this RLModule in case it's setup in
            `inference_only` mode.
        """
