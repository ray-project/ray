import numpy as np
import tree  # pip install dm_tree

from ray.rllib.core.models.base import STATE_IN
from ray.rllib.core.models.specs.specs_dict import SpecDict
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.test_utils import check

_, tf, _ = try_import_tf()
torch, _ = try_import_torch()


class ModelChecker:
    def __init__(self, config):
        self.config = config

        # To compare number of params between frameworks.
        self.param_counts = {}
        # To compare computed outputs from fixed-weights-nets between frameworks.
        self.output_values = {}

        # We will pass an observation filled with this one random value through
        # all DL networks (after they have been set to fixed-weights) to compare
        # the computed outputs.
        self.random_fill_input_value = np.random.uniform(-0.1, 0.1)

        # Dict of models to check against each other.
        self.models = {}

    def add(self, framework: str = "torch"):
        model = self.models[framework] = self.config.build(framework=framework)

        # Pass a B=1 observation through the model.
        if isinstance(model.input_specs, SpecDict):
            inputs = {}
            for key, spec in model.input_specs.items():
                key = key[0]
                if spec is not None:
                    inputs[key] = spec.fill(self.random_fill_input_value)
                else:
                    inputs[key] = None
        else:
            inputs = model.input_specs.fill(self.random_fill_input_value)

        outputs = model(inputs)
        # Bring model into a reproducible, comparable state (so we can compare
        # computations across frameworks). Use only a value-sequence of len=1 here
        # as it could possibly be that the layers are stored in different order
        # across the different frameworks.
        model._set_to_dummy_weights(value_sequence=(self.random_fill_input_value,))
        # And do another forward pass.
        comparable_outputs = model(inputs)

        # Store the number of parameters for this framework's net.
        self.param_counts[framework] = model.get_num_parameters()
        # Store the fixed-weights-net outputs for this framework's net.
        if framework == "tf2":
            self.output_values[framework] = tree.map_structure(
                lambda s: s.numpy() if s is not None else None, comparable_outputs
            )
        else:
            self.output_values[framework] = tree.map_structure(
                lambda s: s.detach().numpy() if s is not None else None,
                comparable_outputs,
            )

        return outputs

    def check(self):
        main_key = next(iter(self.models.keys()))
        # Compare number of trainable and non-trainable params between all
        # frameworks.
        for c in self.param_counts.values():
            check(c, self.param_counts[main_key])
        # Compare dummy outputs by exact values given that all nets received the
        # same input and all nets have the same (dummy) weight values.
        for v in self.output_values.values():
            check(v, self.output_values[main_key], rtol=0.001)
