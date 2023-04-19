"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf
"""
from typing import Optional

import numpy as np
import tensorflow as tf

from models.components.mlp import MLP
from models.components.representation_layer import RepresentationLayer


class DynamicsPredictor(tf.keras.Model):
    def __init__(
        self,
        *,
        model_dimension: Optional[str] = "XS",
        num_categoricals: Optional[int] = None,
        num_classes_per_categorical: Optional[int] = None,
    ):
        super().__init__()

        self.mlp = MLP(
            # TODO: In Danijar's code, the Dynamics Net only has a single layer, no
            #  matter the model size.
            num_dense_layers=1,
            model_dimension=model_dimension,
            output_layer_size=None,
        )
        self.representation_layer = RepresentationLayer(
            model_dimension=model_dimension,
            num_categoricals=num_categoricals,
            num_classes_per_categorical=num_classes_per_categorical,
        )

    def call(self, h, return_z_probs=False):
        """

        Args:
            h: The deterministic hidden state of the sequence model.
            return_z_probs: Whether to return the probabilities for the categorical
                distribution (in the shape of [B, num_categoricals, num_classes])
                as a second return value.
        """
        # Send internal state through MLP.
        out = self.mlp(h)
        # Generate a z vector (stochastic, discrete sample).
        return self.representation_layer(out, return_z_probs=return_z_probs)


if __name__ == "__main__":
    # DreamerV2/3 Atari input space: B x 32 (num_categoricals) x 32 (num_classes)
    h_dim = 8
    inputs = np.random.random(size=(1, 8))
    model = DynamicsPredictor()
    out = model(inputs)
    print(out.shape)

