from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class Stride2D(nn.Module):
    """A striding layer for doing torch Conv2DTranspose operations.

    Using this layer before the 0-padding (on a 3D input "image") and before
    the actual ConvTranspose2d allows for a padding="same" behavior that matches
    100% that of a `tf.keras.layers.Conv2DTranspose` layer.

    Examples:
        Input image (4x4):
        A B C D
        E F G H
        I J K L
        M N O P

        Stride with stride=2 -> output image=(7x7)
        A 0 B 0 C 0 D
        0 0 0 0 0 0 0
        E 0 F 0 G 0 H
        0 0 0 0 0 0 0
        I 0 J 0 K 0 L
        0 0 0 0 0 0 0
        M 0 N 0 O 0 P
    """

    def __init__(self, width, height, stride_w, stride_h):
        """Initializes a Stride2D instance.

        Args:
            width: The width of the 3D input "image".
            height: The height of the 3D input "image".
            stride_w: The stride in width direction, with which to stride the incoming
                image.
            stride_h: The stride in height direction, with which to stride the incoming
                image.
        """
        super().__init__()

        self.width = width
        self.height = height
        self.stride_w = stride_w
        self.stride_h = stride_h

        self.register_buffer(
            "zeros",
            torch.zeros(
                size=(
                    self.width * self.stride_w - (self.stride_w - 1),
                    self.height * self.stride_h - (self.stride_h - 1),
                ),
                dtype=torch.float32,
            ),
        )

        self.out_width, self.out_height = self.zeros.shape[0], self.zeros.shape[1]
        # Squeeze in batch and channel dims.
        self.zeros = self.zeros.unsqueeze(0).unsqueeze(0)

        where_template = torch.zeros(
            (self.stride_w, self.stride_h), dtype=torch.float32
        )
        # Set upper/left corner to 1.0.
        where_template[0][0] = 1.0
        # then tile across the entire (strided) image size.
        where_template = where_template.repeat((self.height, self.width))[
            : -(self.stride_w - 1), : -(self.stride_h - 1)
        ]
        # Squeeze in batch and channel dims and convert to bool.
        where_template = where_template.unsqueeze(0).unsqueeze(0).bool()
        self.register_buffer("where_template", where_template)

    def forward(self, x):
        # Repeat incoming image stride(w/h) times to match the strided output template.
        repeated_x = (
            x.repeat_interleave(self.stride_w, dim=-2).repeat_interleave(
                self.stride_h, dim=-1
            )
        )[:, :, : -(self.stride_w - 1), : -(self.stride_h - 1)]
        # Where `self.where_template` == 1.0 -> Use image pixel, otherwise use
        # zero filler value.
        return torch.where(self.where_template, repeated_x, self.zeros)
