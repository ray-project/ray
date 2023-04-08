from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class Stride2D(nn.Module):
    """A striding layer for doing torch Conv2DTranspose operations."""

    def __init__(self, width, height, stride_w, stride_h):
        super().__init__()

        self.width = width
        self.height = height
        self.stride_w = stride_w
        self.stride_h = stride_h

        self.zeros = torch.zeros(
            size=(
                self.width * self.stride_w - (self.stride_w - 1),
                self.height * self.stride_h - (self.stride_h - 1),
            ),
            dtype=torch.float32,
        )
        self.out_width, self.out_height = self.zeros.shape[0], self.zeros.shape[1]
        # Squeeze in batch and channel dims.
        self.zeros = self.zeros.unsqueeze(0).unsqueeze(0)

        self.where_template = torch.zeros(
            (self.stride_w, self.stride_h), dtype=torch.float32
        )
        # Set upper/left corner to 1.0.
        self.where_template[0][0] = 1.0
        # then tile across the entire (strided) image size.
        self.where_template = self.where_template.repeat((self.height, self.width))[
            : -(self.stride_w - 1), : -(self.stride_h - 1)
        ]
        # Squeeze in batch and channel dims and convert to bool.
        self.where_template = self.where_template.unsqueeze(0).unsqueeze(0).bool()

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
