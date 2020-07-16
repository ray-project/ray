import numpy as np

from ray.rllib.utils.framework import get_activation_fn, try_import_torch
#from ray.rllib.utils.framework import , get_variable, \
#    try_import_tf

torch, nn = try_import_torch()


class NoisyLayer(nn.Module):
    """A Layer that adds learnable Noise
    a common dense layer: y = w^{T}x + b
    a noisy layer: y = (w + \\epsilon_w*\\sigma_w)^{T}x +
        (b+\\epsilon_b*\\sigma_b)
    where \epsilon are random variables sampled from factorized normal
    distributions and \\sigma are trainable variables which are expected to
    vanish along the training procedure
    """

    def __init__(self, in_size, out_size, sigma0, activation="relu"):
        """Initializes a NoisyLayer object.

        Args:
            in_size:
            out_size:
            sigma0:
            non_linear:
        """
        super().__init__()

        self.in_size = in_size
        self.out_size = out_size
        self.sigma0 = sigma0
        self.activation = activation

        self.sigma_w = torch.Tensor(
            data=np.random.uniform(
                low=-1.0 / np.sqrt(float(self.in_size)),
                high=1.0 / np.sqrt(float(self.in_size)),
                size=[self.in_size, out_size]),
            dtype=torch.float32,
            requires_grad=True)
        # TF noise generation can be unreliable on GPU
        # If generating the noise on the CPU,
        # lowering sigma0 to 0.1 may be helpful
        self.sigma_b = torch.Tensor(
            data=np.full(
                shape=[out_size], fill_value=sigma0 / np.sqrt(float(self.in_size))),
            requires_grad=True)
        self.w = torch.Tensor(
            data=np.full(
                shape=[self.in_size, self.out_size],
                fill_value=6 / np.sqrt(float(in_size) + float(out_size))),
            requires_grad=True)
        self.b = torch.Tensor(data=np.zeros([out_size]), requires_grad=True)

    def forward(self, inputs):
        epsilon_in = self._f_epsilon(torch.normal(
            mean=torch.zeros([self.in_size]), std=torch.ones([self.in_size])))
        epsilon_out = self._f_epsilon(torch.normal(
            mean=torch.zeros([self.out_size]), std=torch.ones([self.out_size])))
        epsilon_w = torch.matmul(
            torch.unsqueeze(epsilon_in, -1),
            other=torch.unsqueeze(epsilon_out, 0))
        epsilon_b = epsilon_out

        action_activation = torch.matmul(
            inputs, self.w + self.sigma_w * epsilon_w
        ) + self.b + self.sigma_b * epsilon_b

        fn = get_activation_fn(self.activation, framework="torch")
        if fn is not None:
            action_activation = fn(action_activation)
        return action_activation

    def _f_epsilon(self, x):
        return torch.sign(x) * torch.pow(torch.abs(x), 0.5)
