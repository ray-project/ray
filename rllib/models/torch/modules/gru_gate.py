from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class GRUGate(nn.Module):
    # change from tf implementation: needs input_shape as a parameter
    def __init__(self, input_shape, init_bias=0., **kwargs):
        super().__init__(**kwargs)
        self._init_bias = init_bias

        h_shape, x_shape = input_shape
        if x_shape[-1] != h_shape[-1]:
            raise ValueError(
                "Both inputs to GRUGate must have equal size in last axis!")

        dim = int(h_shape[-1])
        self._w_r = torch.zeros(dim, dim)
        self._w_z = torch.zeros(dim, dim)
        self._w_h = torch.zeros(dim, dim)

        self._u_r = torch.zeros(dim, dim)
        self._u_z = torch.zeros(dim, dim)
        self._u_h = torch.zeros(dim, dim)

        nn.init.xavier_uniform(self._w_r)
        nn.init.xavier_uniform(self._w_z)
        nn.init.xavier_uniform(self._w_h)

        nn.init.xavier_uniform(self._u_r)
        nn.init.xavier_uniform(self._u_z)
        nn.init.xavier_uniform(self._u_h)

        #TODO what dtype should we be using
        dtype = torch.float32
        self._init_bias = self._init_bias(dtype)
        self._bias_z = torch.empty((dim, ), self._init_bias)


#        self._bias_z = bias_initializer(shape=(dim,),)

    def call(self, inputs, **kwargs):
        # Pass in internal state first.
        h, X = inputs

        #TODO check if this is the same as multiplication along axis=1
        r = torch.einsum("ij...,jk...->ik...", X, self._w_r) + \
            torch.einsum("ij...,jk...->ik...", h, self._u_r)
        #        r = tf.tensordot(X, self._w_r, axes=1) + \
        #            tf.tensordot(h, self._u_r, axes=1)
        r = nn.Sigmoid(r)

        z = torch.einsum("ij...,jk...->ik...", X, self._w_z) + \
            torch.einsum("ij...,jk...->ik...", h, self._u_z)

        #        z = tf.tensordot(X, self._w_z, axes=1) + \
        #            tf.tensordot(h, self._u_z, axes=1) - self._bias_z
        z = nn.Sigmoid(z)

        h_next = torch.einsum("ij...,jk...->ik...", X, self._w_h) + \
            torch.einsum("ij...,jk...->ik...", h, self._u_h)

        #        h_next = tf.tensordot(X, self._w_h, axes=1) + \
        #            tf.tensordot((h * r), self._u_h, axes=1)
        h_next = nn.Tanh(h_next)

        return (1 - z) * h + z * h_next
