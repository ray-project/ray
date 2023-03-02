from ray.rllib.utils import try_import_torch
from torch.nn import functional as F

torch, nn = try_import_torch()


def batchify_states(states_list, batch_size, device=None):
    """
    Batchify data into batches of size batch_size
    """
    state_batches = [s[None, :].expand(batch_size, -1) for s in states_list]
    if device is not None:
        state_batches = [s.to(device) for s in state_batches]

    return state_batches


def symlog(x, alpha=1.0):
    return torch.sign(x) * torch.log(1 + alpha * torch.abs(x)) / alpha


def symexp(x, alpha=1.0):
    return torch.sign(x) * (torch.exp(alpha * torch.abs(x)) - 1) / alpha


class FreezeParameters:
    def __init__(self, parameters):
        self.parameters = parameters
        self.param_states = [p.requires_grad for p in self.parameters]

    def __enter__(self):
        for param in self.parameters:
            param.requires_grad = False

    def __exit__(self, exc_type, exc_val, exc_tb):
        for i, param in enumerate(self.parameters):
            param.requires_grad = self.param_states[i]


class TwoHotEncoder:
    def __init__(
            self,
            min=-20,
            max=20,
            num_bins=255,
            device='cpu'
    ):
        self.num_bins = num_bins
        self.bins = torch.arange(min, max, (max - min) / num_bins).to(device=device)
        self.device = device
        self.denominator = self.bins[1] - self.bins[0]

    def encode(self, xs):
        xs = xs.squeeze(1)
        k = torch.bucketize(xs, self.bins)

        lower = torch.abs((self.bins[k - 1] - xs)).reshape(-1, 1) * F.one_hot(k, self.num_bins).to(self.device)
        upper = torch.abs((self.bins[k] - xs)).reshape(-1, 1) * F.one_hot(k - 1, self.num_bins).to(self.device)
        result = (lower + upper) / self.denominator

        return result
