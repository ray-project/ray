import torch


class Summer(torch.nn.Module):
    def __init__(self, increment: int):
        self.increment = torch.tensor(increment)
        super().__init__()

    def forward(self, inp):
        inp = torch.tensor(inp)
        return inp.reshape(len(inp), -1).sum(axis=1) + self.increment


#
