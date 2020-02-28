import torch
import torch.nn as nn
import torch.nn.functional as F

class RoundIdGradient(torch.autograd.Function):
  @staticmethod
  def forward(ctx, x):
    return x.round()

  @staticmethod
  def backward(ctx, g):
    return g

from torch.nn import Module, ModuleList, Sequential
from torch.nn import Conv2d, BatchNorm2d, PixelShuffle
class Residual(Module):
  def __init__(self, channels, k_size=3, layers=3):
    super().__init__()

    self.convs = ModuleList([
      Conv2d(channels, channels, (k_size, k_size), padding=(k_size-1)//2)
      for i in range(layers)])
    self.bns = ModuleList([
      BatchNorm2d(channels)
      for i in range(layers)])

  def forward(self, x):
    identity = x
    for i in range(len(self.convs)):
      x = self.bns[i](x)
      x = F.relu(x)
      x = self.convs[i](x)
    x = x + identity
    return x

class BNReLUConv2D(Module):
  def __init__(self, in_channels, *args, **kwargs):
    super().__init__()

    self.bn = BatchNorm2d(in_channels)
    self.conv = Conv2d(in_channels, *args, **kwargs)

  def forward(self, x):
    x = self.bn(x)
    x = F.relu(x)
    x = self.conv(x)
    return x

class Net(Module):
  def __init__(self):
    super(Net, self).__init__()

    self.c1 = BNReLUConv2D(3, 32, (3, 3), padding=1)
    self.c2 = BNReLUConv2D(32, 64, (3, 3), stride=2, padding=1)

    self.r = Residual(64, layers=3)

    self.d1 = Conv2d(64, 256, (3, 3), padding=1)
    self.u1 = PixelShuffle(2)

    self.out1 = BNReLUConv2D(64, 32, (3, 3), padding=1)
    self.out2 = BNReLUConv2D(32, 3, (3, 3), padding=1)

  def forward(self, x):
    x = self.c1(x)
    x = self.c2(x)

    x = self.r(x)
    x = RoundIdGradient.apply(x)

    x = self.d1(x)
    x = self.u1(x)

    x = self.out1(x)
    x = self.out2(x)

    return x

# class Net(Module):
#   def __init__(self):
#     super(Net, self).__init__()

#     self.c1 = BNReLUConv2D(3, 32, (3, 3), padding=1)
#     self.c2 = BNReLUConv2D(32, 64, (3, 3), padding=1)

#     self.r1 = Residual(64, layers=1)
#     self.d1 = BNReLUConv2D(64, 128, (3, 3), stride=2, padding=1)

#     self.r2 = Residual(128, layers=1)

#     self.bot = BNReLUConv2D(128, 128, (3, 3), stride=2, padding=1)

#     self.d2 = Conv2d(128, 512, (3, 3), padding=1)
#     self.u1 = PixelShuffle(2)
#     self.r3 = Residual(128, layers=1)

#     self.d3 = Conv2d(128, 256, (3, 3), padding=1)
#     self.u2 = PixelShuffle(2)

#     self.out = Residual(64, layers=1)
#     self.out1 = BNReLUConv2D(64, 32, (3, 3), padding=1)
#     self.out2 = BNReLUConv2D(32, 3, (3, 3), padding=1)

#   def forward(self, x):
#     x = self.c1(x)
#     x = self.c2(x)

#     id1 = x

#     x = self.r1(x)
#     x = self.d1(x)

#     id2 = x

#     x = self.r2(x)

#     x = self.bot(x)
#     x = RoundIdGradient.apply(x)

#     x = self.d2(x)
#     x = self.u1(x)
#     x = self.r3(x)

#     x = x + id2

#     x = self.d3(x)
#     x = self.u2(x)

#     x = x + id1

#     x = self.out(x)
#     x = self.out1(x)
#     x = self.out2(x)

#     return x
