import flax
from flax import optim
import flax.linen as nn

from functools import partial 


class ToyModel(nn.Module):
    num_classes: int

    def setup(self):
        self.conv1 = nn.Conv(features=32, kernel_size=(3, 3))
        self.relu = nn.relu
        self.avg_pool = partial(nn.avg_pool, window_shape=(2, 2), strides=(2, 2))
        self.conv2 = nn.Conv(features=64, kernel_size=(3, 3))
        self.fc1 = nn.Dense(features=256)
        self.fc2 = nn.Dense(features=self.num_classes)
        self.log_softmax = nn.log_softmax

    # @nn.compact
    def __call__(self, x):
        x = self.conv1(x)
        x = self.relu(x)
        x = self.avg_pool(x)
        x = self.conv2(x)
        x = self.relu(x)
        x = self.avg_pool(x)
        x = x.reshape((x.shape[0], -1))  # flatten
        x = self.fc1(x)
        x = self.relu(x)
        x = self.fc2(x)
        x = self.log_softmax(x)
        return x