import pickle
import ray
import torch
import torch.nn.functional as F
import torch.nn as nn


class ConvNet(nn.Module):
    def __init__(self):
        super(ConvNet, self).__init__()
        self.conv1 = nn.Conv2d(1, 3, kernel_size=3)
        self.fc = nn.Linear(192, 10)

    def forward(self, x):
        x = F.relu(F.max_pool2d(self.conv1(x), 3))
        x = x.view(-1, 192)
        x = self.fc(x)
        return F.log_softmax(x, dim=1)

def save_me():
 	model = ConvNet()
 	torch.save(model, "./test.th")
 	return 1

ray_func = ray.remote(save_me)

ray.init()
ray.get(ray_func.remote())