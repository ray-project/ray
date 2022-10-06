from torch import nn


def get_activation(name, framework="torch"):
    return {"torch": {"relu": nn.ReLU, "tanh": nn.Tanh, "swish": nn.SiLU}}[framework][
        name
    ]


def get_linear(name, framework="torch"):
    return {"torch": nn.Linear}[framework]
