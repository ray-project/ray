# Python test module invoked from Go via cross-language RemotePython.
# Functions are plain module-level (no @ray.remote required on the Python side;
# the Go driver submits the descriptor directly and the Python worker loads it).

def add(a, b):
    return a + b


def multiply(a, b):
    return a * b


def concat(s1, s2):
    return s1 + s2


def divide(a, b):
    return a / b


def get_dict():
    return {"key": "value"}


class Calculator:
    def __init__(self, initial=0):
        self.value = initial

    def add(self, x):
        self.value += x
        return self.value

    def multiply(self, x):
        self.value *= x
        return self.value

    def get_value(self):
        return self.value
