# This file is used by CrossLanguageDeploymentTest.java to test cross-language
# invocation.
from ray import serve


def echo_server(v):
    return v


@serve.deployment
class Counter(object):
    def __init__(self, value):
        self.value = int(value)

    def increase(self, delta):
        self.value += int(delta)
        return str(self.value)

    def reconfigure(self, value_str):
        self.value = int(value_str)
