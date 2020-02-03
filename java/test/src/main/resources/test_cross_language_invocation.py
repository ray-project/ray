# This file is used by CrossLanguageInvocationTest.java to test cross-language
# invocation.

import six

import ray


@ray.remote
def py_func(value):
    assert isinstance(value, bytes)
    f = ray.java_function("org.ray.api.test.CrossLanguageInvocationTest",
                          "bytesEcho")
    r = f.remote(value)
    return b"[Python]py_func -> " + ray.get(r)


@ray.remote
class Counter(object):
    def __init__(self, value):
        self.value = int(value)
        c = ray.java_actor_class(
            "org.ray.api.test.CrossLanguageInvocationTest$TestActor")
        self.java_actor = c.remote(b"Counter")

    def increase(self, delta):
        self.value += int(delta)
        s = str(self.value).encode("utf-8") if six.PY3 else str(self.value)
        r = self.java_actor.concat.remote(s)
        return ray.get(r)
