# This file is used by CrossLanguageInvocationTest.java to test cross-language
# invocation.

import ray


@ray.remote
def py_func(value):
    assert isinstance(value, bytes)
    return b"Response from Python: " + value


@ray.remote
def py_func_call_java_function(value):
    assert isinstance(value, bytes)
    f = ray.java_function("org.ray.api.test.CrossLanguageInvocationTest",
                          "bytesEcho")
    r = f.remote(value)
    return b"[Python]py_func -> " + ray.get(r)


@ray.remote
def py_func_call_java_actor(value):
    assert isinstance(value, bytes)
    c = ray.java_actor_class(
        "org.ray.api.test.CrossLanguageInvocationTest$TestActor")
    java_actor = c.remote(b"Counter")
    r = java_actor.concat.remote(value)
    return ray.get(r)


@ray.remote
def py_func_call_java_actor_from_handle(value):
    assert isinstance(value, bytes)
    actor_handle = ray.actor.ActorHandle._deserialization_helper(value)
    r = actor_handle.concat.remote(b"2")
    return ray.get(r)


@ray.remote
def py_func_call_python_actor_from_handle(value):
    assert isinstance(value, bytes)
    actor_handle = ray.actor.ActorHandle._deserialization_helper(value)
    r = actor_handle.increase.remote(2)
    return ray.get(r)


@ray.remote
def py_func_pass_python_actor_handle():
    counter = Counter.remote(2)
    f = ray.java_function("org.ray.api.test.CrossLanguageInvocationTest",
                          "callPythonActorHandle")
    r = f.remote(counter._serialization_helper()[0])
    return ray.get(r)


@ray.remote
class Counter(object):
    def __init__(self, value):
        self.value = int(value)

    def increase(self, delta):
        self.value += int(delta)
        return str(self.value).encode("utf-8")
