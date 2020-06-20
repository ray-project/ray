# This file is used by CrossLanguageInvocationTest.java to test cross-language
# invocation.

import ray


@ray.remote
def py_return_input(v):
    return v


@ray.remote
def py_func_call_java_function():
    try:
        # None
        r = ray.java_function("io.ray.api.test.CrossLanguageInvocationTest",
                              "returnInput").remote(None)
        assert ray.get(r) is None
        # bool
        r = ray.java_function("io.ray.api.test.CrossLanguageInvocationTest",
                              "returnInputBoolean").remote(True)
        assert ray.get(r) is True
        # int
        r = ray.java_function("io.ray.api.test.CrossLanguageInvocationTest",
                              "returnInputInt").remote(100)
        assert ray.get(r) == 100
        # double
        r = ray.java_function("io.ray.api.test.CrossLanguageInvocationTest",
                              "returnInputDouble").remote(1.23)
        assert ray.get(r) == 1.23
        # string
        r = ray.java_function("io.ray.api.test.CrossLanguageInvocationTest",
                              "returnInputString").remote("Hello World!")
        assert ray.get(r) == "Hello World!"
        # list (tuple will be packed by pickle,
        # so only list can be transferred across language)
        r = ray.java_function("io.ray.api.test.CrossLanguageInvocationTest",
                              "returnInputIntArray").remote([1, 2, 3])
        assert ray.get(r) == [1, 2, 3]
        # pack
        f = ray.java_function("io.ray.api.test.CrossLanguageInvocationTest",
                              "pack")
        input = [100, "hello", 1.23, [1, "2", 3.0]]
        r = f.remote(*input)
        assert ray.get(r) == input
        return "success"
    except Exception as ex:
        return str(ex)


@ray.remote
def py_func_call_java_actor(value):
    assert isinstance(value, bytes)
    c = ray.java_actor_class(
        "io.ray.api.test.CrossLanguageInvocationTest$TestActor")
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
    f = ray.java_function("io.ray.api.test.CrossLanguageInvocationTest",
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
