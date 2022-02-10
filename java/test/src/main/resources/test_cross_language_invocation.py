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
        r = ray.java_function(
            "io.ray.test.CrossLanguageInvocationTest", "returnInput"
        ).remote(None)
        assert ray.get(r) is None
        # bool
        r = ray.java_function(
            "io.ray.test.CrossLanguageInvocationTest", "returnInputBoolean"
        ).remote(True)
        assert ray.get(r) is True
        # int
        r = ray.java_function(
            "io.ray.test.CrossLanguageInvocationTest", "returnInputInt"
        ).remote(100)
        assert ray.get(r) == 100
        # double
        r = ray.java_function(
            "io.ray.test.CrossLanguageInvocationTest", "returnInputDouble"
        ).remote(1.23)
        assert ray.get(r) == 1.23
        # string
        r = ray.java_function(
            "io.ray.test.CrossLanguageInvocationTest", "returnInputString"
        ).remote("Hello World!")
        assert ray.get(r) == "Hello World!"
        # list (tuple will be packed by pickle,
        # so only list can be transferred across language)
        r = ray.java_function(
            "io.ray.test.CrossLanguageInvocationTest", "returnInputIntArray"
        ).remote([1, 2, 3])
        assert ray.get(r) == [1, 2, 3]
        # pack
        f = ray.java_function("io.ray.test.CrossLanguageInvocationTest", "pack")
        input = [100, "hello", 1.23, [1, "2", 3.0]]
        r = f.remote(*input)
        assert ray.get(r) == input
        return "success"
    except Exception as ex:
        return str(ex)


@ray.remote
def py_func_call_java_actor(value):
    assert isinstance(value, bytes)
    c = ray.java_actor_class("io.ray.test.CrossLanguageInvocationTest$TestActor")
    java_actor = c.remote(b"Counter")
    r = java_actor.concat.remote(value)
    return ray.get(r)


@ray.remote
def py_func_call_java_actor_from_handle(actor_handle):
    r = actor_handle.concat.remote(b"2")
    return ray.get(r)


@ray.remote
def py_func_call_python_actor_from_handle(actor_handle):
    r = actor_handle.increase.remote(2)
    return ray.get(r)


@ray.remote
def py_func_pass_python_actor_handle():
    counter = Counter.remote(2)
    f = ray.java_function(
        "io.ray.test.CrossLanguageInvocationTest", "callPythonActorHandle"
    )
    r = f.remote(counter)
    return ray.get(r)


@ray.remote
def py_func_python_raise_exception():
    1 / 0


@ray.remote
def py_func_java_throw_exception():
    f = ray.java_function("io.ray.test.CrossLanguageInvocationTest", "throwException")
    r = f.remote()
    return ray.get(r)


@ray.remote
def py_func_nest_python_raise_exception():
    f = ray.java_function(
        "io.ray.test.CrossLanguageInvocationTest", "raisePythonException"
    )
    r = f.remote()
    return ray.get(r)


@ray.remote
def py_func_nest_java_throw_exception():
    f = ray.java_function(
        "io.ray.test.CrossLanguageInvocationTest", "throwJavaException"
    )
    r = f.remote()
    return ray.get(r)


@ray.remote
class Counter(object):
    def __init__(self, value):
        self.value = int(value)

    def increase(self, delta):
        self.value += int(delta)
        return str(self.value).encode("utf-8")


@ray.remote
def py_func_create_named_actor():
    counter = Counter.options(name="py_named_actor", lifetime="detached").remote(100)
    assert ray.get(counter.increase.remote(1)) == b"101"
    return b"true"


@ray.remote
def py_func_get_and_invoke_named_actor():
    java_named_actor = ray.get_actor("java_named_actor")
    assert ray.get(java_named_actor.concat.remote(b"world")) == b"helloworld"
    return b"true"


@ray.remote
def py_func_call_java_overrided_method_with_default_keyword():
    cls = ray.java_actor_class("io.ray.test.ExampleImpl")
    handle = cls.remote()
    return ray.get(handle.echo.remote("hi"))
