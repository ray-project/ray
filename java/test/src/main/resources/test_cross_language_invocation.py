# This file is used by CrossLanguageInvocationTest.java to test cross-language
# invocation.

import asyncio

import pyarrow as pa

import ray


@ray.remote
def py_return_input(v):
    return v


@ray.remote
def py_func_call_java_function():
    try:
        # None
        r = ray.cross_language.java_function(
            "io.ray.test.CrossLanguageInvocationTest", "returnInput"
        ).remote(None)
        assert ray.get(r) is None
        # bool
        r = ray.cross_language.java_function(
            "io.ray.test.CrossLanguageInvocationTest", "returnInputBoolean"
        ).remote(True)
        assert ray.get(r) is True
        # int
        r = ray.cross_language.java_function(
            "io.ray.test.CrossLanguageInvocationTest", "returnInputInt"
        ).remote(100)
        assert ray.get(r) == 100
        # double
        r = ray.cross_language.java_function(
            "io.ray.test.CrossLanguageInvocationTest", "returnInputDouble"
        ).remote(1.23)
        assert ray.get(r) == 1.23
        # string
        r = ray.cross_language.java_function(
            "io.ray.test.CrossLanguageInvocationTest", "returnInputString"
        ).remote("Hello World!")
        assert ray.get(r) == "Hello World!"
        # list (tuple will be packed by pickle,
        # so only list can be transferred across language)
        r = ray.cross_language.java_function(
            "io.ray.test.CrossLanguageInvocationTest", "returnInputIntArray"
        ).remote([1, 2, 3])
        assert ray.get(r) == [1, 2, 3]
        # pack
        f = ray.cross_language.java_function(
            "io.ray.test.CrossLanguageInvocationTest", "pack"
        )
        input = [100, "hello", 1.23, [1, "2", 3.0]]
        r = f.remote(*input)
        assert ray.get(r) == input
        return "success"
    except Exception as ex:
        return str(ex)


@ray.remote
def py_func_call_java_actor(value):
    assert isinstance(value, bytes)
    c = ray.cross_language.java_actor_class("io.ray.test.TestActor")
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
    f = ray.cross_language.java_function(
        "io.ray.test.CrossLanguageInvocationTest", "callPythonActorHandle"
    )
    r = f.remote(counter)
    return ray.get(r)


@ray.remote
def py_func_python_raise_exception():
    1 / 0


@ray.remote
def py_func_java_throw_exception():
    f = ray.cross_language.java_function(
        "io.ray.test.CrossLanguageInvocationTest", "throwException"
    )
    r = f.remote()
    return ray.get(r)


@ray.remote
def py_func_nest_python_raise_exception():
    f = ray.cross_language.java_function(
        "io.ray.test.CrossLanguageInvocationTest", "raisePythonException"
    )
    r = f.remote()
    return ray.get(r)


@ray.remote
def py_func_nest_java_throw_exception():
    f = ray.cross_language.java_function(
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
class AsyncCounter(object):
    async def __init__(self, value):
        self.value = int(value)
        self.event = asyncio.Event()

    async def block_task(self):
        self.event.wait()

    async def increase(self, delta):
        self.value += int(delta)
        self.event.set()
        return str(self.value).encode("utf-8")


@ray.remote
class SyncCounter(object):
    def __init__(self, value):
        self.value = int(value)
        self.event = asyncio.Event()

    def block_task(self):
        self.event.wait()

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
    cls = ray.cross_language.java_actor_class("io.ray.test.ExampleImpl")
    handle = cls.remote()
    return ray.get(handle.echo.remote("hi"))


@ray.remote
def py_func_call_java_overloaded_method():
    cls = ray.cross_language.java_actor_class("io.ray.test.ExampleImpl")
    handle = cls.remote()
    ref1 = handle.overloadedFunc.remote("first")
    ref2 = handle.overloadedFunc.remote("first", "second")
    result = ray.get([ref1, ref2])
    assert result == ["first", "firstsecond"]
    return True


@ray.remote
def py_put_into_object_store():
    column_values = [0, 1, 2, 3, 4]
    column_array = pa.array(column_values)
    table = pa.Table.from_arrays([column_array], names=["ArrowBigIntVector"])
    return table


@ray.remote
def py_object_store_get_and_check(table):
    column_values = [0, 1, 2, 3, 4]
    column_array = pa.array(column_values)
    expected_table = pa.Table.from_arrays([column_array], names=["ArrowBigIntVector"])

    for column_name in table.column_names:
        column1 = table[column_name]
        column2 = expected_table[column_name]

        indices = pa.compute.equal(column1, column2).to_pylist()
        differing_rows = [i for i, index in enumerate(indices) if not index]

        if differing_rows:
            print(f"Differences in column '{column_name}':")
            for row in differing_rows:
                value1 = column1[row].as_py()
                value2 = column2[row].as_py()
                print(f"Row {row}: {value1} != {value2}")
            raise RuntimeError("Check failed, two tables are not equal!")

    return table
