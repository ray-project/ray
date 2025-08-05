import sys

import pytest

import ray

def test_pass_to_task(ray_start_regular_shared):
    obj_ref = ray.put("Hello world!")

    @ray.remote
    def f(arg: ray.ObjectRef) -> str:
        assert arg == obj_ref
        return ray.get(arg)

    assert ray.get(f.remote(ray.util.pass_by_reference(obj_ref))) == "Hello world!"

def test_pass_to_nested_task(ray_start_regular_shared):
    obj_ref = ray.put("Hello world!")

    @ray.remote
    def g(arg: ray.ObjectRef):
        assert arg == obj_ref
        return ray.get(arg)

    @ray.remote
    def f(arg: ray.ObjectRef) -> str:
        assert arg == obj_ref
        return ray.get(g.remote(ray.util.pass_by_reference(obj_ref)))

    assert ray.get(f.remote(ray.util.pass_by_reference(obj_ref))) == "Hello world!"

def test_pass_to_actor(ray_start_regular_shared):
    constructor_obj_ref = ray.put("Hello constructor!")
    method_obj_ref = ray.put("Hello method!")

    @ray.remote
    class A:
        def __init__(self, arg: ray.ObjectRef):
            assert arg == constructor_obj_ref
            self._constructor_obj_ref = constructor_obj_ref

        def get_constructor_ref(self) -> str:
            return ray.get(self._constructor_obj_ref)
        
        def method(self, arg: ray.ObjectRef) -> str:
            assert arg == method_obj_ref
            return ray.get(method_obj_ref)

    a = A.remote(ray.util.pass_by_reference(constructor_obj_ref))
    assert ray.get(a.get_constructor_ref.remote()) == "Hello constructor!"
    assert ray.get(a.method.remote(ray.util.pass_by_reference(method_obj_ref))) == "Hello method!"

if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
