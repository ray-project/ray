import pytest
import ray
import random
from multipart_basic import Basic, RefBased

@pytest.fixture
def ray_init_shutdown():
    ray.init()
    yield
    ray.shutdown()

class Payload:
    def __init__(self):
        self.id = random.randint(0, 100000)
    
    def __eq__(self, __o: object) -> bool:
        if type(self) != type(__o):
            return False
        else:
            return self.get_id() == __o.get_id()

    def get_id(self):
        return self.id

@pytest.mark.parametrize("Implementation", [Basic, RefBased])
def test_standard_implementations(ray_init_shutdown, Implementation):
    num_items = 100
    items = [Payload() for _ in range(num_items)]
    impl = Implementation.remote(items)
    
    for idx in range(num_items):
        assert items[idx] == ray.get(impl.__getitem__.remote(idx))

