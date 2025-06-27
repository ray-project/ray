import ray
from ray._common.test_utils import Collector

def add_numbers(a, b):
    return a + b

def test_collector_class():
    collector = Collector.remote()

    random_items = ['this', 'is', 1 , 'demo', 'string']

    for item in random_items:
        collector.add.remote(item)

    result = ray.get(collector.get.remote())

    assert len(result) == len(random_items)

    for i in range(0, len(result)):
        assert result[i] == random_items[i]
