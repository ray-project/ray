from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import base64

import ray
from ray.tune.registry import _to_pinnable, _from_pinnable


_pinned_objects = []
PINNED_OBJECT_PREFIX = "ray.tune.PinnedObject:"


def pin_in_object_store(obj):
    obj_id = ray.put(_to_pinnable(obj))
    _pinned_objects.append(ray.get(obj_id))
    return "{}{}".format(
        PINNED_OBJECT_PREFIX, base64.b64encode(obj_id.id()).decode("utf-8"))


def get_pinned_object(pinned_id):
    from ray.local_scheduler import ObjectID
    return _from_pinnable(ray.get(ObjectID(
        base64.b64decode(pinned_id[len(PINNED_OBJECT_PREFIX):]))))


if __name__ == '__main__':
    ray.init()
    X = pin_in_object_store("hello")
    print(X)
    result = get_pinned_object(X)
    print(result)
