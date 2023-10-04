from copy import copy

from pydantic import BaseModel
from pydantic_core import SchemaSerializer
from pydantic._internal._model_construction import ModelMetaclass

import pickle
from ray import cloudpickle

class User(BaseModel):
    id: int
    name: str = "Jane Doe"

b = pickle.dumps(User)
u2 = pickle.loads(b)

john = u2(id=1, name="John")
print(john)
