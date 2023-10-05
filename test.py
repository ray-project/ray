from copy import copy

import pydantic
from pydantic import BaseModel
from pydantic._internal._model_construction import ModelMetaclass

from pydantic_core import SchemaSerializer
from pydantic_core._pydantic_core import SchemaSerializer


class CloudpickleableSchemaSerializer: 
    def __init__(self, *args):
        self._args = args
        print(args)
        self._schema_serializer = SchemaSerializer(*args)

    def __reduce__(self):
        return CloudpickleableSchemaSerializer, self._args

    def __getattr__(self, attr: str):
        return getattr(self._schema_serializer, attr)

pydantic._internal._model_construction.SchemaSerializer = CloudpickleableSchemaSerializer
pydantic._internal._dataclasses.SchemaSerializer = CloudpickleableSchemaSerializer
pydantic.type_adapter.SchemaSerializer = CloudpickleableSchemaSerializer

import pickle
from ray import cloudpickle

class User(BaseModel):
    id: int
    name: str = "Jane Doe"

b = cloudpickle.dumps(User)
u2 = cloudpickle.loads(b)

john = u2(id=1, name="John")
print(john)
