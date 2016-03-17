import importlib

import orchpy

def serialize(obj):
  if hasattr(obj, "serialize"):
    primitive_obj = ((type(obj).__module__, type(obj).__name__), obj.serialize())
  else:
    # TODO(rkn): Right now we don't handle arbitrary python objects, but later
    # we can unpack the fields of a python object into a list and call
    # orchpy.lib.serialize_object.
    primitive_obj = ("primitive", obj)
  return orchpy.lib.serialize_object(primitive_obj)

def deserialize(capsule):
  primitive_obj = orchpy.lib.deserialize_object(capsule)
  if primitive_obj[0] == "primitive":
    return primitive_obj[1]
  else:
    # assert primitive_obj[0] must be a tuple of module and class name
    type_module, type_name = primitive_obj[0]
    module = importlib.import_module(type_module)
    if hasattr(module.__dict__[type_name], "deserialize"):
      obj = module.__dict__[type_name]()
      obj.deserialize(primitive_obj[1])
      return obj
