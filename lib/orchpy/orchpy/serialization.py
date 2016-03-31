import importlib

import orchpy

def to_primitive(obj):
  if hasattr(obj, "serialize"):
    primitive_obj = ((type(obj).__module__, type(obj).__name__), obj.serialize())
  else:
    primitive_obj = ("primitive", obj)
  return primitive_obj

def from_primitive(primitive_obj):
  if primitive_obj[0] == "primitive":
    obj = primitive_obj[1]
  else:
    # This code assumes that the type module.__dict__[type_name] knows how to deserialize itself
    type_module, type_name = primitive_obj[0]
    module = importlib.import_module(type_module)
    obj = module.__dict__[type_name]()
    obj.deserialize(primitive_obj[1])
  return obj

def serialize(obj):
  primitive_obj = to_primitive(obj)
  return orchpy.lib.serialize_object(primitive_obj)

def deserialize(capsule):
  primitive_obj = orchpy.lib.deserialize_object(capsule)
  return from_primitive(primitive_obj)

def serialize_call(func_name, args):
  primitive_args = [(arg if isinstance(arg, orchpy.lib.ObjRef) else to_primitive(arg)) for arg in args]
  return orchpy.lib.serialize_call(func_name, primitive_args)

def deserialize_call(call):
  func_name, primitive_args, return_objrefs = orchpy.lib.deserialize_call(call)
  args = [(arg if isinstance(arg, orchpy.lib.ObjRef) else from_primitive(arg)) for arg in primitive_args]
  return func_name, args, return_objrefs
