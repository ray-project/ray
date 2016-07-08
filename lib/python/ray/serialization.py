import importlib
import numpy as np

import ray

# The following definitions are required because Python doesn't allow custom
# attributes for primitive types. We need custom attributes for (a) implementing
# destructors that close the shared memory segment that the object resides in
# and (b) fixing https://github.com/amplab/ray/issues/72.

class Int(int):
  pass

class Long(long):
  pass

class Float(float):
  pass

class List(list):
  pass

class Dict(dict):
  pass

class Tuple(tuple):
  pass

class Str(str):
  pass

class NDArray(np.ndarray):
  pass

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

def is_arrow_serializable(value):
  return isinstance(value, np.ndarray) and value.dtype.name in ["int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64", "float32", "float64"]

def serialize(worker_capsule, obj):
  primitive_obj = to_primitive(obj)
  obj_capsule, contained_objrefs = ray.lib.serialize_object(worker_capsule, primitive_obj) # contained_objrefs is a list of the objrefs contained in obj
  return obj_capsule, contained_objrefs

def deserialize(worker_capsule, capsule):
  primitive_obj = ray.lib.deserialize_object(worker_capsule, capsule)
  return from_primitive(primitive_obj)

def serialize_task(worker_capsule, func_name, args):
  primitive_args = [(arg if isinstance(arg, ray.lib.ObjRef) else to_primitive(arg)) for arg in args]
  return ray.lib.serialize_task(worker_capsule, func_name, primitive_args)

def deserialize_task(worker_capsule, task):
  func_name, primitive_args, return_objrefs = ray.lib.deserialize_task(worker_capsule, task)
  args = [(arg if isinstance(arg, ray.lib.ObjRef) else from_primitive(arg)) for arg in primitive_args]
  return func_name, args, return_objrefs
