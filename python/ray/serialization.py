from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import cloudpickle as pickle

import ray.numbuf


class RaySerializationException(Exception):
  def __init__(self, message, example_object):
    Exception.__init__(self, message)
    self.example_object = example_object


class RayDeserializationException(Exception):
  def __init__(self, message, class_id):
    Exception.__init__(self, message)
    self.class_id = class_id


class RayNotDictionarySerializable(Exception):
  pass


def check_serializable(cls):
  """Throws an exception if Ray cannot serialize this class efficiently.

  Args:
    cls (type): The class to be serialized.

  Raises:
    Exception: An exception is raised if Ray cannot serialize this class
      efficiently.
  """
  if is_named_tuple(cls):
    # This case works.
    return
  if not hasattr(cls, "__new__"):
    print("The class {} does not have a '__new__' attribute and is probably "
          "an old-stye class. Please make it a new-style class by inheriting "
          "from 'object'.")
    raise RayNotDictionarySerializable("The class {} does not have a "
                                       "'__new__' attribute and is probably "
                                       "an old-style class. We do not support "
                                       "this. Please make it a new-style "
                                       "class by inheriting from 'object'."
                                       .format(cls))
  try:
    obj = cls.__new__(cls)
  except:
    raise RayNotDictionarySerializable("The class {} has overridden '__new__'"
                                       ", so Ray may not be able to serialize "
                                       "it efficiently.".format(cls))
  if not hasattr(obj, "__dict__"):
    raise RayNotDictionarySerializable("Objects of the class {} do not have a "
                                       "'__dict__' attribute, so Ray cannot "
                                       "serialize it efficiently.".format(cls))
  if hasattr(obj, "__slots__"):
    raise RayNotDictionarySerializable("The class {} uses '__slots__', so Ray "
                                       "may not be able to serialize it "
                                       "efficiently.".format(cls))


# This field keeps track of a whitelisted set of classes that Ray will
# serialize.
type_to_class_id = dict()
whitelisted_classes = dict()
classes_to_pickle = set()
custom_serializers = dict()
custom_deserializers = dict()


def is_named_tuple(cls):
  """Return True if cls is a namedtuple and False otherwise."""
  b = cls.__bases__
  if len(b) != 1 or b[0] != tuple:
    return False
  f = getattr(cls, "_fields", None)
  if not isinstance(f, tuple):
    return False
  return all(type(n) == str for n in f)


def add_class_to_whitelist(cls, class_id, pickle=False, custom_serializer=None,
                           custom_deserializer=None):
  """Add cls to the list of classes that we can serialize.

  Args:
    cls (type): The class that we can serialize.
    class_id: A string of bytes used to identify the class.
    pickle (bool): True if the serialization should be done with pickle. False
      if it should be done efficiently with Ray.
    custom_serializer: This argument is optional, but can be provided to
      serialize objects of the class in a particular way.
    custom_deserializer: This argument is optional, but can be provided to
      deserialize objects of the class in a particular way.
  """
  type_to_class_id[cls] = class_id
  whitelisted_classes[class_id] = cls
  if pickle:
    classes_to_pickle.add(class_id)
  if custom_serializer is not None:
    custom_serializers[class_id] = custom_serializer
    custom_deserializers[class_id] = custom_deserializer


def serialize(obj):
  """This is the callback that will be used by numbuf.

  If numbuf does not know how to serialize an object, it will call this method.

  Args:
    obj (object): A Python object.

  Returns:
    A dictionary that has the key "_pyttype_" to identify the class, and
      contains all information needed to reconstruct the object.
  """
  if type(obj) not in type_to_class_id:
    raise RaySerializationException("Ray does not know how to serialize "
                                    "objects of type {}.".format(type(obj)),
                                    obj)
  class_id = type_to_class_id[type(obj)]

  if class_id in classes_to_pickle:
    serialized_obj = {"data": pickle.dumps(obj),
                      "pickle": True}
  elif class_id in custom_serializers:
    serialized_obj = {"data": custom_serializers[class_id](obj)}
  else:
    # Handle the namedtuple case.
    if is_named_tuple(type(obj)):
      serialized_obj = {}
      serialized_obj["_ray_getnewargs_"] = obj.__getnewargs__()
    elif hasattr(obj, "__dict__"):
      serialized_obj = obj.__dict__
    else:
      raise RaySerializationException("We do not know how to serialize the "
                                      "object '{}'".format(obj), obj)
  result = dict(serialized_obj, **{"_pytype_": class_id})
  return result


def deserialize(serialized_obj):
  """This is the callback that will be used by numbuf.

  If numbuf encounters a dictionary that contains the key "_pytype_" during
    deserialization, it will ask this callback to deserialize the object.

  Args:
    serialized_obj (object): A dictionary that contains the key "_pytype_".

  Returns:
    A Python object.

  Raises:
    An exception is raised if we do not know how to deserialize the object.
  """
  class_id = serialized_obj["_pytype_"]

  if "pickle" in serialized_obj:
    # The object was pickled, so unpickle it.
    obj = pickle.loads(serialized_obj["data"])
  else:
    assert class_id not in classes_to_pickle
    if class_id not in whitelisted_classes:
      # If this happens, that means that the call to _register_class, which
      # should have added the class to the list of whitelisted classes, has not
      # yet propagated to this worker. It should happen if we wait a little
      # longer.
      raise RayDeserializationException("The class {} is not one of the "
                                        "whitelisted classes."
                                        .format(class_id), class_id)
    cls = whitelisted_classes[class_id]
    if class_id in custom_deserializers:
      obj = custom_deserializers[class_id](serialized_obj["data"])
    else:
      # In this case, serialized_obj should just be the __dict__ field.
      if "_ray_getnewargs_" in serialized_obj:
        obj = cls.__new__(cls, *serialized_obj["_ray_getnewargs_"])
      else:
        obj = cls.__new__(cls)
        serialized_obj.pop("_pytype_")
        obj.__dict__.update(serialized_obj)
  return obj


def set_callbacks():
  """Register the custom callbacks with numbuf.

  The serialize callback is used to serialize objects that numbuf does not know
  how to serialize (for example custom Python classes). The deserialize
  callback is used to serialize objects that were serialized by the serialize
  callback.
  """
  ray.numbuf.register_callbacks(serialize, deserialize)


def clear_state():
  type_to_class_id.clear()
  whitelisted_classes.clear()
  classes_to_pickle.clear()
  custom_serializers.clear()
  custom_deserializers.clear()
