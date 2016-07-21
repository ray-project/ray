import cloudpickle

def serialize(function):
  return cloudpickle.dumps(function)

def deserialize(serialized_function):
  return cloudpickle.loads(serialized_function)

def dumps(func, arg_types, return_types):
  return cloudpickle.dumps((func, arg_types, return_types))

def loads(function):
  return cloudpickle.loads(function)
