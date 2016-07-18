import cloudpickle

def dumps(func, arg_types, return_types):
  return cloudpickle.dumps((func, arg_types, return_types))

def loads(function):
  return cloudpickle.loads(function)
