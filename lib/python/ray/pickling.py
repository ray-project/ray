# Note that a little bit of code here is taken and slightly modified from the pickler because it was not possible to change its behavior otherwise.

import sys
import typing
from ctypes import c_void_p
from cloudpickle import pickle, cloudpickle, CloudPickler, load, loads

try:
	from ctypes import pythonapi
	pythonapi.PyCell_Set  # Make sure this exists
except:
	pythonapi = None

def dump(obj, file, protocol=2):
  return BetterPickler(file, protocol).dump(obj)

def dumps(obj):
  stringio = cloudpickle.StringIO()
  dump(obj, stringio)
  return stringio.getvalue()

def _make_skel_func(code, closure, base_globals = None):
  """ Creates a skeleton function object that contains just the provided
      code and the correct number of cells in func_closure.  All other
      func attributes (e.g. func_globals) are empty.
  """
  if base_globals is None: base_globals = {}
  base_globals['__builtins__'] = __builtins__
  return _make_skel_func.__class__(code, base_globals, None, None, tuple(closure))

def _fill_function(func, globals, defaults, closure, dict):
  """ Fills in the rest of function data into the skeleton function object
      that were created via _make_skel_func(), including closures.
  """
  result = cloudpickle._fill_function(func, globals, defaults, dict)
  if pythonapi is not None:
    for i, v in enumerate(closure):
      pythonapi.PyCell_Set(c_void_p(id(result.__closure__[i])), c_void_p(id(v)))
  return result

def _create_type(type_repr):
  return eval(type_repr.replace("~", ""), None, (lambda d: d.setdefault("typing", typing) and None or d)(dict(typing.__dict__)))

class BetterPickler(CloudPickler):
  def save_function_tuple(self, func):
    code, f_globals, defaults, closure, dct, base_globals = self.extract_func_data(func)

    self.save(_fill_function)
    self.write(pickle.MARK)

    self.save(_make_skel_func if pythonapi else cloudpickle._make_skel_func)
    self.save((code, map(lambda _: cloudpickle._make_cell(None), closure) if closure and pythonapi is not None else closure, base_globals))
    self.write(pickle.REDUCE)
    self.memoize(func)

    self.save(f_globals)
    self.save(defaults)
    self.save(closure)
    self.save(dct)
    self.write(pickle.TUPLE)
    self.write(pickle.REDUCE)
  def save_cell(self, obj):
    self.save(cloudpickle._make_cell)
    self.save((obj.cell_contents,))
    self.write(pickle.REDUCE)
  def save_type(self, obj):
    self.save(_create_type)
    self.save((repr(obj),))
    self.write(pickle.REDUCE)
  dispatch = CloudPickler.dispatch.copy()
  dispatch[(lambda _: lambda: _)(0).__closure__[0].__class__] = save_cell
  dispatch[typing.GenericMeta] = save_type
