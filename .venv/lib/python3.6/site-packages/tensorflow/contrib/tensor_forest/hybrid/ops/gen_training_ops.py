"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: training_ops.cc
"""

import collections as _collections
import six as _six

from tensorflow.python import pywrap_tensorflow as _pywrap_tensorflow
from tensorflow.python.eager import context as _context
from tensorflow.python.eager import core as _core
from tensorflow.python.eager import execute as _execute
from tensorflow.python.framework import dtypes as _dtypes
from tensorflow.python.framework import errors as _errors
from tensorflow.python.framework import tensor_shape as _tensor_shape

from tensorflow.core.framework import op_def_pb2 as _op_def_pb2
# Needed to trigger the call to _set_call_cpp_shape_fn.
from tensorflow.python.framework import common_shapes as _common_shapes
from tensorflow.python.framework import op_def_registry as _op_def_registry
from tensorflow.python.framework import ops as _ops
from tensorflow.python.framework import op_def_library as _op_def_library
from tensorflow.python.util.deprecation import deprecated_endpoints
from tensorflow.python.util import dispatch as _dispatch
from tensorflow.python.util.tf_export import tf_export


_hard_routing_function_outputs = ["path_probability", "path"]
_HardRoutingFunctionOutput = _collections.namedtuple(
    "HardRoutingFunction", _hard_routing_function_outputs)


@_dispatch.add_dispatch_list
@tf_export('hard_routing_function')
def hard_routing_function(input_data, tree_parameters, tree_biases, max_nodes, tree_depth, name=None):
  r"""  Chooses a single path for each instance in `input_data` and returns the leaf

    the probability of the path and the path taken.

    tree_depth: The depth of the decision tree.

    input_data: The training batch's features as a 2-d tensor; `input_data[i][j]`
     gives the j-th feature of the i-th input.
    tree_parameters: `tree_parameters[i]` gives the weight of
     the logistic regression model that translates from node features to
     probabilities.
    tree_biases: `tree_biases[i]` gives the bias of the logistic
     regression model that translates from node features to
     probabilities.

    path_probability: `path_probability[i]` gives the probability of reaching each
     node in `path[i]`.
    path: `path[i][j]` gives the jth node in the path taken by the ith data
     instance.

  Args:
    input_data: A `Tensor` of type `float32`.
    tree_parameters: A `Tensor` of type `float32`.
    tree_biases: A `Tensor` of type `float32`.
    max_nodes: An `int`.
    tree_depth: An `int`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (path_probability, path).

    path_probability: A `Tensor` of type `float32`.
    path: A `Tensor` of type `int32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "HardRoutingFunction", name, _ctx._post_execution_callbacks,
        input_data, tree_parameters, tree_biases, "max_nodes", max_nodes,
        "tree_depth", tree_depth)
      _result = _HardRoutingFunctionOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return hard_routing_function_eager_fallback(
            input_data, tree_parameters, tree_biases, max_nodes=max_nodes,
            tree_depth=tree_depth, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              hard_routing_function, input_data=input_data,
                                     tree_parameters=tree_parameters,
                                     tree_biases=tree_biases,
                                     max_nodes=max_nodes,
                                     tree_depth=tree_depth, name=name)
        if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
          return result
        raise
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  max_nodes = _execute.make_int(max_nodes, "max_nodes")
  tree_depth = _execute.make_int(tree_depth, "tree_depth")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "HardRoutingFunction", input_data=input_data,
                               tree_parameters=tree_parameters,
                               tree_biases=tree_biases, max_nodes=max_nodes,
                               tree_depth=tree_depth, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          hard_routing_function, input_data=input_data,
                                 tree_parameters=tree_parameters,
                                 tree_biases=tree_biases, max_nodes=max_nodes,
                                 tree_depth=tree_depth, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("max_nodes", _op.get_attr("max_nodes"), "tree_depth",
            _op.get_attr("tree_depth"))
  _execute.record_gradient(
      "HardRoutingFunction", _inputs_flat, _attrs, _result, name)
  _result = _HardRoutingFunctionOutput._make(_result)
  return _result



def hard_routing_function_eager_fallback(input_data, tree_parameters, tree_biases, max_nodes, tree_depth, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function hard_routing_function
  """
  _ctx = ctx if ctx else _context.context()
  max_nodes = _execute.make_int(max_nodes, "max_nodes")
  tree_depth = _execute.make_int(tree_depth, "tree_depth")
  input_data = _ops.convert_to_tensor(input_data, _dtypes.float32)
  tree_parameters = _ops.convert_to_tensor(tree_parameters, _dtypes.float32)
  tree_biases = _ops.convert_to_tensor(tree_biases, _dtypes.float32)
  _inputs_flat = [input_data, tree_parameters, tree_biases]
  _attrs = ("max_nodes", max_nodes, "tree_depth", tree_depth)
  _result = _execute.execute(b"HardRoutingFunction", 2, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "HardRoutingFunction", _inputs_flat, _attrs, _result, name)
  _result = _HardRoutingFunctionOutput._make(_result)
  return _result

_ops.RegisterShape("HardRoutingFunction")(None)


_k_feature_gradient_outputs = ["routing_gradient", "data_gradient",
                              "weight_gradient"]
_KFeatureGradientOutput = _collections.namedtuple(
    "KFeatureGradient", _k_feature_gradient_outputs)


@_dispatch.add_dispatch_list
@tf_export('k_feature_gradient')
def k_feature_gradient(input_data, tree_parameters, tree_biases, routes, layer_num, random_seed, name=None):
  r"""    Computes the derivative of the routing loss with respect to each decision

      node.  Each decision node is constrained to make a decision based on only
      k features.

      layer_num: The layer number of this tree.
      random_seed: The base random seed.

      input_data: The training batch's features as a 2-d tensor;
       `input_data[i][j]` gives the j-th feature of the i-th input.
      tree_parameters: `tree_parameters[i]` gives the weight of
       the logistic regression model that translates from node features to
       probabilities.
      tree_biases: `tree_biases[i]` gives the bias of the logistic
       regression model that translates from node features to
       probabilities.
      routes: The routes computed by routing_function_op.

      routing_gradient: `routing_gradient` provides du / df, where u is the
       routing function and f is the (vector of) decision functions.  A decision
       function f_i computes the routing decision at node i.

      data_gradient: `data_gradient` provides df / dx, where f is the (vector
       of) decision functions and x is a batch of data.

      weights_gradient: `weights_gradient` provides df / dw, where f is the
       (vector of) decision functions and w is the matrix of parameters that
       determine how instances are routed through a tree.

      f_i, the decision function at node i, is parameterized by t_i (parameters)
      and b_i (bias) and takes data x as input.  This op is called in
      training_ops.py to compute du / df, and we use that to compute

      du / dx = du / df * df / dx,
      du / dt = du / df * df / dt, and
      du / db = du / df * df / db.

  Args:
    input_data: A `Tensor` of type `float32`.
    tree_parameters: A `Tensor` of type `float32`.
    tree_biases: A `Tensor` of type `float32`.
    routes: A `Tensor` of type `float32`.
    layer_num: An `int`.
    random_seed: An `int`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (routing_gradient, data_gradient, weight_gradient).

    routing_gradient: A `Tensor` of type `float32`.
    data_gradient: A `Tensor` of type `float32`.
    weight_gradient: A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "KFeatureGradient", name, _ctx._post_execution_callbacks, input_data,
        tree_parameters, tree_biases, routes, "layer_num", layer_num,
        "random_seed", random_seed)
      _result = _KFeatureGradientOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return k_feature_gradient_eager_fallback(
            input_data, tree_parameters, tree_biases, routes,
            layer_num=layer_num, random_seed=random_seed, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              k_feature_gradient, input_data=input_data,
                                  tree_parameters=tree_parameters,
                                  tree_biases=tree_biases, routes=routes,
                                  layer_num=layer_num,
                                  random_seed=random_seed, name=name)
        if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
          return result
        raise
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  layer_num = _execute.make_int(layer_num, "layer_num")
  random_seed = _execute.make_int(random_seed, "random_seed")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "KFeatureGradient", input_data=input_data,
                            tree_parameters=tree_parameters,
                            tree_biases=tree_biases, routes=routes,
                            layer_num=layer_num, random_seed=random_seed,
                            name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          k_feature_gradient, input_data=input_data,
                              tree_parameters=tree_parameters,
                              tree_biases=tree_biases, routes=routes,
                              layer_num=layer_num, random_seed=random_seed,
                              name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("layer_num", _op.get_attr("layer_num"), "random_seed",
            _op.get_attr("random_seed"))
  _execute.record_gradient(
      "KFeatureGradient", _inputs_flat, _attrs, _result, name)
  _result = _KFeatureGradientOutput._make(_result)
  return _result



def k_feature_gradient_eager_fallback(input_data, tree_parameters, tree_biases, routes, layer_num, random_seed, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function k_feature_gradient
  """
  _ctx = ctx if ctx else _context.context()
  layer_num = _execute.make_int(layer_num, "layer_num")
  random_seed = _execute.make_int(random_seed, "random_seed")
  input_data = _ops.convert_to_tensor(input_data, _dtypes.float32)
  tree_parameters = _ops.convert_to_tensor(tree_parameters, _dtypes.float32)
  tree_biases = _ops.convert_to_tensor(tree_biases, _dtypes.float32)
  routes = _ops.convert_to_tensor(routes, _dtypes.float32)
  _inputs_flat = [input_data, tree_parameters, tree_biases, routes]
  _attrs = ("layer_num", layer_num, "random_seed", random_seed)
  _result = _execute.execute(b"KFeatureGradient", 3, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "KFeatureGradient", _inputs_flat, _attrs, _result, name)
  _result = _KFeatureGradientOutput._make(_result)
  return _result

_ops.RegisterShape("KFeatureGradient")(None)


@_dispatch.add_dispatch_list
@tf_export('k_feature_routing_function')
def k_feature_routing_function(input_data, tree_parameters, tree_biases, layer_num, max_nodes, num_features_per_node, random_seed, name=None):
  r"""  Returns the probability that each input will reach each leaf node.  Each

    decision is made based on k features.

    layer_num: The layer number of this tree.
    max_nodes: The number of nodes in the tree.
    num_features_per_node: The number of features each node can use to make a
     decision.
    random_seed: The base random seed.

    input_data: The training batch's features as a 2-d tensor; `input_data[i][j]`
     gives the j-th feature of the i-th input.
    tree_parameters: `tree_parameters[i]` gives the weight of
     the logistic regression model that translates from node features to
     probabilities.
    tree_biases: `tree_biases[i]` gives the bias of the logistic
     regression model that translates from node features to
     probabilities.
    tree_features: `tree_features[i]` gives the decision feature for node i.

    probabilities: `probabilities[i][j]` is the probability that input i
     will reach node j.

  Args:
    input_data: A `Tensor` of type `float32`.
    tree_parameters: A `Tensor` of type `float32`.
    tree_biases: A `Tensor` of type `float32`.
    layer_num: An `int`.
    max_nodes: An `int`.
    num_features_per_node: An `int`.
    random_seed: An `int`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "KFeatureRoutingFunction", name, _ctx._post_execution_callbacks,
        input_data, tree_parameters, tree_biases, "layer_num", layer_num,
        "max_nodes", max_nodes, "num_features_per_node",
        num_features_per_node, "random_seed", random_seed)
      return _result
    except _core._FallbackException:
      try:
        return k_feature_routing_function_eager_fallback(
            input_data, tree_parameters, tree_biases, layer_num=layer_num,
            max_nodes=max_nodes, num_features_per_node=num_features_per_node,
            random_seed=random_seed, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              k_feature_routing_function, input_data=input_data,
                                          tree_parameters=tree_parameters,
                                          tree_biases=tree_biases,
                                          layer_num=layer_num,
                                          max_nodes=max_nodes,
                                          num_features_per_node=num_features_per_node,
                                          random_seed=random_seed, name=name)
        if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
          return result
        raise
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  layer_num = _execute.make_int(layer_num, "layer_num")
  max_nodes = _execute.make_int(max_nodes, "max_nodes")
  num_features_per_node = _execute.make_int(num_features_per_node, "num_features_per_node")
  random_seed = _execute.make_int(random_seed, "random_seed")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "KFeatureRoutingFunction", input_data=input_data,
                                   tree_parameters=tree_parameters,
                                   tree_biases=tree_biases,
                                   layer_num=layer_num, max_nodes=max_nodes,
                                   num_features_per_node=num_features_per_node,
                                   random_seed=random_seed, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          k_feature_routing_function, input_data=input_data,
                                      tree_parameters=tree_parameters,
                                      tree_biases=tree_biases,
                                      layer_num=layer_num,
                                      max_nodes=max_nodes,
                                      num_features_per_node=num_features_per_node,
                                      random_seed=random_seed, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("layer_num", _op.get_attr("layer_num"), "max_nodes",
            _op.get_attr("max_nodes"), "num_features_per_node",
            _op.get_attr("num_features_per_node"), "random_seed",
            _op.get_attr("random_seed"))
  _execute.record_gradient(
      "KFeatureRoutingFunction", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def k_feature_routing_function_eager_fallback(input_data, tree_parameters, tree_biases, layer_num, max_nodes, num_features_per_node, random_seed, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function k_feature_routing_function
  """
  _ctx = ctx if ctx else _context.context()
  layer_num = _execute.make_int(layer_num, "layer_num")
  max_nodes = _execute.make_int(max_nodes, "max_nodes")
  num_features_per_node = _execute.make_int(num_features_per_node, "num_features_per_node")
  random_seed = _execute.make_int(random_seed, "random_seed")
  input_data = _ops.convert_to_tensor(input_data, _dtypes.float32)
  tree_parameters = _ops.convert_to_tensor(tree_parameters, _dtypes.float32)
  tree_biases = _ops.convert_to_tensor(tree_biases, _dtypes.float32)
  _inputs_flat = [input_data, tree_parameters, tree_biases]
  _attrs = ("layer_num", layer_num, "max_nodes", max_nodes,
  "num_features_per_node", num_features_per_node, "random_seed", random_seed)
  _result = _execute.execute(b"KFeatureRoutingFunction", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "KFeatureRoutingFunction", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("KFeatureRoutingFunction")(None)


@_dispatch.add_dispatch_list
@tf_export('routing_function')
def routing_function(input_data, tree_parameters, tree_biases, max_nodes, name=None):
  r"""  Returns the probability that each input will reach each leaf node.

    max_nodes: The number of nodes in the tree.

    input_data: The training batch's features as a 2-d tensor; `input_data[i][j]`
     gives the j-th feature of the i-th input.
    tree_parameters: `tree_parameters[i]` gives the weight of
     the logistic regression model that translates from node features to
     probabilities.
    tree_biases: `tree_biases[i]` gives the bias of the logistic
     regression model that translates from node features to
     probabilities.

    probabilities: `probabilities[i][j]` is the probability that input i
     will reach node j.

  Args:
    input_data: A `Tensor` of type `float32`.
    tree_parameters: A `Tensor` of type `float32`.
    tree_biases: A `Tensor` of type `float32`.
    max_nodes: An `int`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "RoutingFunction", name, _ctx._post_execution_callbacks, input_data,
        tree_parameters, tree_biases, "max_nodes", max_nodes)
      return _result
    except _core._FallbackException:
      try:
        return routing_function_eager_fallback(
            input_data, tree_parameters, tree_biases, max_nodes=max_nodes,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              routing_function, input_data=input_data,
                                tree_parameters=tree_parameters,
                                tree_biases=tree_biases, max_nodes=max_nodes,
                                name=name)
        if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
          return result
        raise
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  max_nodes = _execute.make_int(max_nodes, "max_nodes")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "RoutingFunction", input_data=input_data,
                           tree_parameters=tree_parameters,
                           tree_biases=tree_biases, max_nodes=max_nodes,
                           name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          routing_function, input_data=input_data,
                            tree_parameters=tree_parameters,
                            tree_biases=tree_biases, max_nodes=max_nodes,
                            name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("max_nodes", _op.get_attr("max_nodes"))
  _execute.record_gradient(
      "RoutingFunction", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def routing_function_eager_fallback(input_data, tree_parameters, tree_biases, max_nodes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function routing_function
  """
  _ctx = ctx if ctx else _context.context()
  max_nodes = _execute.make_int(max_nodes, "max_nodes")
  input_data = _ops.convert_to_tensor(input_data, _dtypes.float32)
  tree_parameters = _ops.convert_to_tensor(tree_parameters, _dtypes.float32)
  tree_biases = _ops.convert_to_tensor(tree_biases, _dtypes.float32)
  _inputs_flat = [input_data, tree_parameters, tree_biases]
  _attrs = ("max_nodes", max_nodes)
  _result = _execute.execute(b"RoutingFunction", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "RoutingFunction", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("RoutingFunction")(None)


@_dispatch.add_dispatch_list
@tf_export('routing_gradient')
def routing_gradient(input_data, tree_parameters, tree_biases, routes, max_nodes, name=None):
  r"""  Computes the derivative of the routing loss with respect to each decision

    node.

    max_nodes: The number of nodes in the tree.

    tree_parameters: `tree_parameters[i]` gives the weight of
     the logistic regression model that translates from node features to
     probabilities.
    tree_biases: `tree_biases[i]` gives the bias of the logistic
     regression model that translates from node features to
     probabilities.
    routes: The routes computed by routing_function_op.

    routing_gradient: `routing_gradient` provides du / df, where u is the routing
     function and f is the (vector of) decision functions.  A decision function
     f_i computes the routing decision at node i.

     f_i is parameterized by t_i (parameters) and b_i (bias) and takes data x as
     input.  This op is called in training_ops.py to compute du / df, and we use
     that to compute

       du / dx = du / df * df / dx,
       du / dt = du / df * df / dt, and
       du / db = du / df * df / db.

  Args:
    input_data: A `Tensor` of type `float32`.
    tree_parameters: A `Tensor` of type `float32`.
    tree_biases: A `Tensor` of type `float32`.
    routes: A `Tensor` of type `float32`.
    max_nodes: An `int`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "RoutingGradient", name, _ctx._post_execution_callbacks, input_data,
        tree_parameters, tree_biases, routes, "max_nodes", max_nodes)
      return _result
    except _core._FallbackException:
      try:
        return routing_gradient_eager_fallback(
            input_data, tree_parameters, tree_biases, routes,
            max_nodes=max_nodes, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              routing_gradient, input_data=input_data,
                                tree_parameters=tree_parameters,
                                tree_biases=tree_biases, routes=routes,
                                max_nodes=max_nodes, name=name)
        if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
          return result
        raise
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  max_nodes = _execute.make_int(max_nodes, "max_nodes")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "RoutingGradient", input_data=input_data,
                           tree_parameters=tree_parameters,
                           tree_biases=tree_biases, routes=routes,
                           max_nodes=max_nodes, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          routing_gradient, input_data=input_data,
                            tree_parameters=tree_parameters,
                            tree_biases=tree_biases, routes=routes,
                            max_nodes=max_nodes, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("max_nodes", _op.get_attr("max_nodes"))
  _execute.record_gradient(
      "RoutingGradient", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def routing_gradient_eager_fallback(input_data, tree_parameters, tree_biases, routes, max_nodes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function routing_gradient
  """
  _ctx = ctx if ctx else _context.context()
  max_nodes = _execute.make_int(max_nodes, "max_nodes")
  input_data = _ops.convert_to_tensor(input_data, _dtypes.float32)
  tree_parameters = _ops.convert_to_tensor(tree_parameters, _dtypes.float32)
  tree_biases = _ops.convert_to_tensor(tree_biases, _dtypes.float32)
  routes = _ops.convert_to_tensor(routes, _dtypes.float32)
  _inputs_flat = [input_data, tree_parameters, tree_biases, routes]
  _attrs = ("max_nodes", max_nodes)
  _result = _execute.execute(b"RoutingGradient", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "RoutingGradient", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("RoutingGradient")(None)


_stochastic_hard_routing_function_outputs = ["path_probability", "path"]
_StochasticHardRoutingFunctionOutput = _collections.namedtuple(
    "StochasticHardRoutingFunction",
    _stochastic_hard_routing_function_outputs)


@_dispatch.add_dispatch_list
@tf_export('stochastic_hard_routing_function')
def stochastic_hard_routing_function(input_data, tree_parameters, tree_biases, tree_depth, random_seed, name=None):
  r"""  Samples a path for each instance in `input_data` and returns the

    probability of the path and the path taken.

    tree_depth: The depth of the decision tree.
    random_seed: The base random seed.

    input_data: The training batch's features as a 2-d tensor; `input_data[i][j]`
     gives the j-th feature of the i-th input.
    tree_parameters: `tree_parameters[i]` gives the weight of
     the logistic regression model that translates from node features to
     probabilities.
    tree_biases: `tree_biases[i]` gives the bias of the logistic
     regression model that translates from node features to
     probabilities.

    path_probability: `path_probability[i]` gives the probability of reaching each
     node in `path[i]`.
    path: `path[i][j]` gives the jth node in the path taken by the ith data
     instance.

  Args:
    input_data: A `Tensor` of type `float32`.
    tree_parameters: A `Tensor` of type `float32`.
    tree_biases: A `Tensor` of type `float32`.
    tree_depth: An `int`.
    random_seed: An `int`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (path_probability, path).

    path_probability: A `Tensor` of type `float32`.
    path: A `Tensor` of type `int32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "StochasticHardRoutingFunction", name, _ctx._post_execution_callbacks,
        input_data, tree_parameters, tree_biases, "tree_depth", tree_depth,
        "random_seed", random_seed)
      _result = _StochasticHardRoutingFunctionOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return stochastic_hard_routing_function_eager_fallback(
            input_data, tree_parameters, tree_biases, tree_depth=tree_depth,
            random_seed=random_seed, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              stochastic_hard_routing_function, input_data=input_data,
                                                tree_parameters=tree_parameters,
                                                tree_biases=tree_biases,
                                                tree_depth=tree_depth,
                                                random_seed=random_seed,
                                                name=name)
        if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
          return result
        raise
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  tree_depth = _execute.make_int(tree_depth, "tree_depth")
  random_seed = _execute.make_int(random_seed, "random_seed")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "StochasticHardRoutingFunction", input_data=input_data,
                                         tree_parameters=tree_parameters,
                                         tree_biases=tree_biases,
                                         tree_depth=tree_depth,
                                         random_seed=random_seed, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          stochastic_hard_routing_function, input_data=input_data,
                                            tree_parameters=tree_parameters,
                                            tree_biases=tree_biases,
                                            tree_depth=tree_depth,
                                            random_seed=random_seed,
                                            name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("tree_depth", _op.get_attr("tree_depth"), "random_seed",
            _op.get_attr("random_seed"))
  _execute.record_gradient(
      "StochasticHardRoutingFunction", _inputs_flat, _attrs, _result, name)
  _result = _StochasticHardRoutingFunctionOutput._make(_result)
  return _result



def stochastic_hard_routing_function_eager_fallback(input_data, tree_parameters, tree_biases, tree_depth, random_seed, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function stochastic_hard_routing_function
  """
  _ctx = ctx if ctx else _context.context()
  tree_depth = _execute.make_int(tree_depth, "tree_depth")
  random_seed = _execute.make_int(random_seed, "random_seed")
  input_data = _ops.convert_to_tensor(input_data, _dtypes.float32)
  tree_parameters = _ops.convert_to_tensor(tree_parameters, _dtypes.float32)
  tree_biases = _ops.convert_to_tensor(tree_biases, _dtypes.float32)
  _inputs_flat = [input_data, tree_parameters, tree_biases]
  _attrs = ("tree_depth", tree_depth, "random_seed", random_seed)
  _result = _execute.execute(b"StochasticHardRoutingFunction", 2,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "StochasticHardRoutingFunction", _inputs_flat, _attrs, _result, name)
  _result = _StochasticHardRoutingFunctionOutput._make(_result)
  return _result

_ops.RegisterShape("StochasticHardRoutingFunction")(None)


_stochastic_hard_routing_gradient_outputs = ["routing_gradient",
                                            "data_gradient",
                                            "parameter_gradient",
                                            "bias_gradient"]
_StochasticHardRoutingGradientOutput = _collections.namedtuple(
    "StochasticHardRoutingGradient",
    _stochastic_hard_routing_gradient_outputs)


@_dispatch.add_dispatch_list
@tf_export('stochastic_hard_routing_gradient')
def stochastic_hard_routing_gradient(input_data, tree_parameters, tree_biases, path_probability, path, tree_depth, name=None):
  r"""  Computes the derivative of the routing loss with respect to each decision

    node.

    tree_depth: The depth of the decision tree.

    input_data: The training batch's features as a 2-d tensor; `input_data[i][j]`
     gives the j-th feature of the i-th input
    tree_parameters: `tree_parameters[i]` gives the weight of
     the logistic regression model that translates from node features to
     probabilities.
    tree_biases: `tree_biases[i]` gives the bias of the logistic
     regression model that translates from node features to
     probabilities.
    path_probability: `path_probability[i]` gives the probability of reaching each
     node in `path[i]`.
    path: `path[i][j]` gives the jth node in the path taken by the ith data
     instance.

    routing_gradient: `routing_gradient` provides du / df, where u is the routing
     function and f is the (vector of) decision functions.  A decision function
     f_i computes the routing decision at node i.
    data_gradient: `data_gradient` provides df / dx, where f is the (vector
     of) decision functions and x is a batch of data.
    parameter_gradient: `parameter_gradient` provides df / dw, where f is the
     (vector of) decision functions and w is the matrix of parameters that
     determine how instances are routed through a tree.
    bias_gradient: `bias_gradient` provides df / db, where f is the
     (vector of) decision functions and b is the vector of bias parameters that
     determine how instances are routed through a tree.

    f_i is parameterized by t_i (parameters) and b_i (bias) and takes data x as
    input.  This op is called in training_ops.py to compute du / df, and we use
    that to compute

       du / dx = du / df * df / dx,
       du / dt = du / df * df / dt, and
       du / db = du / df * df / db.

  Args:
    input_data: A `Tensor` of type `float32`.
    tree_parameters: A `Tensor` of type `float32`.
    tree_biases: A `Tensor` of type `float32`.
    path_probability: A `Tensor` of type `float32`.
    path: A `Tensor` of type `int32`.
    tree_depth: An `int`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (routing_gradient, data_gradient, parameter_gradient, bias_gradient).

    routing_gradient: A `Tensor` of type `float32`.
    data_gradient: A `Tensor` of type `float32`.
    parameter_gradient: A `Tensor` of type `float32`.
    bias_gradient: A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "StochasticHardRoutingGradient", name, _ctx._post_execution_callbacks,
        input_data, tree_parameters, tree_biases, path_probability, path,
        "tree_depth", tree_depth)
      _result = _StochasticHardRoutingGradientOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return stochastic_hard_routing_gradient_eager_fallback(
            input_data, tree_parameters, tree_biases, path_probability, path,
            tree_depth=tree_depth, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              stochastic_hard_routing_gradient, input_data=input_data,
                                                tree_parameters=tree_parameters,
                                                tree_biases=tree_biases,
                                                path_probability=path_probability,
                                                path=path,
                                                tree_depth=tree_depth,
                                                name=name)
        if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
          return result
        raise
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  tree_depth = _execute.make_int(tree_depth, "tree_depth")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "StochasticHardRoutingGradient", input_data=input_data,
                                         tree_parameters=tree_parameters,
                                         tree_biases=tree_biases,
                                         path_probability=path_probability,
                                         path=path, tree_depth=tree_depth,
                                         name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          stochastic_hard_routing_gradient, input_data=input_data,
                                            tree_parameters=tree_parameters,
                                            tree_biases=tree_biases,
                                            path_probability=path_probability,
                                            path=path, tree_depth=tree_depth,
                                            name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("tree_depth", _op.get_attr("tree_depth"))
  _execute.record_gradient(
      "StochasticHardRoutingGradient", _inputs_flat, _attrs, _result, name)
  _result = _StochasticHardRoutingGradientOutput._make(_result)
  return _result



def stochastic_hard_routing_gradient_eager_fallback(input_data, tree_parameters, tree_biases, path_probability, path, tree_depth, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function stochastic_hard_routing_gradient
  """
  _ctx = ctx if ctx else _context.context()
  tree_depth = _execute.make_int(tree_depth, "tree_depth")
  input_data = _ops.convert_to_tensor(input_data, _dtypes.float32)
  tree_parameters = _ops.convert_to_tensor(tree_parameters, _dtypes.float32)
  tree_biases = _ops.convert_to_tensor(tree_biases, _dtypes.float32)
  path_probability = _ops.convert_to_tensor(path_probability, _dtypes.float32)
  path = _ops.convert_to_tensor(path, _dtypes.int32)
  _inputs_flat = [input_data, tree_parameters, tree_biases, path_probability, path]
  _attrs = ("tree_depth", tree_depth)
  _result = _execute.execute(b"StochasticHardRoutingGradient", 4,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "StochasticHardRoutingGradient", _inputs_flat, _attrs, _result, name)
  _result = _StochasticHardRoutingGradientOutput._make(_result)
  return _result

_ops.RegisterShape("StochasticHardRoutingGradient")(None)


@_dispatch.add_dispatch_list
@tf_export('unpack_path')
def unpack_path(path, path_values, name=None):
  r"""  Takes a batch of paths through a tree and a batch of values along those paths

    and returns a batch_size by num_nodes encoding of the path values.

    path: `path[i][j]` gives the jth node in the path taken by the ith data
     instance.
    path_values: `path_values[i][j]` gives the value associated with node j in the
     path defined by the ith instance

    unpacked_paths: `unpacked_paths[i][path[i][k]]` is path_values[i][k] for k in
     [0, tree_depth).  All other elements of unpacked_paths are zero.

  Args:
    path: A `Tensor` of type `int32`.
    path_values: A `Tensor` of type `float32`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "UnpackPath",
        name, _ctx._post_execution_callbacks, path, path_values)
      return _result
    except _core._FallbackException:
      try:
        return unpack_path_eager_fallback(
            path, path_values, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              unpack_path, path=path, path_values=path_values, name=name)
        if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
          return result
        raise
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "UnpackPath", path=path, path_values=path_values, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          unpack_path, path=path, path_values=path_values, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "UnpackPath", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def unpack_path_eager_fallback(path, path_values, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function unpack_path
  """
  _ctx = ctx if ctx else _context.context()
  path = _ops.convert_to_tensor(path, _dtypes.int32)
  path_values = _ops.convert_to_tensor(path_values, _dtypes.float32)
  _inputs_flat = [path, path_values]
  _attrs = None
  _result = _execute.execute(b"UnpackPath", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "UnpackPath", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("UnpackPath")(None)

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "HardRoutingFunction"
#   input_arg {
#     name: "input_data"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "tree_parameters"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "tree_biases"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "path_probability"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "path"
#     type: DT_INT32
#   }
#   attr {
#     name: "max_nodes"
#     type: "int"
#   }
#   attr {
#     name: "tree_depth"
#     type: "int"
#   }
# }
# op {
#   name: "KFeatureGradient"
#   input_arg {
#     name: "input_data"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "tree_parameters"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "tree_biases"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "routes"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "routing_gradient"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "data_gradient"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "weight_gradient"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "layer_num"
#     type: "int"
#   }
#   attr {
#     name: "random_seed"
#     type: "int"
#   }
# }
# op {
#   name: "KFeatureRoutingFunction"
#   input_arg {
#     name: "input_data"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "tree_parameters"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "tree_biases"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "probabilities"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "layer_num"
#     type: "int"
#   }
#   attr {
#     name: "max_nodes"
#     type: "int"
#   }
#   attr {
#     name: "num_features_per_node"
#     type: "int"
#   }
#   attr {
#     name: "random_seed"
#     type: "int"
#   }
# }
# op {
#   name: "RoutingFunction"
#   input_arg {
#     name: "input_data"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "tree_parameters"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "tree_biases"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "probabilities"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "max_nodes"
#     type: "int"
#   }
# }
# op {
#   name: "RoutingGradient"
#   input_arg {
#     name: "input_data"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "tree_parameters"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "tree_biases"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "routes"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "routing_gradient"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "max_nodes"
#     type: "int"
#   }
# }
# op {
#   name: "StochasticHardRoutingFunction"
#   input_arg {
#     name: "input_data"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "tree_parameters"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "tree_biases"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "path_probability"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "path"
#     type: DT_INT32
#   }
#   attr {
#     name: "tree_depth"
#     type: "int"
#   }
#   attr {
#     name: "random_seed"
#     type: "int"
#   }
# }
# op {
#   name: "StochasticHardRoutingGradient"
#   input_arg {
#     name: "input_data"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "tree_parameters"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "tree_biases"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "path_probability"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "path"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "routing_gradient"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "data_gradient"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "parameter_gradient"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "bias_gradient"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "tree_depth"
#     type: "int"
#   }
# }
# op {
#   name: "UnpackPath"
#   input_arg {
#     name: "path"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "path_values"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "unpacked_path"
#     type: DT_FLOAT
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\n\220\001\n\023HardRoutingFunction\022\016\n\ninput_data\030\001\022\023\n\017tree_parameters\030\001\022\017\n\013tree_biases\030\001\032\024\n\020path_probability\030\001\032\010\n\004path\030\003\"\020\n\tmax_nodes\022\003int\"\021\n\ntree_depth\022\003int\n\270\001\n\020KFeatureGradient\022\016\n\ninput_data\030\001\022\023\n\017tree_parameters\030\001\022\017\n\013tree_biases\030\001\022\n\n\006routes\030\001\032\024\n\020routing_gradient\030\001\032\021\n\rdata_gradient\030\001\032\023\n\017weight_gradient\030\001\"\020\n\tlayer_num\022\003int\"\022\n\013random_seed\022\003int\n\270\001\n\027KFeatureRoutingFunction\022\016\n\ninput_data\030\001\022\023\n\017tree_parameters\030\001\022\017\n\013tree_biases\030\001\032\021\n\rprobabilities\030\001\"\020\n\tlayer_num\022\003int\"\020\n\tmax_nodes\022\003int\"\034\n\025num_features_per_node\022\003int\"\022\n\013random_seed\022\003int\nl\n\017RoutingFunction\022\016\n\ninput_data\030\001\022\023\n\017tree_parameters\030\001\022\017\n\013tree_biases\030\001\032\021\n\rprobabilities\030\001\"\020\n\tmax_nodes\022\003int\n{\n\017RoutingGradient\022\016\n\ninput_data\030\001\022\023\n\017tree_parameters\030\001\022\017\n\013tree_biases\030\001\022\n\n\006routes\030\001\032\024\n\020routing_gradient\030\001\"\020\n\tmax_nodes\022\003int\n\234\001\n\035StochasticHardRoutingFunction\022\016\n\ninput_data\030\001\022\023\n\017tree_parameters\030\001\022\017\n\013tree_biases\030\001\032\024\n\020path_probability\030\001\032\010\n\004path\030\003\"\021\n\ntree_depth\022\003int\"\022\n\013random_seed\022\003int\n\334\001\n\035StochasticHardRoutingGradient\022\016\n\ninput_data\030\001\022\023\n\017tree_parameters\030\001\022\017\n\013tree_biases\030\001\022\024\n\020path_probability\030\001\022\010\n\004path\030\003\032\024\n\020routing_gradient\030\001\032\021\n\rdata_gradient\030\001\032\026\n\022parameter_gradient\030\001\032\021\n\rbias_gradient\030\001\"\021\n\ntree_depth\022\003int\n:\n\nUnpackPath\022\010\n\004path\030\003\022\017\n\013path_values\030\001\032\021\n\runpacked_path\030\001")
