from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import deque, OrderedDict
import logging
import numpy as np
import pickle

import ray
from ray.rllib.utils import try_import_tf

tf = try_import_tf()


logger = logging.getLogger(__name__)

def unflatten(vector, shapes):
    i = 0
    arrays = []
    for shape in shapes:
        size = np.prod(shape, dtype=np.int)
        array = vector[i:(i + size)].reshape(shape)
        arrays.append(array)
        i += size
    assert len(vector) == i, "Passed weight does not have the correct shape."
    return arrays


class TensorFlowVariables(object):
    """A class used to set and get weights for Tensorflow networks.

    Attributes:
        sess (tf.Session): The tensorflow session used to run assignment.
        variables (Dict[str, tf.Variable]): Extracted variables from the loss
            or additional variables that are passed in.
        placeholders (Dict[str, tf.placeholders]): Placeholders for weights.
        assignment_nodes (Dict[str, tf.Tensor]): Nodes that assign weights.
    """

    def __init__(self, output, sess=None, input_variables=None):
        """Creates TensorFlowVariables containing extracted variables.

        The variables are extracted by performing a BFS search on the
        dependency graph with loss as the root node. After the tree is
        traversed and those variables are collected, we append input_variables
        to the collected variables. For each variable in the list, the
        variable has a placeholder and assignment operation created for it.

        Args:
            output (tf.Operation, List[tf.Operation]): The tensorflow
                operation to extract all variables from.
            sess (tf.Session): Session used for running the get and set
                methods.
            input_variables (List[tf.Variables]): Variables to include in the
                list.
        """
        self.sess = sess
        if not isinstance(output, (list, tuple)):
            output = [output]
        queue = deque(output)
        variable_names = []
        explored_inputs = set(output)

        # We do a BFS on the dependency graph of the input function to find
        # the variables.
        while len(queue) != 0:
            tf_obj = queue.popleft()
            if tf_obj is None:
                continue
            # The object put into the queue is not necessarily an operation,
            # so we want the op attribute to get the operation underlying the
            # object. Only operations contain the inputs that we can explore.
            if hasattr(tf_obj, "op"):
                tf_obj = tf_obj.op
            for input_op in tf_obj.inputs:
                if input_op not in explored_inputs:
                    queue.append(input_op)
                    explored_inputs.add(input_op)
            # Tensorflow control inputs can be circular, so we keep track of
            # explored operations.
            for control in tf_obj.control_inputs:
                if control not in explored_inputs:
                    queue.append(control)
                    explored_inputs.add(control)
            if ("Variable" in tf_obj.node_def.op
                    or "VarHandle" in tf_obj.node_def.op):
                variable_names.append(tf_obj.node_def.name)
        self.variables = OrderedDict()
        variable_list = [
            v for v in tf.global_variables()
            if v.op.node_def.name in variable_names
        ]
        if input_variables is not None:
            variable_list += input_variables
        for v in variable_list:
            self.variables[v.op.node_def.name] = v

        self.placeholders = {}
        self.assignment_nodes = {}

        # Create new placeholders to put in custom weights.
        for k, var in self.variables.items():
            self.placeholders[k] = tf.placeholder(
                var.value().dtype,
                var.get_shape().as_list(),
                name="Placeholder_" + k)
            self.assignment_nodes[k] = var.assign(self.placeholders[k])

    def set_session(self, sess):
        """Sets the current session used by the class.

        Args:
            sess (tf.Session): Session to set the attribute with.
        """
        self.sess = sess

    def get_flat_size(self):
        """Returns the total length of all of the flattened variables.

        Returns:
            The length of all flattened variables concatenated.
        """
        return sum(
            np.prod(v.get_shape().as_list()) for v in self.variables.values())

    def _check_sess(self):
        """Checks if the session is set, and if not throw an error message."""
        assert self.sess is not None, ("The session is not set. Set the "
                                       "session either by passing it into the "
                                       "TensorFlowVariables constructor or by "
                                       "calling set_session(sess).")

    def get_flat(self):
        """Gets the weights and returns them as a flat array.

        Returns:
            1D Array containing the flattened weights.
        """
        self._check_sess()
        return np.concatenate([
            v.eval(session=self.sess).flatten()
            for v in self.variables.values()
        ])

    def set_flat(self, new_weights):
        """Sets the weights to new_weights, converting from a flat array.

        Note:
            You can only set all weights in the network using this function,
            i.e., the length of the array must match get_flat_size.

        Args:
            new_weights (np.ndarray): Flat array containing weights.
        """
        self._check_sess()
        shapes = [v.get_shape().as_list() for v in self.variables.values()]
        arrays = unflatten(new_weights, shapes)
        placeholders = [
            self.placeholders[k] for k, v in self.variables.items()
        ]
        self.sess.run(
            list(self.assignment_nodes.values()),
            feed_dict=dict(zip(placeholders, arrays)))

    def get_weights(self):
        """Returns a dictionary containing the weights of the network.

        Returns:
            Dictionary mapping variable names to their weights.
        """
        self._check_sess()
        return {
            k: v.eval(session=self.sess)
            for k, v in self.variables.items()
        }

    def set_weights(self, new_weights):
        """Sets the weights to new_weights.

        Note:
            Can set subsets of variables as well, by only passing in the
            variables you want to be set.

        Args:
            new_weights (Dict): Dictionary mapping variable names to their
                weights.
        """
        self._check_sess()
        assign_list = [
            self.assignment_nodes[name] for name in new_weights.keys()
            if name in self.assignment_nodes
        ]
        assert assign_list, ("No variables in the input matched those in the "
                             "network. Possible cause: Two networks were "
                             "defined in the same TensorFlow graph. To fix "
                             "this, place each network definition in its own "
                             "tf.Graph.")
        self.sess.run(
            assign_list,
            feed_dict={
                self.placeholders[name]: value
                for (name, value) in new_weights.items()
                if name in self.placeholders
            })


def tf_differentiable(num_return_vals):
    def tf_differentiable_helper(method):
        #TODO(vsatish): Can we automatically determine 
        #that a method is differentiable at runtime based on inputs?

        def differentiable_method(self, identifier, forward_pass, 
                                  persistent_tape, dys, dys_dtype,
                                  kwarg_types, kwarg_dtypes, kws,
                                  arg_types, arg_dtypes, *args):

            if not tf.executing_eagerly():
                #TODO(vsatish): Can we automatically 
                #enable this? (Tricky b/c it must be done at start of program.)
                raise RuntimeError("Tensorflow Eager execution must "
                                   "be enabled for experimental differentiable ray " 
                                   "functionality.")

            if forward_pass:
                # execute the forward pass

                if len(kws) > 0: #TODO(vsatish): Clean this up.
                    kwargs = dict(zip(kws, args[-len(kws):]))
                    args = args[:-len(kws)]
                else:
                    kwargs = {}


                # revert the arguments back to their original types
                assert len(arg_types) == len(args), ("Argument types: {} do not directly map " 
                                                     "to arguments: {}.".format(arg_types, args))

#                assert isinstance(kwarg_types, dict), ("Keyword argument types must be defined with a dict.")
#                assert len(kwarg_types) == len(kwargs), ("Keyword argument types: {} do not directly map " 
#                                                         "to keyword arguments: {}.".format(kwarg_types, kwargs))

                tf_constants = [] # these must be explicitly watched under the scope of tf.GradientTape
                tensors = [] 

                processed_args = []
                for arg_type, arg_dtype, arg in zip(arg_types, arg_dtypes, args):
                    if arg_type == "tf_var":
                        processed_args.append(tf.Variable(arg, dtype=arg_dtype))
                        tensors.append(processed_args[-1])
                    elif arg_type == "tf_const":
                        processed_args.append(tf.constant(arg, dtype=arg_dtype))
                        tf_constants.append(processed_args[-1])
                        tensors.append(processed_args[-1])
                    elif arg_type == "native":
                        processed_args.append(arg)
                    else:
                        raise ValueError("Invalid argument type: {}".format(arg_type))

                processed_kwargs = {}
                for kw, arg in kwargs.items():
                    arg_type = kwarg_types[kw]
                    arg_dtype = kwarg_dtypes[kw]
                    #TODO (vsatish): Does it even make sense to have keyword tensors?
                    # We definitely shouldn't allow them to be differentiable, right?
                    if arg_type == "tf_var":
                        processed_kwargs[kw] = tf.Variable(arg, dtype=arg_dtype)
                        tensors.append(processed_kwargs[kw])
                    elif arg_type == "tf_const":
                        processed_kwargs[kw] = tf.constant(arg, dtype=arg_dtype)
                        tf_constants.append(processed_kwargs[kw])
                        tensors.append(processed_kwargs[kw])
                    elif arg_type == "native":
                        processed_kwargs[kw] = arg
                    else:
                        raise ValueError("Invalid keyword argument type: {}".format(arg_type))

                # execute raw method with tf.GradientTape
                with tf.GradientTape(persistent=persistent_tape) as tape:
                    # watch all of the constants 
                    for constant in tf_constants:
                        tape.watch(constant)

                    results = method(self, *processed_args, **processed_kwargs)

                # cache the tf state for backward pass
                if not hasattr(self, "__ray_tf_info__"):
                    self.__ray_tf_info__ = {}
                self.__ray_tf_info__[identifier] = (tape, tensors, results)

                # process the results
                if isinstance(results, (list, tuple)):
                    processed_results = [result.numpy() if isinstance(result, (tf.Tensor, tf.Variable)) else result for result in results]
                else:
                    processed_results = results.numpy() if isinstance(results, (tf.Tensor, tf.Variable)) else results

                return processed_results
            else:
                # execute the backward pass
                tape, tensors, results = self.__ray_tf_info__[identifier]

                dys_processed = [] 
                if not isinstance(results, (list, tuple)):
                    results = [results]
                for dy, result in zip(dys, results):
                    if isinstance(dy, np.floating) and dy == 0.0:
                            dys_processed.append(tf.constant(np.zeros_like(result), dtype=dys_dtype))
                    else:
                        dys_processed.append(tf.constant(dy, dtype=dys_dtype))
                dys = dys_processed

                grads = tape.gradient(results, tensors, output_gradients=dys)

                if not persistent_tape:
                    # we can no longer use the tape
                    del self.__ray_tf_info__[identifier]

                grads = [grad.numpy() if grad is not None 
                         else grad for grad in grads]  # a gradient can be none if a 
                                                       # source we are taking the gradient w.r.t is never used
                return grads[0] if len(grads) == 1 else grads

        differentiable_method.__ray_tf_differentiable__ = True
        differentiable_method.__ray_num_return_vals__ = num_return_vals

        return differentiable_method
    return tf_differentiable_helper

"""
def _info_to_tensor(obj):
    tensor = tf.constant(
        np.array([c for c in pickle.dumps(obj)]).astype(np.float64))
    return tensor


def _tensor_to_info(tensor):
    array = tensor.numpy()
    assert array.ndim == 1
    size = array.size
    b = bytearray(size)
    for i in range(size):
        b[i] = int(array[i])
    return pickle.loads(bytes(b))
"""

class TFObjectID(ray.ObjectID):
    def __init__(self, object_id, graph_tensor, identifier,
                 actor_method, num_tensor_inputs, persistent_tape=False):
        # graph_tensor is a dummy tensor used to link the TF graph

        ray.ObjectID.__init__(self, object_id.binary())
        self._persistent_tape = persistent_tape
        self._identifier = identifier
        self._actor_method = actor_method
        self._num_tensor_inputs = num_tensor_inputs

        self.graph_tensor = graph_tensor

    """
    def backward(self, dys):
        forward = False
        grad_ids = self._actor_method._internal_remote(
            args=[self._identifier, forward, 
                  self._persistent_tape, dys, None, None],
            kwargs={},
            num_return_vals=self._num_tensor_inputs)
        return _info_to_tensor([grad_ids] if isinstance(grad_ids, ray.ObjectID) else grad_ids)
    """


def _submit_tf(actor_method, args, kwargs, num_return_vals):
    if not tf.executing_eagerly():
        #TODO(vsatish): Can we automatically 
        #enable this? (Tricky b/c it must be done at start of program.)
        raise RuntimeError("Tensorflow Eager execution must "
                           "be enabled for experimental differentiable ray " 
                           "functionality.")

   # parse the arguments and convert them to serializable types if required
#    object_ids = []
#    tf_object_ids = []
    tensors = [] # these will be used solely for the TF graph and may contain dummy tensors
#    is_tensor_tf_object_id = [] # was an input tensor a TFObjectID or a TF Tensor

    arg_types = []
    arg_dtypes = []
    return_dtype = None
    processed_args = []
    for arg in args:
        if isinstance(arg, TFObjectID):
#            tf_object_ids.append(arg)        
#            object_ids.append(arg)
            tensors.append(arg.graph_tensor) # this is a dummy tensor used to link the TF computation graph
            arg_types.append("tf_var") # we always want these to be watched
            arg_dtypes.append(arg.graph_tensor.numpy().dtype)
#            is_tensor_tf_object_id.append(True)
            processed_args.append(arg)
            return_dtype = arg_dtypes[-1]
        elif isinstance(arg, (tf.Variable, tf.Tensor)):
            if isinstance(arg, tf.Variable):
                arg_type = "tf_var"
            elif isinstance(arg, tf.Tensor):
                arg_type = "tf_const"                        
#            is_tensor_tf_object_id.append(False)
            arg_types.append(arg_type)
            arg_dtypes.append(arg.numpy().dtype)
            tensors.append(arg)
            processed_args.append(arg.numpy())
            return_dtype = arg_dtypes[-1]
        elif isinstance(arg, ray.ObjectID):
#            object_ids.append(arg)
            arg_types.append("native")
            arg_dtypes.append(ray.ObjectID)
            processed_args.append(arg)
        else:
            arg_types.append("native")
            arg_dtypes.append(type(arg))
            processed_args.append(arg)

    kwarg_types = {}
    kwarg_dtypes = {}
    processed_kwargs = []
    kws = []
    for kw, arg in kwargs.items():
        if isinstance(arg, TFObjectID):
#            tf_object_ids.append(arg)        
#            object_ids.append(arg)
            tensors.append(arg.graph_tensor) # this is a dummy tensor used to link the TF computation graph
            kwarg_types[kw] = "tf_const"
            kwarg_dtypes[kw] = arg.graph_tensor.numpy().dtype
#            is_tensor_tf_object_id.append(True)
            processed_kwargs.append(arg)
            kws.append(kw)
#            raise RuntimeError("TFObjectID kwargs are not yet supported!")
        elif isinstance(arg, (tf.Variable, tf.Tensor)):
            if isinstance(arg, tf.Variable):
                arg_type = "tf_var"
            elif isinstance(arg, (tf.Tensor)):
                arg_type = "tf_const"                        
#            is_tensor_tf_object_id.append(False)
            kwarg_types[kw] = arg_type
            kwarg_dtypes[kw] = arg.numpy().dtype
            tensors.append(arg)
            processed_kwargs.append(arg.numpy())
            kws.append(kw)
        elif isinstance(arg, ray.ObjectID):
#            object_ids.append(arg)
            kwarg_types[kw] = "native"
            kwarg_dtypes[kw] = ray.ObjectID
            processed_kwargs.append(arg)
            kws.append(kw)
#            raise RuntimeError("TFObjectID kwargs are not yet supported!")
        else:
            kwarg_types[kw] = "native"
            kwarg_dtypes[kw] = type(arg)
            processed_kwargs.append(arg)
            kws.append(kw)

    print("SUBMIT_OP")
    print(kws, processed_kwargs, processed_args)

#    assert len(tensors) == len(is_tensor_tf_object_id)
#    assert len(tf_object_ids) == sum(is_tensor_tf_object_id)

    result_tf_obj_ids = []
   
    @tf.custom_gradient
    def submit_op(*tensors):
        #NOTE: the tensors passed in here are merely to 
        #link the TF computation graph, but are never actually used

        # submit the remote op
        identifier = np.random.bytes(20)
        forward = True
        persistent_tape = True #TODO:(vsatish) when would we want this to be True?
        dy, dys_type = None, None
        result_obj_ids = actor_method._internal_remote([identifier, forward, 
                                                        persistent_tape, dy, dys_type,
                                                        kwarg_types, kwarg_dtypes, kws, 
                                                        arg_types, arg_dtypes, *(processed_args+processed_kwargs)],
                                                        {}, 
                                                        num_return_vals)
       
        if isinstance(result_obj_ids, ray.ObjectID):
            result_obj_ids = [result_obj_ids]
 
        for obj_id in result_obj_ids:
            graph_tensor = tf.constant(1.0, dtype=return_dtype) # this is a dummy tensor used to link the TF computation graph
            result_tf_obj_ids.append(TFObjectID(obj_id, graph_tensor, 
                                                identifier, actor_method, 
                                                len(tensors), 
                                                persistent_tape=persistent_tape))

        def grad(*dys):
            dys = [dy.numpy() for dy in dys]
#            dy_object_ids = [_tensor_to_info(dy) for dy in dys]

#            assert len(dy_object_ids) == len(result_tf_object_ids), ("Must have one-to-one mapping between "
#                                                                     "downstream gradients and fn outputs.")
#            assert all([isinstance(object_id, ray.ObjectID) for object_id in object_ids]), ("All downstream gradients "
#                                                                                            "must be ray ObjectIDs")

            # compute gradients
            forward = False
            dys_dtype = return_dtype
            kwarg_types, kwarg_dtypes, kws = None, None, None
            arg_types, arg_dtypes = None, None
            num_return_vals = len(tensors)
            grad_ids = actor_method._internal_remote([identifier, forward, 
                                                      persistent_tape, dys, dys_dtype,
                                                      kwarg_types, kwarg_dtypes, kws,
                                                      arg_types, arg_dtypes],
                                                      {},
                                                      num_return_vals)

            if isinstance(grad_ids, ray.ObjectID):
                grad_ids = [grad_ids]
            grads = ray.get(grad_ids) # DESTROYS PARALLELISM!!!

            return [tf.constant(grad, dtype=return_dtype) if grad is not None 
                    else grad for grad in grads]

        results = [tf_obj_id.graph_tensor for tf_obj_id in result_tf_obj_ids] # this is a dummy tensor used 
                                                                              # to link the TF computation graph
        return results, grad
    
    results = submit_op(*tensors)
    for tf_obj_id, graph_tensor in zip(result_tf_obj_ids, results): 
        tf_obj_id.graph_tensor = graph_tensor #TODO:(vsatish) find cleaner way to do this

    return result_tf_obj_ids[0] if len(result_tf_obj_ids) == 1 else result_tf_obj_ids

def _post_process_get(tf_object_ids, results):
    if not tf.executing_eagerly():
        #TODO(vsatish): Can we automatically 
        #enable this? (Tricky b/c it must be done at start of program.)
        raise RuntimeError("Tensorflow Eager execution must "
                           "be enabled for experimental differentiable ray " 
                           "functionality.")

    tensors = [tf_obj_id.graph_tensor for tf_obj_id in tf_object_ids] # these are dummy tensors used 
                                                                      # to link the TF computation graph
    print("TENSORS", tensors)
    print("RESULTS", results)

    @tf.custom_gradient
    def get_op(*tensors):

        """        
        result_tensors = []
        for result, tensor in zip(results, tensors):
            if result != None: 
                result_tensors.append(tf.constant(result, dtype=tensor.dtype))
            else:
                result_tensors.append(-1)        
        """
        result_tensors = [tf.constant(result, dtype=tensor.dtype) for result, tensor in zip(results, tensors)]

        def grad(*dys):
            return dys

        return result_tensors, grad

    return get_op(*tensors)
   
