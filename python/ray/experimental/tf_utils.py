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


def tf_differentiable(method):
    """Decorator for TensorFlow differentiable actor methods.

    Args:
        method (function): The actor method.

    Returns:
        function: The differentiable actor method wrapper.

    """
    def differentiable_method(self, identifier, forward_pass, 
                              persistent_tape, dys, dys_dtype,
                              kwarg_types, kwarg_dtypes, kws,
                              arg_types, arg_dtypes, *args):
        """TensorFlow differentiable actor method wrapper.

        Args:
            identifier (str): The unique byte-string identifier for this method.
            forward_pass (bool): Executing the forward pass.
            persistent_tape (bool): Use a persistent tf.GradientTape.
            dys (list): The partial downstream gradients.
            dys_dtype (list): The data types of partial downstream gradients.
            kwarg_types (list): The types of kwargs.
            kwarg_dtypes (list): The data types of kwargs.
            kws (list): The keywords.
            arg_types (list): The types of args.
            arg_dtypes (list): The data types of args.
            args: The combined args and kwargs. 

        Note:
            When `forward_pass` is `True`, `dys` and `dys_dtype` should be None.
            When `forward_pass` is `False`, `kwarg_types`, `kwarg_dtypes`, `kws`,
            `arg_types` and `arg_dtypes` should be None. No `*args` should be provided.

            Keyword arguments should be appended to the end of `*args`.

        Returns:
            Result of actor method on forward pass and gradients on backward pass. 

        """
        if not tf.executing_eagerly():
            raise RuntimeError("Tensorflow Eager execution must "
                               "be enabled for differentiable " 
                               "functions.")

        if forward_pass:
            assert dys is None and dys_dtype is None, ("During forward pass, dys and dys_dtype "
                                                       "must be None but are: {} and {}, respectively.".format(dys, dys_dtype))

            # we pass in kwargs as args because we want them
            # to be fetched before evaluation on the remote Actor, which
            # won't happen if they are passed through a dict
            if len(kws) > 0:
                kwargs = args[-len(kws):]
                args = args[:-len(kws)]
            else:
                kwargs = {}

            # validate args and kwargs
            assert len(args) == len(arg_types) == len(arg_dtypes), ("Must have same number of arguments," 
                                                                    "types, and dtypes:\n{}\n{}\n{}".format(
                                                                    args, arg_types, arg_dtypes))

            assert len(kwargs) == len(kwarg_types) == len(kwarg_dtypes), ("Must have same number of keyword arguments," 
                                                                          "types, and dtypes:\n{}\n{}\n{}".format(
                                                                          kwargs, kwarg_types, kwarg_dtypes))

            tf_constants = [] # these must be explicitly watched under the scope of tf.GradientTape
            tensors = [] 

            # process the args
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

            # process the kwargs
            processed_kwargs = {}
            for arg_type, arg_dtype, arg, kw in zip(kwarg_types, kwarg_dtypes, kwargs, kws):
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

            # execute internal method
            with tf.GradientTape(persistent=persistent_tape) as tape:
                # watch all of the constants 
                for constant in tf_constants:
                    tape.watch(constant)

                results = method(self, *processed_args, **processed_kwargs)

            # cache the TF state for backward pass
            if not hasattr(self, "__ray_tf_info__"):
                self.__ray_tf_info__ = {}
            self.__ray_tf_info__[identifier] = (tape, tensors, results)

            # process the results
            if isinstance(results, (list, tuple)):
                processed_results = [result.numpy() if isinstance(result, (tf.Tensor, tf.Variable)) 
                                     else result for result in results]
            else:
                processed_results = results.numpy() if isinstance(results, (tf.Tensor, tf.Variable)) else results

            return processed_results
        else:
            assert kwarg_types is None and kwarg_dtypes is None and kws is None, ("During backwards pass, kwarg_types, kwarg_dtypes, "
                                                                                  "and kws must be None but are: "
                                                                                  "{}, {}, and {}, respectively.".format(kwarg_types, kwarg_dtypes, kws)) 

            # fetch the cached TF state
            tape, tensors, results = self.__ray_tf_info__[identifier]

            # process the partial downstream gradients
            if not isinstance(results, (list, tuple)):
                results = [results]
            dys_processed = [] 
            for dy, result in zip(dys, results):
                if isinstance(dy, np.floating) and dy == 0.0:
                    # this is needed because TF inserts 0.0 for results that 
                    # are not used downstream regardless of the proper shape
                    dys_processed.append(tf.constant(np.zeros_like(result), dtype=dys_dtype))
                else:
                    dys_processed.append(tf.constant(dy, dtype=dys_dtype))
            dys = dys_processed

            # get the gradients
            grads = tape.gradient(results, tensors, output_gradients=dys)

            if not persistent_tape:
                # we can no longer use the tape
                del self.__ray_tf_info__[identifier]

            # process the gradients
            grads = [grad.numpy() if grad is not None 
                     else grad for grad in grads]  # a gradient can be none if a 
                                                   # source we are taking the gradient w.r.t is never used
            grads = grads[0] if len(grads) == 1 else grads

            return grads

    # tag method metadata for ray
    differentiable_method.__ray_tf_differentiable__ = True

    return differentiable_method


class TFObjectID(ray.ObjectID):
    """ObjectID wrapper for TensorFlow tensor.

    Attributes:
        graph_tensor (tf.Tensor): A dummy tensor used to link the local TF graph.

    """

    def __init__(self, object_id, graph_tensor):
        """
        Args:
            object_id (ray.ObjectID): The ObjectID to wrap.
            graph_tensor (tf.Tensor): A dummy tensor used to link the local TF graph.

        """
        ray.ObjectID.__init__(self, object_id.binary())
        self.graph_tensor = graph_tensor


def _submit_tf_differentiable(actor_method, args, kwargs, num_return_vals):
    """Invoke TensorFlow differentiable actor method.

    Args:
        actor_method (ray.ActorMethod): The actor method to call.
        args (list): The args.
        kwargs (dict): The kwargs.
        num_return_vals (int): The numer of return values.

    Returns:
        TFObjectID or list of TFObjectID: The returned tensor ObjectIDs.
    """

    if not tf.executing_eagerly():
        raise RuntimeError("Tensorflow Eager execution must "
                           "be enabled for differentiable " 
                           "functions.")

    tensors = []

    # process the args for the remote call
    arg_types = []
    arg_dtypes = []
    return_dtype = None
    processed_args = []
    for arg in args:
        if isinstance(arg, TFObjectID):
            tensors.append(arg.graph_tensor)
            processed_args.append(arg)

            arg_types.append("tf_var") # we always want these to be watched
            arg_dtypes.append(arg.graph_tensor.numpy().dtype)
            return_dtype = arg_dtypes[-1]
        elif isinstance(arg, (tf.Variable, tf.Tensor)):
            tensors.append(arg)
            processed_args.append(arg.numpy())

            if isinstance(arg, tf.Variable):
                arg_type = "tf_var"
            elif isinstance(arg, tf.Tensor):
                arg_type = "tf_const"                         
            arg_types.append(arg_type)
            arg_dtypes.append(arg.numpy().dtype)
            return_dtype = arg_dtypes[-1]
        elif isinstance(arg, ray.ObjectID):
            processed_args.append(arg)
            arg_types.append("native")
            arg_dtypes.append(ray.ObjectID)
        else:
            processed_args.append(arg)
            arg_types.append("native")
            arg_dtypes.append(type(arg))

    # process the kwargs for the remote call
    kwarg_types = []
    kwarg_dtypes = []
    processed_kwargs = []
    kws = []
    for kw, arg in kwargs.items():
        kws.append(kw)
        if isinstance(arg, TFObjectID):
            tensors.append(arg.graph_tensor)
            processed_kwargs.append(arg)

            kwarg_types.append("tf_const")
            kwarg_dtypes.append(arg.graph_tensor.numpy().dtype)
            return_dtype = kwarg_dtypes[-1]
        elif isinstance(arg, (tf.Variable, tf.Tensor)):
            tensors.append(arg)
            processed_kwargs.append(arg.numpy())

            if isinstance(arg, tf.Variable):
                arg_type = "tf_var"
            elif isinstance(arg, (tf.Tensor)):
                arg_type = "tf_const"                        
            kwarg_types.append(arg_type)
            kwarg_dtypes.append(arg.numpy().dtype)
            return_dtype = kwarg_dtypes[-1]
        elif isinstance(arg, ray.ObjectID):
            processed_kwargs.append(arg)
            kwarg_types.append("native")
            kwarg_dtypes.append(ray.ObjectID)
        else:
            processed_kwargs.append(arg)
            kwarg_types.append("native")
            kwarg_dtypes.append(type(arg))

    if return_dtype is None:
        # if none of the args or kwargs are tensors
        raise RuntimeError("Differentiable functions must have "
                           "at least one tensor arg/kwarg." )


    result_tf_obj_ids = []
   
    @tf.custom_gradient
    def submit_op(*tensors):
        """Invokes the forward pass of the remote actor method.
        
        This becomes an operation in our local TF graph representing the remote actor method.

        Args:
            tensors (tf.Tensor): The tensors passed to the actor method.

        Note:
            The `tensors` argument is not actually passed to the *remote*
            actor method. Instead, it is used to link the local TF graph
            and a processed version of `tensors` is passed to the remote method.       

        Returns:
            list, function: A list of tensor outputs and a gradient function.

        Note:
            The tensors returned here are the `graph_tensor` attributes of the `TFObjectID`s returned
            from the remote actor method invocation. They are merely to link the local TF graph.

        """

        # submit the remote op
        identifier = np.random.bytes(20)
        forward = True
        persistent_tape = False #TODO:(vsatish) when would we want this to be True?
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
            graph_tensor = tf.constant(1.0, dtype=return_dtype)
            result_tf_obj_ids.append(TFObjectID(obj_id, graph_tensor))

        def grad(*dys):
            """Invokes the backwards pass of the remote actor method for gradient computation.

            Args:
                dys (list): The partial downstream gradients.

            """
            # process the partial dys
            dys = [dy.numpy() for dy in dys]

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
            grads = ray.get(grad_ids)

            return [tf.constant(grad, dtype=return_dtype) if grad is not None 
                    else grad for grad in grads]

        # extract the dummy tensors used to link the local TF graph
        results = [tf_obj_id.graph_tensor for tf_obj_id in result_tf_obj_ids]
 
        return results, grad
    
    results = submit_op(*tensors)
    for tf_obj_id, graph_tensor in zip(result_tf_obj_ids, results): 
        tf_obj_id.graph_tensor = graph_tensor #TODO(vsatish): Find a cleaner way to do this.

    return result_tf_obj_ids[0] if len(result_tf_obj_ids) == 1 else result_tf_obj_ids


def _post_process_get(tf_object_ids, results):
    """Post-process the `TFObjectID` results from ray.get().

    Right now all it does is wrap the fetched values in `tf.Tensor`s.

    Args:
        tf_object_ids (list): The list of `TFObjectID`s.
        results (list): The corresponding fetched values.

    Returns:
        list: A list of `tf.Tensor`s representing the post-processed values.

    """
    if not tf.executing_eagerly():
        raise RuntimeError("Tensorflow Eager execution must "
                           "be enabled for differentiable " 
                           "functions.")

    tensors = [tf_obj_id.graph_tensor for tf_obj_id in tf_object_ids] 


    @tf.custom_gradient
    def get_op(*tensors):
        """Post-process the results.

        This becomes an operation in our local TF graph representing the call to ray.get().

        Args:
            tensors (list): The `graph_tensor` attributes of the TFObjectIDs.

        Returns:
            list: A list of `tf.Tensor`s representing the post-processed values.

        """

        # convert the results to tensors
        result_tensors = [tf.constant(result, dtype=tensor.dtype) 
                          for result, tensor in zip(results, tensors)]

        def grad(*dys):
            """Backwards pass of ray.get() for gradient computation.

            Right now this does nothing, but in the future it can be used to start the 
            backwards pass of the actor method in advance.

            Args:
                dys (list): The partial downstream gradients.

            """
            return dys

        return result_tensors, grad

    return get_op(*tensors)
   
