from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import deque, OrderedDict
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


class ValType:
    """Enum to hold types of packed values for reconstruction after
       serialization.
    """
    TF_VAR = "tf_variable" # `tf.Variable`
    TF_TEN = "tf_tensor" # `tf.Tensor`; note that `tf.constant`
                         # is `tf.Tensor` under the hood.
    TF_OBJ_ID = "tf_object_id" # Placeholder for un-fetched results
                               # whose real type is undetermined.
    OTHER = "other" # Bucket to hold others. 

    # We need to handle these in a special manner because Ray will 
    # convert scalar NumPy values to native Python `float`s during 
    # transport, losing the original dtype.
    TF_VAR_SC = "tf_variable_scalar" # `tf.Variable` scalar.
    TF_TEN_SC = "tf_tensor_scalar" # `tf.Tensor` scalar.
    NP_SC = "np_scalar" # NumPy scalar.


def _pack(val):
    """Packs a value so that it can be serialized and reconstructed.
    
    Args:
        val (): The value to pack.

    Returns:
        tuple: `(packed, ptype)` where `ptype` is one of the types
            defined in `ValType` and `packed` is the value prepared for 
            serialization.
    """
    packed = None
    ptype = None   
    if isinstance(val, TFObjectID):
        # This is a placeholder for a value returned from a differentiable
        # remote actor method because we won't know the actual type until it is
        # fetched. When it is fetched, we will get a tuple 
        # `(packed, ptype)` that can be reconstructed by calling 
        # `_unpack`.
        packed = val
        ptype = ValType.TF_OBJ_ID
    elif isinstance(val, (tf.Variable, tf.Tensor)):
        # These will be converted to their NumPy equivalent for serialization.
        packed = val.numpy()
        scalar = False
        if np.isscalar(packed):
            scalar = True
            packed = np.array([packed])

        # We take special care for NumPy scalars because they will not be 
        # transported correctly, but instead converted to a native Python 
        # `float` by Ray. To circumvent this, we transport them as 1d arrays
        # and unpack them accordingly. 
        if isinstance(val, tf.Variable) and scalar:
            ptype = ValType.TF_VAR_SC
        elif isinstance(val, tf.Variable):
            ptype = ValType.TF_VAR
        elif isinstance(val, tf.Tensor) and scalar:
            ptype = ValType.TF_TEN_SC
        elif isinstance(val, tf.Tensor):
            ptype = ValType.TF_TEN
    elif type(val).__module__ == np.__name__ and np.isscalar(val):
        # These are handled similar to above.
        packed = np.array([val])
        ptype = ValType.NP_SC
    else:
        # We will assume these are serializable. If not, Ray will throw
        # an exception down the road
        packed = val
        ptype = ValType.OTHER
    return packed, ptype


def _unpack(packed, ptype):
    """Reconstruct a packed value.

    Args:
        packed (): The packed value.
        ptype (ValType): The type before packing.

    Note:
        See `_pack` for more detials.

    Returns:
        : The unpacked value.
    """
    if ptype == ValType.TF_VAR:
        return tf.Variable(packed)
    elif ptype == ValType.TF_TEN:
        return tf.constant(packed)
    elif ptype == ValType.TF_VAR_SC:
        return tf.Variable(packed[0])
    elif ptype == ValType.TF_TEN_SC:
        return tf.constant(packed[0])
    elif ptype == ValType.OTHER:
        return packed
    else:
        raise ValueError("Invalid type to unpack: {}".format(ptype))


def _pack_many(vals):
    """Packs a batch of values for serialization and reconstruction.

    Iteratively applies `_pack`.

    Args:
        vals (list): The values to pack.

    Returns:
        tuple: `(packed, types)` where `packed` is a list of the packed values
            and `types` is a list of the corresponding `ValType`s for
            reconstruction
    """
    return tuple(zip(*list(map(_pack, vals))))

def _unpack_many(packed, ptypes):
    """Reconstructs a batch of packed values.

    Iteratively applies `_unpack`.

    Args:
        packed (list): The packed values.
        ptypes (list): The corresponding `ValType`s.

    Returns:
        list: The reconstructed values. 
    """
    return list(map(lambda x: _unpack(*x), zip(packed, ptypes))) 


def _pack_args(args, kwargs):
    """Packs arguments for serialization and reconstruction on the
       remote actor.
    
    Args:
        args (list): The arguments.
        kwargs (dict): The keyword arguments.

    Returns:
        tuple: A tuple of packed arguments and metadata,
            `(packed_args, arg_ptypes,
              packed_kwargs, kwarg_ptypes, kws,
              tf_args)` where:

            - `packed_args`: The packed arguments.
            - `arg_ptypes`: A corresponding list of `ValType`s for 
                reconstruction.
            - `packed_kwargs`: The packed *arguments only* of the keyword
                arguments. 
            - `kwarg_ptypes`:  A corresponding list of `ValType`s for 
                reconstruction.
            - `kws`: A list of *only the keywords* of the keyword arguments.
            - `tf_args`: A list of all the arguments and keyword arguments
                that are `tf.Variable` or `tf.Tensor`. It will be used solely
                to connect the local TF graph and never be used in an actual
                computation.
          
        The reason we split the keyword arguments into arguments and keywords is
        that if we were to serialize and send a `dict` to the remote actor,
        the `ray.ObjectID`s would not get fetched before the actor method
        computation. By splitting it up, we can transport the arguments as
        if they were normal arguments and stitch them with the keywords inside
        the actor method.
    """
    tf_args = [] # These are all the arguments and keyword arguments
                 # that are `tf.Variable` or `tf.Tensor`. It will be used solely
                 # to connect the local TF graph and never be used in an actual
                 # computation.

    # Pack args.
    packed_args = []
    arg_ptypes = []
    for arg in args:
        if isinstance(arg, TFObjectID):
            tf_args.append(arg.graph_tensor)
        elif isinstance(arg, (tf.Variable, tf.Tensor)):
            tf_args.append(arg)

        packed_arg, arg_ptype = _pack(arg)
        packed_args.append(packed_arg)
        arg_ptypes.append(arg_ptype)

    # Pack kwargs.
    kws = []
    packed_kwargs = []
    kwarg_ptypes = []
    for kw, arg in kwargs.items():
        kws.append(kw)
        if isinstance(arg, TFObjectID):
            tf_args.append(arg.graph_tensor)
        elif isinstance(arg, (tf.Variable, tf.Tensor)):
            tf_args.append(arg)

        packed_arg, arg_ptype = _pack(arg)
        packed_kwargs.append(packed_arg)
        kwarg_ptypes.append(arg_ptype)

    return (packed_args, arg_ptypes,
            packed_kwargs, kwarg_ptypes, kws,
            tf_args)


def _unpack_args(packed_args, arg_ptypes,
                 packed_kwargs, kwarg_ptypes, kws):
    """Reconstruct the packed arguments on the driver.
    
    Args:
        packed_args (list): The packed arguments.
        arg_ptypes (list): The corresponding `ValType`s for reconstruction.
        packed_kwargs (list): The packed arguments *(w/o the keywords)* of
            the keyword arguments. Must be ordered relative to `kws`.
        kwarg_ptypes (list): The corresponding `ValType`s for reconstruction.
        kws (list): The keywords for the keyword arguments of the actor method.
            Must match up with `packed_kwargs`.

        For a more detailed explanation of these, see 
        the docstring of `_pack_args`.

    Returns:
        tuple: A tuple of unpacked arguments consisting of
            `(args, kwargs, tf_args, tf_tensor_args)` where:
        
            - `args`: The unpacked arguments.
            - `kwargs`: The unpacked keyword arguments as a `dict`.
            - `tf_args`: The arguments and keyword arguments that are either 
                `tf.Variable` or `tf.Tensor`. This will be used for the 
                backwards pass.
            - `tf_tensor_args`: The arguments and keyword arguments that are
                `tf.Tensor`. They must be explicitly watched by the 
                `tf.GradientTape` during the forward pass in order to take a 
                gradient w.r.t them during the backward pass.
    """
    tf_args = [] # The arguments and keyword arguments that are either 
                 # `tf.Variable` or `tf.Tensor`. This will be used for the 
                 # backwards pass.    

    tf_tensor_args = [] # The arguments and keyword arguments that are
                        # `tf.Tensor`. They must be explicitly watched by the 
                        # `tf.GradientTape` during the forward pass in order 
                        # to take a gradient w.r.t them during the backward 
                        # pass.    

    # Unpack the args.
    args = []
    for packed_arg, arg_ptype in zip(packed_args, arg_ptypes):
        if arg_ptype == ValType.TF_OBJ_ID:
            packed_arg, arg_ptype = packed_arg # These are specially packed like this
                                               # because we won't know the actual type
                                               # until it is fetched.
        args.append(_unpack(packed_arg, arg_ptype))
        if arg_ptype in (ValType.TF_TEN, ValType.TF_TEN_SC):
            tf_args.append(args[-1])
            tf_tensor_args.append(args[-1])
        elif arg_ptype in (ValType.TF_VAR, ValType.TF_VAR_SC):
            tf_args.append(args[-1])

    # Unpack the kwargs.
    kwargs = {}
    for packed_arg, arg_ptype, kw in zip(packed_kwargs, kwarg_ptypes, kws):
        if arg_ptype == ValType.TF_OBJ_ID:
            packed_arg, arg_ptype = packed_arg # These are specially packed like this
                                               # because we won't know the actual type
                                               # until it is fetched.
        kwargs[kw] = _unpack(packed_arg, arg_ptype)
        if arg_ptype in (ValType.TF_TEN, ValType.TF_TEN_SC):
            tf_args.append(kwargs[kw])
            tf_tensor_args.append(kwargs[kw])
        elif arg_ptype in (ValType.TF_VAR, ValType.TF_VAR_SC):
            tf_args.append(kwargs[kw])

    return (args, kwargs, 
            tf_args, tf_tensor_args)


def _pack_pairwise(vals):
    """Pack the values in a pair-wise fashion for serialization and 
       reconstruction on the driver.

    This is handy for when we want to build a list of elements `(packed, ptype)`
    instead of building two lists of `packed` and `ptypes`. The former plays
    well with the results and gradients returned from the remote actor method
    because we don't have to change the number of return values to accomodate
    and extra one for `ptypes`.

    Args:
        vals (list): The values to pack.
    
    Returns:
        list: A list of `(packed, ptype)`. See `_pack` for 
            more details.
    """
    packed, ptypes = _pack_many(vals)

    pairwise_packed = list(zip(packed, ptypes))
    return pairwise_packed


def _differentiable_forward(self, identifier, method, persistent_tape, 
                            packed_args, arg_ptypes, 
                            packed_kwargs, kwarg_ptypes, kws):
    """Forward pass of Tensorflow differentiable actor method.

    Args:
        self (ray.Actor): The ray Actor invoking this method.
        identifier (str): The unique byte-string used to associate this
            forward pass with the corresponding backward pass.
        method (function): The Python method to internally invoke.
        persistent_tape (bool): Use a persistent `tf.GradientTape`.
        packed_args (list): The packed arguments for the actor method.
        arg_ptypes (list): The corresponding `ValType`s for reconstruction.
        packed_kwargs (list): The packed arguments *(w/o the keywords)* of
            the keyword arguments for the actor method. Must be ordered 
            relative to `kws`.
        kwarg_ptypes (list): The corresponding `ValType`s for reconstruction.
        kws (list): The keywords for the keyword arguments of the actor method.
            Must match up with `packed_kwargs`.

    Note:
        See `_pack_args` for details on `packed_*` and `*_ptypes` and why we 
        split the keyword arguments into the keywords and arguments.

    Returns:
        list: The packed result of invoking the actor method on the unpacked 
            arguments and keyword arguments. See `_pack_pairwise` for more
            details.
    """
    # Unpack arguments.
    (args, kwargs, tf_args, tf_tensor_args) = _unpack_args(packed_args, arg_ptypes,
                                                           packed_kwargs, kwarg_ptypes, kws)
 
    # Invoke actor method.
    with tf.GradientTape(persistent=persistent_tape) as tape:
        # watch all of the `tf.Tensor` args
        for tensor in tf_tensor_args:
            tape.watch(tensor)

        results = method(self, *args, **kwargs)

    if not isinstance(results, tuple):
        results = [results]
#TODO: FIX THIS!!!
#    elif self.__ray_num_return_vals__ == 1:
#        results = [results]

    # Cache the TF state for backward pass.
    if not hasattr(self, "__ray_tf_info__"):
        self.__ray_tf_info__ = {}
    self.__ray_tf_info__[identifier] = (tape, tf_args, results)

    # Pack the results.
    packed_results = _pack_pairwise(results)
    if len(packed_results) == 1:
        packed_results = packed_results[0]

    return packed_results


def _differentiable_backward(self, identifier, persistent_tape, 
                             packed_dys, dys_ptypes):
    """Backward pass of Tensorflow differentiable actor method.

    Args:
        self (ray.Actor): The ray Actor invoking this method.
        identifier (str): The unique byte-string used to associate
                          this backward pass with the corresponding
                          forward pass.
        persistent_tape (bool): Whether or not to delete the `tf.GradientTape`
                                tape after using it.
        packed_dys (list): The packed partial downstream gradients.
        dys_ptypes (list): The corresponding `ValType`s for reconstruction.

    Note:
        See `_pack_many` for details on `packed_dys` and `dys_ptypes`

    Returns:
        list: The packed gradients. See `_pack_pairwise` for more details.
    """
    # Unpack the dys.
    dys = _unpack_many(packed_dys, dys_ptypes)

    # Fetch the cached TF state.
    tape, inputs, results = self.__ray_tf_info__[identifier]

    # Prune the cached results and dys so that we only have the TF results
    # and the corresponding dys.
    tf_results = []
    pruned_dys = []
    for result, dy in zip(results, dys):
        if isinstance(result, (tf.Tensor, tf.Variable)):
            tf_results.append(result)
            if isinstance(dy, (tf.Tensor, tf.Variable)) and np.isscalar(dy.numpy()) and dy.numpy() == 0.0:
                # This is needed because TF inserts `0.0` for results that
                # are not used downstream regardless of the proper shape.
                pruned_dys.append(tf.constant(np.zeros_like(result), dtype=result.dtype)) # We don't use `dy.dtype` b/c TF will default to tf.float32, which might not be correct.
            else:
                pruned_dys.append(dy)

    # Compute gradients.
    grads = tape.gradient(tf_results, inputs, output_gradients=pruned_dys)

    if not persistent_tape:
        # We can no longer use the tape.
        del self.__ray_tf_info__[identifier]

    # Pack the gradients.
    packed_grads = _pack_pairwise(grads)

    return packed_grads


def tf_differentiable(method):
    """Decorator for TensorFlow differentiable actor methods.

    Args:
        method (function): The actor method.

    Returns:
        function: The differentiable actor method wrapper.

    """

    def differentiable_method(self, identifier, forward_pass, persistent_tape,
                              packed_dys, dys_ptypes, kwarg_ptypes, kws,
                              arg_ptypes, *packed_args):
        """TensorFlow differentiable actor method wrapper.

        Args:
            self (ray.Actor): The ray Actor invoking this method.
            identifier (str): The unique byte-string used to associate the
                              forward and backward passes of this invocation.
            forward_pass (bool): Executing the forward pass.
            persistent_tape (bool): Use a persistent tf.GradientTape.
            packed_dys (list): The packed partial downstream gradients.
            dys_ptypes (list): The corresponding `ValType`s for reconstruction.
            kwarg_ptypes (list): The `ValType`s for the keyword arguments 
                for reconstruction.
            kws (list): The keywords for the keyword arguments.
            arg_ptypes (list): The `ValType`s for the arguments 
                for reconstruction.
            packed_args: The combined packed args *and kwargs*.

        Note:
            When `forward_pass` is `True`, `dys` and `dys_ptypes`
            should be `None`.
            When `forward_pass` is `False`, `kwarg_ptypes`,
            `kws`, and `arg_ptypes` should be `None`. No `packed_args`
            should be provided.

            The packed arguments *(w/o the keywords)* of the keyword arguments 
            should be appended to the end of `packed_args`. See the note in the
            docstring for `pack_args` for why we do this.

            See the docstrings for `_differentiable_forward` and
            `_differentiable_backward` for more details on the forward
            and backward passes.

        Returns:
            list: Packed result of invoking actor method on forward pass 
                and gradients on backward pass. See `_pack_pairwise` for more
                details.
        """
        if not tf.executing_eagerly():
            raise RuntimeError("Tensorflow Eager execution must "
                               "be enabled for differentiable "
                               "functions.")

        if forward_pass:
            assert packed_dys is None and dys_ptypes is None, (
                "During forward pass, dys and dys_ptypes "
                "must be None but are: {} and {}, respectively.".format(
                    packed_dys, dys_ptypes))

            # We pass in kwargs as args because we want them
            # to be fetched before evaluation on the remote Actor, which
            # won't happen if they are passed through a `dict`.
            if len(kws) > 0:
                packed_kwargs = packed_args[-len(kws):]
                packed_args = packed_args[:-len(kws)]
            else:
                packed_kwargs = {}

            # Validate args and kwargs.
            assert len(packed_args) == len(arg_ptypes), (
                "Must have same number of arguments"
                "and types:\n{}\n{}".format(
                    packed_args, arg_ptypes))

            assert len(packed_kwargs) == len(kwarg_ptypes), (
                "Must have same number of keyword arguments"
                "and types:\n{}\n{}".format(
                    packed_kwargs, kwarg_ptypes))

            # Execute the forward pass.
            return _differentiable_forward(
                self, identifier, method, persistent_tape, packed_args, arg_ptypes,
                packed_kwargs, kwarg_ptypes, kws)
        else:
            assert len(packed_args) == 0, ("During backwards pass, no " 
                                           "packed_args must be provided "
                                           "but packed_args is {}.".format(packed_args))
            assert arg_ptypes is None and \
                   kwarg_ptypes is None and \
                   kws is None, (
                     "During backwards pass, arg_ptypes, "
                     "kwarg_ptypes, and kws must be None but are: "
                     "{}, {}, and {}, respectively.".format(arg_ptypes,
                                                            kwarg_ptypes, kws))

            # Execute the backward pass.
            return _differentiable_backward(self, identifier, persistent_tape,
                                            packed_dys, dys_ptypes)

    # Tag method metadata for Ray.
    differentiable_method.__ray_tf_differentiable__ = True

    return differentiable_method


class TFObjectID(ray.ObjectID):
    """ObjectID wrapper for un-fetched results whose real type is 
       undetermined. Contains extra metadata to link driver TF graph.

    Attributes:
        graph_tensor (tf.Tensor): A dummy tensor used to
                                  link the driver TF graph.
    """

    def __init__(self, object_id, graph_tensor):
        """
        Args:
            object_id (ray.ObjectID): The ObjectID to wrap.
            graph_tensor (tf.Tensor): A dummy tensor used to
                                      link the driver TF graph.
        """
        ray.ObjectID.__init__(self, object_id.binary())
        self.graph_tensor = graph_tensor


def _submit_tf_differentiable(actor_method, args, kwargs, num_return_vals):
    """Invoke TensorFlow differentiable actor method.

    This will add a new op to the driver TF graph representing the remote actor
    method invocation.

    Args:
        actor_method (ray.ActorMethod): The actor method to call.
        args (list): The arguments.
        kwargs (dict): The keyword arguments.
        num_return_vals (int): The numer of return values.

    Returns:
        TFObjectID or list of TFObjectID: The un-fetched results.
    """
    if not tf.executing_eagerly():
        raise RuntimeError("Tensorflow Eager execution must "
                           "be enabled for differentiable "
                           "functions.")

    # Pack the arguments for serialization and reconstruction on the remote 
    # Actor
    (packed_args, arg_ptypes,
     packed_kwargs, kwarg_ptypes, kws,
     tf_args) = _pack_args(args, kwargs)

    result_tf_obj_ids = []

    @tf.custom_gradient
    def submit_op(*tf_args):
        """Invokes the forward pass of the remote actor method.

        This becomes an operation in the driver TF graph representing the
        remote actor method invocation.

        Args:
            tf_args (tf.Tensor or tf.Variable): The TF arguments to
                                                  the actor method.

        Note:
            The `tf_args` argument is not actually passed to the remote
            actor method. Instead, it is used to link the driver TF graph
            and a the packed versions (`packed_args`/`packed_kwargs`)
            are actually passed to the remote method.

        Returns:
            list, function: A list of output `tf.Tensor`s
                            and a gradient function.

        Note:
            The outputs returned here are the `graph_tensor`
            attributes of the `TFObjectID`s returned from the
            remote actor method invocation. They are merely to
            link the driver TF graph and are never used in any
            actual computation.
        """
        # Invoke the forward pass of the remote actor method
        # with the packed inputs.
        identifier = np.random.bytes(20)
        forward = True
        persistent_tape = False
        dys, dys_ptypes = None, None
        combined_packed_args = packed_args + packed_kwargs # See the note
        # in the `differentiable_method` docstring for why we do this.
        result_obj_ids = actor_method._internal_remote([
            identifier, forward, persistent_tape, dys, dys_ptypes, kwarg_ptypes,
            kws, arg_ptypes
        ] + combined_packed_args, {}, num_return_vals)

        # Wrap each of the `ray.ObjectID`s returned in a `TFObjectID`.
        if isinstance(result_obj_ids, ray.ObjectID):
            result_obj_ids = [result_obj_ids]
        for obj_id in result_obj_ids:
            graph_tensor = tf.constant(1.0)
            result_tf_obj_ids.append(TFObjectID(obj_id, graph_tensor))

        def grad(*dys):
            """Invokes the backwards pass of the remote actor
               method for gradient computation.

            Args:
                dys (list): The partial downstream gradients.

            Returns:
                list: The gradients.
            """
            # Invoke the backward pass of the remote actor
            # method with the packed `dys`.
            forward = False
            kwarg_ptypes, kws = None, None
            arg_ptypes = None
            num_return_vals = len(
                tf_args)  # The number of returned gradients is
            # the number of TF arguments.
            packed_dys, dy_ptypes = _pack_many(dys)
            grad_ids = actor_method._internal_remote([
                identifier, forward, persistent_tape, packed_dys, dy_ptypes,
                kwarg_ptypes, kws, arg_ptypes,
            ], {}, num_return_vals)
            
            # Fetch the resulting gradients.
            grads = ray.get(grad_ids)

            # Unpack the gradients.
            grads_unpacked = _unpack_many(*zip(*grads))

            return grads_unpacked

        # Extract the dummy `graph_tensor`s used to link the driver TF graph.
        results = [tf_obj_id.graph_tensor for tf_obj_id in result_tf_obj_ids]

        return results, grad

    results = submit_op(*tf_args)

    # Manually connect the local TF graph.
    # TODO(vsatish): Figure out why the graph is disconnected without this
    # or find a cleaner way to do this.
    for tf_obj_id, graph_tensor in zip(result_tf_obj_ids, results):
        tf_obj_id.graph_tensor = graph_tensor

    return result_tf_obj_ids[0] if len(
        result_tf_obj_ids) == 1 else result_tf_obj_ids


def _post_process_get(tf_object_ids, results):
    """Post-process the `TFObjectID` results from ray.get().

    Right now all it does is unpack the fetched results.

    Args:
        tf_object_ids (list): The `TFObjectID`s.
        results (list): The corresponding fetched values.

    Returns:
        list: The unpacked results.
    """
    if not tf.executing_eagerly():
        raise RuntimeError("Tensorflow Eager execution must "
                           "be enabled for differentiable "
                           "functions.")

    # Unpack the results.
    unpacked_results = _unpack_many(*zip(*results)) 

    # We want to link only the TF results to the `get_op` below. Thus, we must
    # split the unpacked results into the TF results and everything else.
    graph_tensors = [] # Only for TF results.
    unpacked_tf_results = []
    unpacked_tf_result_ind = [] # We will need this to stitch everything back together later.
    unpacked_tf_result_var_ind = [] # We need this because the `get_op` implicitly converts all its returned values to `tf.Tensor` when it might actually be `tf.Variable`. We solve this by explicitly casting these back. 
    unpacked_other_results = []
    for idx, (tf_obj_id, unpacked_result, (_, result_ptype)) in enumerate(zip(tf_object_ids, unpacked_results, results)):
        if result_ptype in [ValType.TF_VAR, ValType.TF_TEN, ValType.TF_VAR_SC, ValType.TF_TEN_SC]:
            graph_tensors.append(tf_obj_id.graph_tensor)
            unpacked_tf_results.append(unpacked_result)
            unpacked_tf_result_ind.append(idx)
            if result_ptype in [ValType.TF_VAR, ValType.TF_VAR_SC]:
                unpacked_tf_result_var_ind.append(idx)
        else:
            unpacked_other_results.append(unpacked_result)

    @tf.custom_gradient
    def get_op(*graph_tensors):
        """Post-process the results.

        This becomes an operation in our driver TF graph representing
        the call to `ray.get`. It doesn't really do anything because the actual
        post-processing is done in the enclosing function.

        Args:
            graph_tensors (list): The `graph_tensor` attributes of
                                  the TFObjectIDs in `ray.get`.

        Note:
            `graph_tensors` is used solely to link the driver TF graph
            and is never used in any computation. The actual results
            are in `unpacked_tf_results` that we close over.

        Returns:
            list: A list of `tf.Tensor`s representing the post-processed
                  results.
        """
        def grad(*dys):
            """Backwards pass of `ray.get` for gradient computation.

            Right now this does nothing, but in the future it can be
            used to start the backwards pass of the actor method in advance.

            Args:
                dys (list): The partial downstream gradients.

            Returns:
                list: The gradients.
            """
            return dys

        return unpacked_tf_results, grad

    unpacked_tf_results = get_op(*graph_tensors)

    # Stitch the TF and non-TF results back together.
    unpacked_results_stitched_together = []
    for i in range(len(unpacked_results)):
        if i in unpacked_tf_result_var_ind:
            unpacked_results_stitched_together.append(tf.Variable(unpacked_tf_results.pop(0)))
        elif i in unpacked_tf_result_ind:
            unpacked_results_stitched_together.append(unpacked_tf_results.pop(0))
        else:
            unpacked_results_stitched_together.append(unpacked_other_results.pop(0))
    return unpacked_results_stitched_together
