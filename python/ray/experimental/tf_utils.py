from collections import OrderedDict, deque

import numpy as np

from ray.rllib.utils import force_list
from ray.rllib.utils.framework import try_import_tf

tf1, tf, tfv = try_import_tf()


def unflatten(vector, shapes):
    i = 0
    arrays = []
    for shape in shapes:
        size = np.prod(shape, dtype=np.int)
        array = vector[i : (i + size)].reshape(shape)
        arrays.append(array)
        i += size
    assert len(vector) == i, "Passed weight does not have the correct shape."
    return arrays


class TensorFlowVariables:
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
            sess (Optional[tf.Session]): Optional tf.Session used for running
                the get and set methods in tf graph mode.
                Use None for tf eager.
            input_variables (List[tf.Variables]): Variables to include in the
                list.
        """
        self.sess = sess
        output = force_list(output)
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
            if "Variable" in tf_obj.node_def.op or "VarHandle" in tf_obj.node_def.op:
                variable_names.append(tf_obj.node_def.name)
        self.variables = OrderedDict()
        variable_list = [
            v for v in tf1.global_variables() if v.op.node_def.name in variable_names
        ]
        if input_variables is not None:
            variable_list += input_variables

        if not tf1.executing_eagerly():
            for v in variable_list:
                self.variables[v.op.node_def.name] = v

            self.placeholders = {}
            self.assignment_nodes = {}

            # Create new placeholders to put in custom weights.
            for k, var in self.variables.items():
                self.placeholders[k] = tf1.placeholder(
                    var.value().dtype,
                    var.get_shape().as_list(),
                    name="Placeholder_" + k,
                )
                self.assignment_nodes[k] = var.assign(self.placeholders[k])
        else:
            for v in variable_list:
                self.variables[v.name] = v

    def get_flat_size(self):
        """Returns the total length of all of the flattened variables.

        Returns:
            The length of all flattened variables concatenated.
        """
        return sum(np.prod(v.get_shape().as_list()) for v in self.variables.values())

    def get_flat(self):
        """Gets the weights and returns them as a flat array.

        Returns:
            1D Array containing the flattened weights.
        """
        # Eager mode.
        if not self.sess:
            return np.concatenate(
                [v.numpy().flatten() for v in self.variables.values()]
            )
        # Graph mode.
        return np.concatenate(
            [v.eval(session=self.sess).flatten() for v in self.variables.values()]
        )

    def set_flat(self, new_weights):
        """Sets the weights to new_weights, converting from a flat array.

        Note:
            You can only set all weights in the network using this function,
            i.e., the length of the array must match get_flat_size.

        Args:
            new_weights (np.ndarray): Flat array containing weights.
        """
        shapes = [v.get_shape().as_list() for v in self.variables.values()]
        arrays = unflatten(new_weights, shapes)
        if not self.sess:
            for v, a in zip(self.variables.values(), arrays):
                v.assign(a)
        else:
            placeholders = [self.placeholders[k] for k, v in self.variables.items()]
            self.sess.run(
                list(self.assignment_nodes.values()),
                feed_dict=dict(zip(placeholders, arrays)),
            )

    def get_weights(self):
        """Returns a dictionary containing the weights of the network.

        Returns:
            Dictionary mapping variable names to their weights.
        """
        # Eager mode.
        if not self.sess:
            return self.variables
        # Graph mode.
        return self.sess.run(self.variables)

    def set_weights(self, new_weights: dict):
        """Sets the weights to new_weights.

        Note:
            Can set subsets of variables as well, by only passing in the
            variables you want to be set.

        Args:
            new_weights: Dictionary mapping variable names to their
                weights.
        """
        if self.sess is None:
            for name, var in self.variables.items():
                var.assign(new_weights[name])
        else:
            assign_list, feed_dict = self._assign_weights(new_weights)
            self.sess.run(assign_list, feed_dict=feed_dict)

    def _assign_weights(self, weights):
        """Sets weigths using exact or closest assignable variable name

        Args:
            weights: Dictionary mapping variable names to their
                weights.
        Returns:
            Tuple[List, Dict]: assigned variables list, dict of
                placeholders and weights
        """

        assigned = []
        feed_dict = {}
        assignable = set(self.assignment_nodes.keys())

        def nb_common_elem(l1, l2):
            return len([e for e in l1 if e in l2])

        def assign(name, value):
            feed_dict[self.placeholders[name]] = value
            assigned.append(name)
            assignable.remove(name)

        for name, value in weights.items():
            if name in assignable:
                assign(name, value)
            else:
                common = {
                    var: nb_common_elem(name.split("/"), var.split("/"))
                    for var in assignable
                }
                select = [
                    close_var
                    for close_var, cn in sorted(common.items(), key=lambda i: -i[1])
                    if cn > 0 and value.shape == self.assignment_nodes[close_var].shape
                ]
                if select:
                    assign(select[0], value)

        assert assigned, (
            "No variables in the input matched those in the network. "
            "Possible cause: Two networks were defined in the same "
            "TensorFlow graph. To fix this, place each network "
            "definition in its own tf.Graph."
        )

        assert len(assigned) == len(weights), (
            "All weights couldn't be assigned because no variable "
            "had an exact/close name or had same shape"
        )

        return [self.assignment_nodes[v] for v in assigned], feed_dict
