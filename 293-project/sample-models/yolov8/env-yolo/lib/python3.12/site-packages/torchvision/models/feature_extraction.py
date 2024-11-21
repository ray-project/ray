import inspect
import math
import re
import warnings
from collections import OrderedDict
from copy import deepcopy
from itertools import chain
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import torch
import torchvision
from torch import fx, nn
from torch.fx.graph_module import _copy_attr


__all__ = ["create_feature_extractor", "get_graph_node_names"]


class LeafModuleAwareTracer(fx.Tracer):
    """
    An fx.Tracer that allows the user to specify a set of leaf modules, i.e.
    modules that are not to be traced through. The resulting graph ends up
    having single nodes referencing calls to the leaf modules' forward methods.
    """

    def __init__(self, *args, **kwargs):
        self.leaf_modules = {}
        if "leaf_modules" in kwargs:
            leaf_modules = kwargs.pop("leaf_modules")
            self.leaf_modules = leaf_modules
        super().__init__(*args, **kwargs)

    def is_leaf_module(self, m: nn.Module, module_qualname: str) -> bool:
        if isinstance(m, tuple(self.leaf_modules)):
            return True
        return super().is_leaf_module(m, module_qualname)


class NodePathTracer(LeafModuleAwareTracer):
    """
    NodePathTracer is an FX tracer that, for each operation, also records the
    name of the Node from which the operation originated. A node name here is
    a `.` separated path walking the hierarchy from top level module down to
    leaf operation or leaf module. The name of the top level module is not
    included as part of the node name. For example, if we trace a module whose
    forward method applies a ReLU module, the name for that node will simply
    be 'relu'.

    Some notes on the specifics:
        - Nodes are recorded to `self.node_to_qualname` which is a dictionary
          mapping a given Node object to its node name.
        - Nodes are recorded in the order which they are executed during
          tracing.
        - When a duplicate node name is encountered, a suffix of the form
          _{int} is added. The counter starts from 1.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Track the qualified name of the Node being traced
        self.current_module_qualname = ""
        # A map from FX Node to the qualified name\#
        # NOTE: This is loosely like the "qualified name" mentioned in the
        # torch.fx docs https://pytorch.org/docs/stable/fx.html but adapted
        # for the purposes of the torchvision feature extractor
        self.node_to_qualname = OrderedDict()

    def call_module(self, m: torch.nn.Module, forward: Callable, args, kwargs):
        """
        Override of `fx.Tracer.call_module`
        This override:
        1) Stores away the qualified name of the caller for restoration later
        2) Adds the qualified name of the caller to
           `current_module_qualname` for retrieval by `create_proxy`
        3) Once a leaf module is reached, calls `create_proxy`
        4) Restores the caller's qualified name into current_module_qualname
        """
        old_qualname = self.current_module_qualname
        try:
            module_qualname = self.path_of_module(m)
            self.current_module_qualname = module_qualname
            if not self.is_leaf_module(m, module_qualname):
                out = forward(*args, **kwargs)
                return out
            return self.create_proxy("call_module", module_qualname, args, kwargs)
        finally:
            self.current_module_qualname = old_qualname

    def create_proxy(
        self, kind: str, target: fx.node.Target, args, kwargs, name=None, type_expr=None, *_
    ) -> fx.proxy.Proxy:
        """
        Override of `Tracer.create_proxy`. This override intercepts the recording
        of every operation and stores away the current traced module's qualified
        name in `node_to_qualname`
        """
        proxy = super().create_proxy(kind, target, args, kwargs, name, type_expr)
        self.node_to_qualname[proxy.node] = self._get_node_qualname(self.current_module_qualname, proxy.node)
        return proxy

    def _get_node_qualname(self, module_qualname: str, node: fx.node.Node) -> str:
        node_qualname = module_qualname

        if node.op != "call_module":
            # In this case module_qualname from torch.fx doesn't go all the
            # way to the leaf function/op, so we need to append it
            if len(node_qualname) > 0:
                # Only append '.' if we are deeper than the top level module
                node_qualname += "."
            node_qualname += str(node)

        # Now we need to add an _{index} postfix on any repeated node names
        # For modules we do this from scratch
        # But for anything else, torch.fx already has a globally scoped
        # _{index} postfix. But we want it locally (relative to direct parent)
        # scoped. So first we need to undo the torch.fx postfix
        if re.match(r".+_[0-9]+$", node_qualname) is not None:
            node_qualname = node_qualname.rsplit("_", 1)[0]

        # ... and now we add on our own postfix
        for existing_qualname in reversed(self.node_to_qualname.values()):
            # Check to see if existing_qualname is of the form
            # {node_qualname} or {node_qualname}_{int}
            if re.match(rf"{node_qualname}(_[0-9]+)?$", existing_qualname) is not None:
                postfix = existing_qualname.replace(node_qualname, "")
                if len(postfix):
                    # existing_qualname is of the form {node_qualname}_{int}
                    next_index = int(postfix[1:]) + 1
                else:
                    # existing_qualname is of the form {node_qualname}
                    next_index = 1
                node_qualname += f"_{next_index}"
                break

        return node_qualname


def _is_subseq(x, y):
    """Check if y is a subsequence of x
    https://stackoverflow.com/a/24017747/4391249
    """
    iter_x = iter(x)
    return all(any(x_item == y_item for x_item in iter_x) for y_item in y)


def _warn_graph_differences(train_tracer: NodePathTracer, eval_tracer: NodePathTracer):
    """
    Utility function for warning the user if there are differences between
    the train graph nodes and the eval graph nodes.
    """
    train_nodes = list(train_tracer.node_to_qualname.values())
    eval_nodes = list(eval_tracer.node_to_qualname.values())

    if len(train_nodes) == len(eval_nodes) and all(t == e for t, e in zip(train_nodes, eval_nodes)):
        return

    suggestion_msg = (
        "When choosing nodes for feature extraction, you may need to specify "
        "output nodes for train and eval mode separately."
    )

    if _is_subseq(train_nodes, eval_nodes):
        msg = (
            "NOTE: The nodes obtained by tracing the model in eval mode "
            "are a subsequence of those obtained in train mode. "
        )
    elif _is_subseq(eval_nodes, train_nodes):
        msg = (
            "NOTE: The nodes obtained by tracing the model in train mode "
            "are a subsequence of those obtained in eval mode. "
        )
    else:
        msg = "The nodes obtained by tracing the model in train mode are different to those obtained in eval mode. "
    warnings.warn(msg + suggestion_msg)


def _get_leaf_modules_for_ops() -> List[type]:
    members = inspect.getmembers(torchvision.ops)
    result = []
    for _, obj in members:
        if inspect.isclass(obj) and issubclass(obj, torch.nn.Module):
            result.append(obj)
    return result


def _set_default_tracer_kwargs(original_tr_kwargs: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    default_autowrap_modules = (math, torchvision.ops)
    default_leaf_modules = _get_leaf_modules_for_ops()
    result_tracer_kwargs = {} if original_tr_kwargs is None else original_tr_kwargs
    result_tracer_kwargs["autowrap_modules"] = (
        tuple(set(result_tracer_kwargs["autowrap_modules"] + default_autowrap_modules))
        if "autowrap_modules" in result_tracer_kwargs
        else default_autowrap_modules
    )
    result_tracer_kwargs["leaf_modules"] = (
        list(set(result_tracer_kwargs["leaf_modules"] + default_leaf_modules))
        if "leaf_modules" in result_tracer_kwargs
        else default_leaf_modules
    )
    return result_tracer_kwargs


def get_graph_node_names(
    model: nn.Module,
    tracer_kwargs: Optional[Dict[str, Any]] = None,
    suppress_diff_warning: bool = False,
    concrete_args: Optional[Dict[str, Any]] = None,
) -> Tuple[List[str], List[str]]:
    """
    Dev utility to return node names in order of execution. See note on node
    names under :func:`create_feature_extractor`. Useful for seeing which node
    names are available for feature extraction. There are two reasons that
    node names can't easily be read directly from the code for a model:

        1. Not all submodules are traced through. Modules from ``torch.nn`` all
           fall within this category.
        2. Nodes representing the repeated application of the same operation
           or leaf module get a ``_{counter}`` postfix.

    The model is traced twice: once in train mode, and once in eval mode. Both
    sets of node names are returned.

    For more details on the node naming conventions used here, please see the
    :ref:`relevant subheading <about-node-names>` in the
    `documentation <https://pytorch.org/vision/stable/feature_extraction.html>`_.

    Args:
        model (nn.Module): model for which we'd like to print node names
        tracer_kwargs (dict, optional): a dictionary of keyword arguments for
            ``NodePathTracer`` (they are eventually passed onto
            `torch.fx.Tracer <https://pytorch.org/docs/stable/fx.html#torch.fx.Tracer>`_).
            By default, it will be set to wrap and make leaf nodes all torchvision ops:
            {"autowrap_modules": (math, torchvision.ops,),"leaf_modules": _get_leaf_modules_for_ops(),}
            WARNING: In case the user provides tracer_kwargs, above default arguments will be appended to the user
            provided dictionary.
        suppress_diff_warning (bool, optional): whether to suppress a warning
            when there are discrepancies between the train and eval version of
            the graph. Defaults to False.
        concrete_args (Optional[Dict[str, any]]): Concrete arguments that should
            not be treated as Proxies. According to the `Pytorch docs
            <https://pytorch.org/docs/stable/fx.html#torch.fx.Tracer.trace>`_,
            this parameter's API may not be guaranteed.

    Returns:
        tuple(list, list): a list of node names from tracing the model in
        train mode, and another from tracing the model in eval mode.

    Examples::

        >>> model = torchvision.models.resnet18()
        >>> train_nodes, eval_nodes = get_graph_node_names(model)
    """
    tracer_kwargs = _set_default_tracer_kwargs(tracer_kwargs)
    is_training = model.training
    train_tracer = NodePathTracer(**tracer_kwargs)
    train_tracer.trace(model.train(), concrete_args=concrete_args)
    eval_tracer = NodePathTracer(**tracer_kwargs)
    eval_tracer.trace(model.eval(), concrete_args=concrete_args)
    train_nodes = list(train_tracer.node_to_qualname.values())
    eval_nodes = list(eval_tracer.node_to_qualname.values())
    if not suppress_diff_warning:
        _warn_graph_differences(train_tracer, eval_tracer)
    # Restore training state
    model.train(is_training)
    return train_nodes, eval_nodes


class DualGraphModule(fx.GraphModule):
    """
    A derivative of `fx.GraphModule`. Differs in the following ways:
    - Requires a train and eval version of the underlying graph
    - Copies submodules according to the nodes of both train and eval graphs.
    - Calling train(mode) switches between train graph and eval graph.
    """

    def __init__(
        self, root: torch.nn.Module, train_graph: fx.Graph, eval_graph: fx.Graph, class_name: str = "GraphModule"
    ):
        """
        Args:
            root (nn.Module): module from which the copied module hierarchy is
                built
            train_graph (fx.Graph): the graph that should be used in train mode
            eval_graph (fx.Graph): the graph that should be used in eval mode
        """
        super(fx.GraphModule, self).__init__()

        self.__class__.__name__ = class_name

        self.train_graph = train_graph
        self.eval_graph = eval_graph

        # Copy all get_attr and call_module ops (indicated by BOTH train and
        # eval graphs)
        for node in chain(iter(train_graph.nodes), iter(eval_graph.nodes)):
            if node.op in ["get_attr", "call_module"]:
                if not isinstance(node.target, str):
                    raise TypeError(f"node.target should be of type str instead of {type(node.target)}")
                _copy_attr(root, self, node.target)

        # train mode by default
        self.train()
        self.graph = train_graph

        # (borrowed from fx.GraphModule):
        # Store the Tracer class responsible for creating a Graph separately as part of the
        # GraphModule state, except when the Tracer is defined in a local namespace.
        # Locally defined Tracers are not pickleable. This is needed because torch.package will
        # serialize a GraphModule without retaining the Graph, and needs to use the correct Tracer
        # to re-create the Graph during deserialization.
        if self.eval_graph._tracer_cls != self.train_graph._tracer_cls:
            raise TypeError(
                f"Train mode and eval mode should use the same tracer class. Instead got {self.eval_graph._tracer_cls} for eval vs {self.train_graph._tracer_cls} for train"
            )
        self._tracer_cls = None
        if self.graph._tracer_cls and "<locals>" not in self.graph._tracer_cls.__qualname__:
            self._tracer_cls = self.graph._tracer_cls

    def train(self, mode=True):
        """
        Swap out the graph depending on the selected training mode.
        NOTE this should be safe when calling model.eval() because that just
        calls this with mode == False.
        """
        # NOTE: Only set self.graph if the current graph is not the desired
        # one. This saves us from recompiling the graph where not necessary.
        if mode and not self.training:
            self.graph = self.train_graph
        elif not mode and self.training:
            self.graph = self.eval_graph
        return super().train(mode=mode)


def create_feature_extractor(
    model: nn.Module,
    return_nodes: Optional[Union[List[str], Dict[str, str]]] = None,
    train_return_nodes: Optional[Union[List[str], Dict[str, str]]] = None,
    eval_return_nodes: Optional[Union[List[str], Dict[str, str]]] = None,
    tracer_kwargs: Optional[Dict[str, Any]] = None,
    suppress_diff_warning: bool = False,
    concrete_args: Optional[Dict[str, Any]] = None,
) -> fx.GraphModule:
    """
    Creates a new graph module that returns intermediate nodes from a given
    model as dictionary with user specified keys as strings, and the requested
    outputs as values. This is achieved by re-writing the computation graph of
    the model via FX to return the desired nodes as outputs. All unused nodes
    are removed, together with their corresponding parameters.

    Desired output nodes must be specified as a ``.`` separated
    path walking the module hierarchy from top level module down to leaf
    operation or leaf module. For more details on the node naming conventions
    used here, please see the :ref:`relevant subheading <about-node-names>`
    in the `documentation <https://pytorch.org/vision/stable/feature_extraction.html>`_.

    Not all models will be FX traceable, although with some massaging they can
    be made to cooperate. Here's a (not exhaustive) list of tips:

        - If you don't need to trace through a particular, problematic
          sub-module, turn it into a "leaf module" by passing a list of
          ``leaf_modules`` as one of the ``tracer_kwargs`` (see example below).
          It will not be traced through, but rather, the resulting graph will
          hold a reference to that module's forward method.
        - Likewise, you may turn functions into leaf functions by passing a
          list of ``autowrap_functions`` as one of the ``tracer_kwargs`` (see
          example below).
        - Some inbuilt Python functions can be problematic. For instance,
          ``int`` will raise an error during tracing. You may wrap them in your
          own function and then pass that in ``autowrap_functions`` as one of
          the ``tracer_kwargs``.

    For further information on FX see the
    `torch.fx documentation <https://pytorch.org/docs/stable/fx.html>`_.

    Args:
        model (nn.Module): model on which we will extract the features
        return_nodes (list or dict, optional): either a ``List`` or a ``Dict``
            containing the names (or partial names - see note above)
            of the nodes for which the activations will be returned. If it is
            a ``Dict``, the keys are the node names, and the values
            are the user-specified keys for the graph module's returned
            dictionary. If it is a ``List``, it is treated as a ``Dict`` mapping
            node specification strings directly to output names. In the case
            that ``train_return_nodes`` and ``eval_return_nodes`` are specified,
            this should not be specified.
        train_return_nodes (list or dict, optional): similar to
            ``return_nodes``. This can be used if the return nodes
            for train mode are different than those from eval mode.
            If this is specified, ``eval_return_nodes`` must also be specified,
            and ``return_nodes`` should not be specified.
        eval_return_nodes (list or dict, optional): similar to
            ``return_nodes``. This can be used if the return nodes
            for train mode are different than those from eval mode.
            If this is specified, ``train_return_nodes`` must also be specified,
            and `return_nodes` should not be specified.
        tracer_kwargs (dict, optional): a dictionary of keyword arguments for
            ``NodePathTracer`` (which passes them onto it's parent class
            `torch.fx.Tracer <https://pytorch.org/docs/stable/fx.html#torch.fx.Tracer>`_).
            By default, it will be set to wrap and make leaf nodes all torchvision ops:
            {"autowrap_modules": (math, torchvision.ops,),"leaf_modules": _get_leaf_modules_for_ops(),}
            WARNING: In case the user provides tracer_kwargs, above default arguments will be appended to the user
            provided dictionary.
        suppress_diff_warning (bool, optional): whether to suppress a warning
            when there are discrepancies between the train and eval version of
            the graph. Defaults to False.
        concrete_args (Optional[Dict[str, any]]): Concrete arguments that should
            not be treated as Proxies. According to the `Pytorch docs
            <https://pytorch.org/docs/stable/fx.html#torch.fx.Tracer.trace>`_,
            this parameter's API may not be guaranteed.

    Examples::

        >>> # Feature extraction with resnet
        >>> model = torchvision.models.resnet18()
        >>> # extract layer1 and layer3, giving as names `feat1` and feat2`
        >>> model = create_feature_extractor(
        >>>     model, {'layer1': 'feat1', 'layer3': 'feat2'})
        >>> out = model(torch.rand(1, 3, 224, 224))
        >>> print([(k, v.shape) for k, v in out.items()])
        >>>     [('feat1', torch.Size([1, 64, 56, 56])),
        >>>      ('feat2', torch.Size([1, 256, 14, 14]))]

        >>> # Specifying leaf modules and leaf functions
        >>> def leaf_function(x):
        >>>     # This would raise a TypeError if traced through
        >>>     return int(x)
        >>>
        >>> class LeafModule(torch.nn.Module):
        >>>     def forward(self, x):
        >>>         # This would raise a TypeError if traced through
        >>>         int(x.shape[0])
        >>>         return torch.nn.functional.relu(x + 4)
        >>>
        >>> class MyModule(torch.nn.Module):
        >>>     def __init__(self):
        >>>         super().__init__()
        >>>         self.conv = torch.nn.Conv2d(3, 1, 3)
        >>>         self.leaf_module = LeafModule()
        >>>
        >>>     def forward(self, x):
        >>>         leaf_function(x.shape[0])
        >>>         x = self.conv(x)
        >>>         return self.leaf_module(x)
        >>>
        >>> model = create_feature_extractor(
        >>>     MyModule(), return_nodes=['leaf_module'],
        >>>     tracer_kwargs={'leaf_modules': [LeafModule],
        >>>                    'autowrap_functions': [leaf_function]})

    """
    tracer_kwargs = _set_default_tracer_kwargs(tracer_kwargs)
    is_training = model.training

    if all(arg is None for arg in [return_nodes, train_return_nodes, eval_return_nodes]):

        raise ValueError(
            "Either `return_nodes` or `train_return_nodes` and `eval_return_nodes` together, should be specified"
        )

    if (train_return_nodes is None) ^ (eval_return_nodes is None):
        raise ValueError(
            "If any of `train_return_nodes` and `eval_return_nodes` are specified, then both should be specified"
        )

    if not ((return_nodes is None) ^ (train_return_nodes is None)):
        raise ValueError("If `train_return_nodes` and `eval_return_nodes` are specified, then both should be specified")

    # Put *_return_nodes into Dict[str, str] format
    def to_strdict(n) -> Dict[str, str]:
        if isinstance(n, list):
            return {str(i): str(i) for i in n}
        return {str(k): str(v) for k, v in n.items()}

    if train_return_nodes is None:
        return_nodes = to_strdict(return_nodes)
        train_return_nodes = deepcopy(return_nodes)
        eval_return_nodes = deepcopy(return_nodes)
    else:
        train_return_nodes = to_strdict(train_return_nodes)
        eval_return_nodes = to_strdict(eval_return_nodes)

    # Repeat the tracing and graph rewriting for train and eval mode
    tracers = {}
    graphs = {}
    mode_return_nodes: Dict[str, Dict[str, str]] = {"train": train_return_nodes, "eval": eval_return_nodes}
    for mode in ["train", "eval"]:
        if mode == "train":
            model.train()
        elif mode == "eval":
            model.eval()

        # Instantiate our NodePathTracer and use that to trace the model
        tracer = NodePathTracer(**tracer_kwargs)
        graph = tracer.trace(model, concrete_args=concrete_args)

        name = model.__class__.__name__ if isinstance(model, nn.Module) else model.__name__
        graph_module = fx.GraphModule(tracer.root, graph, name)

        available_nodes = list(tracer.node_to_qualname.values())
        # FIXME We don't know if we should expect this to happen
        if len(set(available_nodes)) != len(available_nodes):
            raise ValueError(
                "There are duplicate nodes! Please raise an issue https://github.com/pytorch/vision/issues"
            )
        # Check that all outputs in return_nodes are present in the model
        for query in mode_return_nodes[mode].keys():
            # To check if a query is available we need to check that at least
            # one of the available names starts with it up to a .
            if not any([re.match(rf"^{query}(\.|$)", n) is not None for n in available_nodes]):
                raise ValueError(
                    f"node: '{query}' is not present in model. Hint: use "
                    "`get_graph_node_names` to make sure the "
                    "`return_nodes` you specified are present. It may even "
                    "be that you need to specify `train_return_nodes` and "
                    "`eval_return_nodes` separately."
                )

        # Remove existing output nodes (train mode)
        orig_output_nodes = []
        for n in reversed(graph_module.graph.nodes):
            if n.op == "output":
                orig_output_nodes.append(n)
        if not orig_output_nodes:
            raise ValueError("No output nodes found in graph_module.graph.nodes")

        for n in orig_output_nodes:
            graph_module.graph.erase_node(n)

        # Find nodes corresponding to return_nodes and make them into output_nodes
        nodes = [n for n in graph_module.graph.nodes]
        output_nodes = OrderedDict()
        for n in reversed(nodes):
            module_qualname = tracer.node_to_qualname.get(n)
            if module_qualname is None:
                # NOTE - Know cases where this happens:
                # - Node representing creation of a tensor constant - probably
                #   not interesting as a return node
                # - When packing outputs into a named tuple like in InceptionV3
                continue
            for query in mode_return_nodes[mode]:
                depth = query.count(".")
                if ".".join(module_qualname.split(".")[: depth + 1]) == query:
                    output_nodes[mode_return_nodes[mode][query]] = n
                    mode_return_nodes[mode].pop(query)
                    break
        output_nodes = OrderedDict(reversed(list(output_nodes.items())))

        # And add them in the end of the graph
        with graph_module.graph.inserting_after(nodes[-1]):
            graph_module.graph.output(output_nodes)

        # Remove unused modules / parameters
        graph_module.graph.eliminate_dead_code()
        graph_module.recompile()

        # Keep track of the tracer and graph, so we can choose the main one
        tracers[mode] = tracer
        graphs[mode] = graph

    # Warn user if there are any discrepancies between the graphs of the
    # train and eval modes
    if not suppress_diff_warning:
        _warn_graph_differences(tracers["train"], tracers["eval"])

    # Build the final graph module
    graph_module = DualGraphModule(model, graphs["train"], graphs["eval"], class_name=name)

    # Restore original training mode
    model.train(is_training)
    graph_module.train(is_training)

    return graph_module
