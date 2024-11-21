import logging
from distutils.version import LooseVersion

import torch
import torch as th
import torch.nn as nn

if LooseVersion(torch.__version__) < LooseVersion("1.8.0"):
    logging.warning(
        f"torch.fx requires version higher than 1.8.0. "
        f"But You are using an old version PyTorch {torch.__version__}. "
    )


def count_clamp(input_shapes, output_shapes):
    """Ensures tensor array sizes are appropriate by clamping specified input and output shapes."""
    return 0


def count_mul(input_shapes, output_shapes):
    """Returns the number of elements in the first output shape."""
    return output_shapes[0].numel()


def count_matmul(input_shapes, output_shapes):
    """Calculates matrix multiplication ops based on input and output tensor shapes for performance profiling."""
    in_shape = input_shapes[0]
    out_shape = output_shapes[0]
    in_features = in_shape[-1]
    num_elements = out_shape.numel()
    return in_features * num_elements


def count_fn_linear(input_shapes, output_shapes, *args, **kwargs):
    """Calculates the total FLOPs for a linear layer, including bias operations if specified."""
    flops = count_matmul(input_shapes, output_shapes)
    if "bias" in kwargs:
        flops += output_shapes[0].numel()
    return flops


from .vision.calc_func import calculate_conv


def count_fn_conv2d(input_shapes, output_shapes, *args, **kwargs):
    """Calculates total operations (FLOPs) for a 2D conv layer based on input and output shapes using
    `calculate_conv`.
    """
    inputs, weight, bias, stride, padding, dilation, groups = args
    if len(input_shapes) == 2:
        x_shape, k_shape = input_shapes
    elif len(input_shapes) == 3:
        x_shape, k_shape, b_shape = input_shapes
    out_shape = output_shapes[0]

    kernel_parameters = k_shape[2:].numel()
    bias_op = 0  # check it later
    in_channel = x_shape[1]

    total_ops = calculate_conv(bias_op, kernel_parameters, out_shape.numel(), in_channel, groups).item()
    return int(total_ops)


def count_nn_linear(module: nn.Module, input_shapes, output_shapes):
    """Counts the FLOPs for a fully connected (linear) layer in a neural network module."""
    return count_matmul(input_shapes, output_shapes)


def count_zero_ops(module: nn.Module, input_shapes, output_shapes, *args, **kwargs):
    """Returns 0 for a neural network module, input shapes, and output shapes in PyTorch."""
    return 0


def count_nn_conv2d(module: nn.Conv2d, input_shapes, output_shapes):
    """Calculates FLOPs for a 2D Conv2D layer in an nn.Module using input and output shapes."""
    bias_op = 1 if module.bias is not None else 0
    out_shape = output_shapes[0]

    in_channel = module.in_channels
    groups = module.groups
    kernel_ops = module.weight.shape[2:].numel()
    total_ops = calculate_conv(bias_op, kernel_ops, out_shape.numel(), in_channel, groups).item()
    return int(total_ops)


def count_nn_bn2d(module: nn.BatchNorm2d, input_shapes, output_shapes):
    """Calculate FLOPs for an nn.BatchNorm2d layer based on the given output shape."""
    assert len(output_shapes) == 1, "nn.BatchNorm2d should only have one output"
    y = output_shapes[0]
    return 2 * y.numel()


zero_ops = (
    nn.ReLU,
    nn.ReLU6,
    nn.Dropout,
    nn.MaxPool2d,
    nn.AvgPool2d,
    nn.AdaptiveAvgPool2d,
)

count_map = {
    nn.Linear: count_nn_linear,
    nn.Conv2d: count_nn_conv2d,
    nn.BatchNorm2d: count_nn_bn2d,
    "function linear": count_fn_linear,
    "clamp": count_clamp,
    "built-in function add": count_zero_ops,
    "built-in method fl": count_zero_ops,
    "built-in method conv2d of type object": count_fn_conv2d,
    "built-in function mul": count_mul,
    "built-in function truediv": count_mul,
}

for k in zero_ops:
    count_map[k] = count_zero_ops

missing_maps = {}

from torch.fx import symbolic_trace
from torch.fx.passes.shape_prop import ShapeProp

from .utils import prRed, prYellow


def null_print(*args, **kwargs):
    """A no-op print function that takes any arguments without performing any actions."""
    return


def fx_profile(mod: nn.Module, input: th.Tensor, verbose=False):
    """Profiles nn.Module for total FLOPs per operation and prints detailed nodes if verbose."""
    gm: torch.fx.GraphModule = symbolic_trace(mod)
    ShapeProp(gm).propagate(input)

    fprint = null_print
    if verbose:
        fprint = print

    v_maps = {}
    total_flops = 0

    for node in gm.graph.nodes:
        # print(f"{node.target},\t{node.op},\t{node.meta['tensor_meta'].dtype},\t{node.meta['tensor_meta'].shape}")
        fprint(f"NodeOP:{node.op},\tTarget:{node.target},\tNodeName:{node.name},\tNodeArgs:{node.args}")
        # node_op_type = str(node.target).split(".")[-1]
        node_flops = None

        input_shapes = []
        fprint("input_shape:", end="\t")
        for arg in node.args:
            if str(arg) not in v_maps:
                continue
            fprint(f"{v_maps[str(arg)]}", end="\t")
            input_shapes.append(v_maps[str(arg)])
        fprint()
        fprint(f"output_shape:\t{node.meta['tensor_meta'].shape}")
        output_shapes = [node.meta["tensor_meta"].shape]
        if node.op in ["output", "placeholder"]:
            node_flops = 0
        elif node.op == "call_function":
            # torch internal functions
            key = str(node.target).split("at")[0].replace("<", "").replace(">", "").strip()
            if key in count_map:
                node_flops = count_map[key](input_shapes, output_shapes, *node.args, **node.kwargs)
            else:
                missing_maps[key] = (node.op, key)
                prRed(f"|{key}| is missing")
        elif node.op == "call_method":
            # torch internal functions
            # fprint(str(node.target) in count_map, str(node.target), count_map.keys())
            key = str(node.target)
            if key in count_map:
                node_flops = count_map[key](input_shapes, output_shapes)
            else:
                missing_maps[key] = (node.op, key)
                prRed(f"{key} is missing")
        elif node.op == "call_module":
            # torch.nn modules
            # m = getattr(mod, node.target, None)
            m = mod.get_submodule(node.target)
            key = type(m)
            fprint(type(m), type(m) in count_map)
            if type(m) in count_map:
                node_flops = count_map[type(m)](m, input_shapes, output_shapes)
            else:
                missing_maps[key] = (node.op,)
                prRed(f"{key} is missing")
            print("module type:", type(m))
            if isinstance(m, zero_ops):
                print("weight_shape: None")
            else:
                print(type(m))
                print(f"weight_shape: {mod.state_dict()[f'{node.target}.weight'].shape}")

        v_maps[str(node.name)] = node.meta["tensor_meta"].shape
        if node_flops is not None:
            total_flops += node_flops
        prYellow(f"Current node's FLOPs: {node_flops}, total FLOPs: {total_flops}")
        fprint("==" * 40)

    if len(missing_maps.keys()) > 0:
        from pprint import pprint

        print("Missing operators: ")
        pprint(missing_maps)
    return total_flops


if __name__ == "__main__":

    class MyOP(nn.Module):
        def forward(self, input):
            """Performs forward pass on given input data."""
            return input / 1

    class MyModule(torch.nn.Module):
        def __init__(self):
            """Initializes MyModule with two linear layers and a custom MyOP operator."""
            super().__init__()
            self.linear1 = torch.nn.Linear(5, 3)
            self.linear2 = torch.nn.Linear(5, 3)
            self.myop = MyOP()

        def forward(self, x):
            """Applies two linear transformations to the input tensor, clamps the second, then combines and processes
            with MyOP operator.
            """
            out1 = self.linear1(x)
            out2 = self.linear2(x).clamp(min=0.0, max=1.0)
            return self.myop(out1 + out2)

    net = MyModule()
    data = th.randn(20, 5)
    flops = fx_profile(net, data, verbose=False)
    print(flops)
