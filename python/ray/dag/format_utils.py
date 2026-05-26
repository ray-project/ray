from ray.dag import DAGNode
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
def get_dag_node_str(
    dag_node: DAGNode,
    body_line,
):
    indent = _get_indentation()
    other_args_to_resolve_lines = _get_other_args_to_resolve_lines(
        dag_node._bound_other_args_to_resolve
    )
    return (
        f"({dag_node.__class__.__name__}, {dag_node._stable_uuid})(\n"
        f"{indent}body={body_line}\n"
        f"{indent}args={_get_args_lines(dag_node._bound_args)}\n"
        f"{indent}kwargs={_get_kwargs_lines(dag_node._bound_kwargs)}\n"
        f"{indent}options={_get_options_lines(dag_node._bound_options)}\n"
        f"{indent}other_args_to_resolve={other_args_to_resolve_lines}\n"
        f")"
    )


def _get_indentation(num_spaces=4):
    return " " * num_spaces


def _format_value(val) -> str:
    if isinstance(val, DAGNode):
        return str(val)
    elif isinstance(val, dict):
        if not val:
            return "{}"
        lines = []
        indent = _get_indentation()
        for k, v in val.items():
            formatted_v = _format_value(v)
            v_lines = formatted_v.split("\n")
            if len(v_lines) == 1:
                lines.append(f"{k}: {v_lines[0]},")
            else:
                lines.append(f"{k}: {v_lines[0]}")
                for line in v_lines[1:]:
                    lines.append(line)
                lines[-1] += ","
        indented_lines = [indent + line for line in lines]
        return "{\n" + "\n".join(indented_lines) + "\n}"
    elif isinstance(val, (list, tuple, set)):
        # 1. Determine the brackets based on the type
        if isinstance(val, list):
            empty_val, open_b, close_b = "[]", "[", "]"
        elif isinstance(val, tuple):
            empty_val, open_b, close_b = "()", "(", ")"
        else:  # set
            empty_val, open_b, close_b = "set()", "{", "}"

        # 2. Handle empty containers
        if not val:
            return empty_val

        # 3. The unified formatting logic (only written once!)
        lines = []
        indent = _get_indentation()
        for ele in val:
            formatted_ele = _format_value(ele)
            ele_lines = formatted_ele.split("\n")
            for i, line in enumerate(ele_lines):
                if i == len(ele_lines) - 1:
                    lines.append(line + ",")
                else:
                    lines.append(line)

        indented_lines = [indent + line for line in lines]
        return f"{open_b}\n" + "\n".join(indented_lines) + f"\n{close_b}"
    else:
        return str(val)


def _get_args_lines(bound_args):
    """Pretty prints bounded args of a DAGNode, and recursively handle
    DAGNode in list / dict containers.
    """
    if not bound_args:
        return "[]"
    indent = _get_indentation()
    lines = []
    for arg in bound_args:
        formatted_arg = _format_value(arg)
        arg_lines = formatted_arg.split("\n")
        for i, line in enumerate(arg_lines):
            if i == len(arg_lines) - 1:
                lines.append(f"{indent}{line},")
            else:
                lines.append(f"{indent}{line}")

    args_line = "["
    for line in lines:
        args_line += f"\n{indent}{line}"
    args_line += f"\n{indent}]"

    return args_line


def _get_kwargs_lines(bound_kwargs):
    """Pretty prints bounded kwargs of a DAGNode, and recursively handle
    DAGNode in list / dict containers.
    """
    if not bound_kwargs:
        return "{}"
    indent = _get_indentation()
    kwargs_lines = []
    for key, val in bound_kwargs.items():
        formatted_val = _format_value(val)
        val_lines = formatted_val.split("\n")
        if len(val_lines) == 1:
            kwargs_lines.append(f"{indent}{key}: {val_lines[0]},")
        else:
            kwargs_lines.append(f"{indent}{key}: {val_lines[0]}")
            for line in val_lines[1:]:
                kwargs_lines.append(f"{indent}{line}")
            kwargs_lines[-1] += ","

    kwargs_line = "{"
    for line in kwargs_lines:
        kwargs_line += f"\n{indent}{line}"
    kwargs_line += f"\n{indent}}}"

    return kwargs_line


def _get_options_lines(bound_options):
    """Pretty prints .options() in DAGNode. Only prints non-empty values."""
    if not bound_options:
        return "{}"
    indent = _get_indentation()
    options_lines = []
    for key, val in bound_options.items():
        if val:
            options_lines.append(f"{indent}{key}: " + str(val))

    options_line = "{"
    for line in options_lines:
        options_line += f"\n{indent}{line}"
    options_line += f"\n{indent}}}"
    return options_line


def _get_other_args_to_resolve_lines(other_args_to_resolve):
    if not other_args_to_resolve:
        return "{}"
    indent = _get_indentation()
    other_args_to_resolve_lines = []
    for key, val in other_args_to_resolve.items():
        if isinstance(val, DAGNode):
            node_repr_lines = str(val).split("\n")
            for index, node_repr_line in enumerate(node_repr_lines):
                if index == 0:
                    other_args_to_resolve_lines.append(
                        f"{indent}{key}:"
                        + f"{indent}"
                        + "\n"
                        + f"{indent}{indent}{indent}"
                        + node_repr_line
                    )
                else:
                    other_args_to_resolve_lines.append(
                        f"{indent}{indent}" + node_repr_line
                    )
        else:
            other_args_to_resolve_lines.append(f"{indent}{key}: " + str(val))

    other_args_to_resolve_line = "{"
    for line in other_args_to_resolve_lines:
        other_args_to_resolve_line += f"\n{indent}{line}"
    other_args_to_resolve_line += f"\n{indent}}}"
    return other_args_to_resolve_line
