from ray.dag import DAGNode
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
def get_dag_node_str(
    dag_node: DAGNode,
    body_line,
):
    indent = _get_indentation()

    def format_attr(val):
        formatted = _format_value(val)
        lines = formatted.split("\n")
        if len(lines) == 1:
            return lines[0]
        return "\n".join([lines[0]] + [f"{indent}{line}" for line in lines[1:]])

    options = (
        {k: v for k, v in dag_node._bound_options.items() if v}
        if dag_node._bound_options
        else {}
    )

    return (
        f"({dag_node.__class__.__name__}, {dag_node._stable_uuid})(\n"
        f"{indent}body={body_line}\n"
        f"{indent}args={format_attr(dag_node._bound_args)}\n"
        f"{indent}kwargs={format_attr(dag_node._bound_kwargs)}\n"
        f"{indent}options={format_attr(options)}\n"
        f"{indent}other_args_to_resolve={format_attr(dag_node._bound_other_args_to_resolve)}\n"
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

        # 3. Sort sets alphabetically to guarantee deterministic output across OSes!
        items_to_iterate = sorted(val, key=str) if isinstance(val, set) else val

        # 4. The unified formatting logic (only written once!)
        lines = []
        indent = _get_indentation()
        for ele in items_to_iterate:
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
