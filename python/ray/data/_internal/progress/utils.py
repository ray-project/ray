import logging

from ray.util.debug import log_once

logger = logging.getLogger(__name__)


def truncate_operator_name(name: str, max_name_length: int) -> str:
    from ray.data.context import DataContext

    ctx = DataContext.get_current()
    if not ctx.enable_progress_bar_name_truncation or len(name) <= max_name_length:
        return name

    op_names = name.split("->")
    if len(op_names) == 1:
        return op_names[0]

    # Include as many operators as possible without approximately
    # exceeding `MAX_NAME_LENGTH`. Always include the first and
    # last operator names so it is easy to identify the DAG.
    truncated_op_names = [op_names[0]]
    for op_name in op_names[1:-1]:
        if (
            len("->".join(truncated_op_names))
            + len("->")
            + len(op_name)
            + len("->")
            + len(op_names[-1])
        ) > max_name_length:
            truncated_op_names.append("...")
            if log_once("ray_data_truncate_operator_name"):
                logger.warning(
                    f"Truncating long operator name to {max_name_length} "
                    "characters. To disable this behavior, set "
                    "`ray.data.DataContext.get_current()."
                    "DEFAULT_ENABLE_PROGRESS_BAR_NAME_TRUNCATION = False`."
                )
            break
        truncated_op_names.append(op_name)
    truncated_op_names.append(op_names[-1])
    return "->".join(truncated_op_names)
