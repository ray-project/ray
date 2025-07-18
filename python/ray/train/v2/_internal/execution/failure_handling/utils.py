from typing import Dict, Union


def get_error_string(controller_error: Union[Exception, Dict[int, Exception]]) -> str:
    """Format an error or dictionary of errors into a readable string.

    Args:
        scheduling_or_poll_error: Either a single exception or a dictionary
            mapping worker ranks to their respective exceptions.

    Returns:
        A formatted string representation of the error(s).
    """
    if isinstance(scheduling_or_poll_error, Exception):
        return f"{scheduling_or_poll_error}"
    else:
        return "\n".join(
            f"[Rank {world_rank}]\n{error}"
            for world_rank, error in scheduling_or_poll_error.items()
        )
