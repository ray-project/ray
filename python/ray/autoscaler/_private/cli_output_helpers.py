"""Lightweight CLI output helpers for cluster launcher context notes.

These functions produce informational output that disambiguates head-node
commands from local-machine commands.  They have NO dependency on the ``ray``
C++ runtime and accept ``cli_logger`` / ``cf`` as explicit parameters so they
can be tested in isolation.
"""


def print_next_steps_context_note(cli_logger, cf):
    """Print a dimmed note at the top of the 'Next steps' block.

    Informs the user that the commands below are intended for the head node
    or for machines within the cluster network.
    """
    cli_logger.print(
        cf.dimmed("Note: The following commands are intended for use on")
    )
    cli_logger.print(
        cf.dimmed("the head node or within the cluster network.")
    )
    cli_logger.newline()


def print_head_node_context_separator(cli_logger, cf):
    """Print a visual separator and context note after head-node output.

    Used by ``ray up`` to separate the streamed ``ray start`` output from
    the local-machine commands that follow.
    """
    cli_logger.print(cf.dimmed("-" * 60))
    cli_logger.print(
        cf.dimmed(
            "Note: The output above is from the head node (via `ray start`)."
        )
    )
    cli_logger.print(
        cf.dimmed(
            "  Commands shown in 'Next steps' may only work from the head node"
        )
    )
    cli_logger.print(cf.dimmed("  or from within the cluster network."))
    cli_logger.newline()


# Group heading used by ``ray up`` for the local commands section.
USEFUL_COMMANDS_HEADING = "Useful commands for your local machine:"
