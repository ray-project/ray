#!/usr/bin/env python

# This is an executable script that runs an example of every single CliLogger
# function for demonstration purposes. Primarily useful for tuning color and
# other formatting.

from ray.autoscaler._private.cli_logger import cli_logger, cf

cli_logger.configure(log_style="auto", verbosity=999)

cli_logger.print(
    cf.bold("Bold ") + cf.italic("Italic ") + cf.underlined("Underlined"))
cli_logger.labeled_value("Label", "value")
cli_logger.print("List: {}", cli_logger.render_list([1, 2, 3]))
cli_logger.newline()
cli_logger.very_verbose("Very verbose")
cli_logger.verbose("Verbose")
cli_logger.verbose_warning("Verbose warning")
cli_logger.verbose_error("Verbose error")
cli_logger.print("Info")
cli_logger.success("Success")
cli_logger.warning("Warning")
cli_logger.error("Error")
cli_logger.newline()
try:
    cli_logger.abort("Abort")
except Exception:
    pass
try:
    cli_logger.doassert(False, "Assert")
except Exception:
    pass
cli_logger.newline()
cli_logger.confirm(True, "example")
cli_logger.newline()
with cli_logger.indented():
    cli_logger.print("Indented")
with cli_logger.group("Group"):
    cli_logger.print("Group contents")
with cli_logger.verbatim_error_ctx("Verbtaim error"):
    cli_logger.print("Error contents")
