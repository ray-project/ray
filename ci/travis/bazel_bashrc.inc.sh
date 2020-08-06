# shellcheck shell=bash
# ^ Avoid shebang line. (This script must be sourced, not executed.)

# This is the content to be included in Travis bashrc

# Inject the wrapper function to bashrc, so in future invocation
# we get a shorthand to export logs to a temp file on each
# test/build event.
export_result_opt() {
  mkdir -p /tmp/bazel_event_logs

  echo "--build_event_json_file $(mktemp /tmp/bazel_event_logs/bazel_log.XXXXX)"
}
