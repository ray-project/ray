#!/usr/bin/env python
#
# This file is based on
# https://github.com/llvm-mirror/clang-tools-extra/blob/5c40544fa40bfb85ec888b6a03421b3905e4a4e7/clang-tidy/tool/clang-tidy-diff.py
#
# ===- clang-tidy-diff.py - ClangTidy Diff Checker ----------*- python -*--===#
#
# Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
# See https://llvm.org/LICENSE.txt for license information.
# SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
#
# ===----------------------------------------------------------------------===#
r"""
ClangTidy Diff Checker
======================
This script reads input from a unified diff, runs clang-tidy on all changed
files and outputs clang-tidy warnings in changed lines only. This is useful to
detect clang-tidy regressions in the lines touched by a specific patch.
Example usage for git/svn users:
  git diff -U0 HEAD^ | clang-tidy-diff.py -p1
  svn diff --diff-cmd=diff -x-U0 | \
      clang-tidy-diff.py -fix -checks=-*,modernize-use-override
"""

import argparse
import glob
import json
import multiprocessing
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import traceback

try:
    import yaml
except ImportError:
    yaml = None

is_py2 = sys.version[0] == "2"

if is_py2:
    import Queue as queue
else:
    import queue as queue


def run_tidy(task_queue, lock, timeout):
    watchdog = None
    while True:
        command = task_queue.get()
        try:
            proc = subprocess.Popen(
                command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )

            if timeout is not None:
                watchdog = threading.Timer(timeout, proc.kill)
                watchdog.start()

            stdout, stderr = proc.communicate()

            with lock:
                sys.stdout.write(stdout.decode("utf-8") + "\n")
                sys.stdout.flush()
                if stderr:
                    sys.stderr.write(stderr.decode("utf-8") + "\n")
                    sys.stderr.flush()
        except Exception as e:
            with lock:
                sys.stderr.write("Failed: " + str(e) + ": ".join(command) + "\n")
        finally:
            with lock:
                if timeout is not None and watchdog is not None:
                    if not watchdog.is_alive():
                        sys.stderr.write(
                            "Terminated by timeout: " + " ".join(command) + "\n"
                        )
                    watchdog.cancel()
            task_queue.task_done()


def start_workers(max_tasks, tidy_caller, task_queue, lock, timeout):
    for _ in range(max_tasks):
        t = threading.Thread(target=tidy_caller, args=(task_queue, lock, timeout))
        t.daemon = True
        t.start()


def merge_replacement_files(tmpdir, mergefile):
    """Merge all replacement files in a directory into a single file"""
    # The fixes suggested by clang-tidy >= 4.0.0 are given under
    # the top level key 'Diagnostics' in the output yaml files
    mergekey = "Diagnostics"
    merged = []
    for replacefile in glob.iglob(os.path.join(tmpdir, "*.yaml")):
        content = yaml.safe_load(open(replacefile, "r"))
        if not content:
            continue  # Skip empty files.
        merged.extend(content.get(mergekey, []))

    if merged:
        # MainSourceFile: The key is required by the definition inside
        # include/clang/Tooling/ReplacementsYaml.h, but the value
        # is actually never used inside clang-apply-replacements,
        # so we set it to '' here.
        output = {"MainSourceFile": "", mergekey: merged}
        with open(mergefile, "w") as out:
            yaml.safe_dump(output, out)
    else:
        # Empty the file:
        open(mergefile, "w").close()


def main():
    parser = argparse.ArgumentParser(
        description="Run clang-tidy against changed files, and "
        "output diagnostics only for modified "
        "lines."
    )
    parser.add_argument(
        "-clang-tidy-binary",
        metavar="PATH",
        default="clang-tidy",
        help="path to clang-tidy binary",
    )
    parser.add_argument(
        "-p",
        metavar="NUM",
        default=0,
        help="strip the smallest prefix containing P slashes",
    )
    parser.add_argument(
        "-regex",
        metavar="PATTERN",
        default=None,
        help="custom pattern selecting file paths to check "
        "(case sensitive, overrides -iregex)",
    )
    parser.add_argument(
        "-iregex",
        metavar="PATTERN",
        default=r".*\.(cpp|cc|c\+\+|cxx|c|cl|h|hpp|m|mm|inc)",
        help="custom pattern selecting file paths to check "
        "(case insensitive, overridden by -regex)",
    )
    parser.add_argument(
        "-j",
        type=int,
        default=1,
        help="number of tidy instances to be run in parallel.",
    )
    parser.add_argument(
        "-timeout", type=int, default=None, help="timeout per each file in seconds."
    )
    parser.add_argument(
        "-fix", action="store_true", default=False, help="apply suggested fixes"
    )
    parser.add_argument(
        "-checks",
        help="checks filter, when not specified, use clang-tidy " "default",
        default="",
    )
    parser.add_argument(
        "-path", dest="build_path", help="Path used to read a compile command database."
    )
    if yaml:
        parser.add_argument(
            "-export-fixes",
            metavar="FILE",
            dest="export_fixes",
            help="Create a yaml file to store suggested fixes in, "
            "which can be applied with clang-apply-replacements.",
        )
    parser.add_argument(
        "-extra-arg",
        dest="extra_arg",
        action="append",
        default=[],
        help="Additional argument to append to the compiler " "command line.",
    )
    parser.add_argument(
        "-extra-arg-before",
        dest="extra_arg_before",
        action="append",
        default=[],
        help="Additional argument to prepend to the compiler " "command line.",
    )
    parser.add_argument(
        "-quiet",
        action="store_true",
        default=False,
        help="Run clang-tidy in quiet mode",
    )
    clang_tidy_args = []
    argv = sys.argv[1:]
    if "--" in argv:
        clang_tidy_args.extend(argv[argv.index("--") :])
        argv = argv[: argv.index("--")]

    args = parser.parse_args(argv)

    # Extract changed lines for each file.
    filename = None
    lines_by_file = {}
    for line in sys.stdin:
        match = re.search('^\+\+\+\ "?(.*?/){%s}([^ \t\n"]*)' % args.p, line)
        if match:
            filename = match.group(2)
        if filename is None:
            continue

        if args.regex is not None:
            if not re.match("^%s$" % args.regex, filename):
                continue
        else:
            if not re.match("^%s$" % args.iregex, filename, re.IGNORECASE):
                continue

        match = re.search("^@@.*\+(\d+)(,(\d+))?", line)
        if match:
            start_line = int(match.group(1))
            line_count = 1
            if match.group(3):
                line_count = int(match.group(3))
            if line_count == 0:
                continue
            end_line = start_line + line_count - 1
            lines_by_file.setdefault(filename, []).append([start_line, end_line])

    if not any(lines_by_file):
        print("No relevant changes found.")
        sys.exit(0)

    max_task_count = args.j
    if max_task_count == 0:
        max_task_count = multiprocessing.cpu_count()
    max_task_count = min(len(lines_by_file), max_task_count)

    tmpdir = None
    if yaml and args.export_fixes:
        tmpdir = tempfile.mkdtemp()

    # Tasks for clang-tidy.
    task_queue = queue.Queue(max_task_count)
    # A lock for console output.
    lock = threading.Lock()

    # Run a pool of clang-tidy workers.
    start_workers(max_task_count, run_tidy, task_queue, lock, args.timeout)

    # Form the common args list.
    common_clang_tidy_args = []
    if args.fix:
        common_clang_tidy_args.append("-fix")
    if args.checks != "":
        common_clang_tidy_args.append("-checks=" + args.checks)
    if args.quiet:
        common_clang_tidy_args.append("-quiet")
    if args.build_path is not None:
        common_clang_tidy_args.append("-p=%s" % args.build_path)
    for arg in args.extra_arg:
        common_clang_tidy_args.append("-extra-arg=%s" % arg)
    for arg in args.extra_arg_before:
        common_clang_tidy_args.append("-extra-arg-before=%s" % arg)

    for name in lines_by_file:
        line_filter_json = json.dumps(
            [{"name": name, "lines": lines_by_file[name]}], separators=(",", ":")
        )

        # Run clang-tidy on files containing changes.
        command = [args.clang_tidy_binary]
        command.append("-line-filter=" + line_filter_json)
        if yaml and args.export_fixes:
            # Get a temporary file. We immediately close the handle so
            # clang-tidy can overwrite it.
            (handle, tmp_name) = tempfile.mkstemp(suffix=".yaml", dir=tmpdir)
            os.close(handle)
            command.append("-export-fixes=" + tmp_name)
        command.extend(common_clang_tidy_args)
        command.append(name)
        command.extend(clang_tidy_args)

        task_queue.put(command)

    # Wait for all threads to be done.
    task_queue.join()

    if yaml and args.export_fixes:
        print("Writing fixes to " + args.export_fixes + " ...")
        try:
            merge_replacement_files(tmpdir, args.export_fixes)
        except Exception:
            sys.stderr.write("Error exporting fixes.\n")
            traceback.print_exc()

    if tmpdir:
        shutil.rmtree(tmpdir)


if __name__ == "__main__":
    main()
