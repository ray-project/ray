#!/usr/bin/env python

import ast
import json
import os
import re
import subprocess
import sys

from collections import defaultdict


def textproto_format(space, key, value, json_encoder=None):
    if json_encoder is None:
        value = {b"true": b"True", b"false": b"False", b"null": b"None"}.get(value, value)
    elif value.startswith(b"\""):
        value = json_encoder.encode(ast.literal_eval(value.decode("utf-8"))).encode("utf-8")
    return b"%s[\"%s\", %s]" % (space, key, value)


def textproto_split(input_lines, json_encoder=None):
    """When given e.g. the output of 'bazel aquery --output=textproto', yields each top-level item
    as a string formatted as JSON (if an encoder is given) or Python AST.
    The input MUST be formatted neatly line-by-line, not any arbitary textproto.
    """
    outputs = []
    re_flags = re.M
    pat_open = re.compile(b"^(\\s*)([-\\w:]+)(\\s*){$", flags=re_flags)
    pat_line = re.compile(b"^(\\s*)([-\\w]+): (.*)$", flags=re_flags)
    pat_close = re.compile(b"}$", flags=re_flags)
    prev_comma = False
    prev_tail = b""
    for full_line in input_lines:
        assert full_line.endswith(b"\n") and b"\n" not in full_line[:-1]  # check if line by line
        pieces = re.split(b"(\\r|\\n)", full_line, 1)
        pieces[1:] = [b"".join(pieces[1:])]
        [line, tail] = pieces
        next_line = pat_open.sub(b"\\1[\"\\2\",\\3[", line)
        outputs.append(b"" if not prev_comma else b"]" if next_line.endswith(b"}") else b",")
        next_line = pat_close.sub(b"]", next_line)
        next_line = pat_line.sub(lambda m: textproto_format(*(m.groups() + (json_encoder,))),
                                 next_line)
        outputs.append(prev_tail + next_line)
        if line == b"}":
            yield b"".join(outputs)
            del outputs[:]
        prev_comma = line != b"}" and (next_line.endswith(b"]") or next_line.endswith(b"\""))
        prev_tail = tail
    if len(outputs) > 0:
        yield b"".join(outputs)
        del outputs[:]


def aquery_parse_shell_calls(stdin, *bazel_command):
    proc = None
    try:
        json_encoder = json.JSONEncoder()
        if len(bazel_command) > 0:
            proc = subprocess.Popen(bazel_command, stdout=subprocess.PIPE)
            stdin = proc.stdout
        else:
            assert not stdin.isatty(), "stdin should be a file or a pipe, not a terminal"
        for item in textproto_split(stdin, json_encoder=json_encoder):
            parsed = json.loads(item) if json_encoder else ast.literal_eval(item.decode("utf-8"))
            (parsed_key, aquery_data) = parsed
            if parsed_key == "actions":
                [mnemonic] = [pair[1] for pair in aquery_data if pair[0] == "mnemonic"]
                if mnemonic == "Genrule":
                    yield [pair[1] for pair in aquery_data if pair[0] == "arguments"]
    finally:
        if proc:
            proc.wait()


def bazel_info_query(key, bazel="bazel"):
    lines = subprocess.check_output([bazel, "info"]).splitlines()
    [result] = [line.split(b": ", 1)[1] for line in lines if line.startswith(key + b":")]
    return result


def run_shellcheck(stdin, bazel_aquery, *shellcheck_args):
    bazel = os.getenv("BAZEL", "bazel")
    shellcheck_args = list(shellcheck_args) if len(shellcheck_args) > 0 else ["shellcheck"]
    all_shells_script_infos = defaultdict(lambda: [])
    bazel_command = [bazel, "aquery", "--show_progress=no", "--include_artifacts=false",
                     "--output=textproto", bazel_aquery]
    for shell_args in aquery_parse_shell_calls(stdin, *bazel_command):
        shell_name = os.path.basename(os.path.splitext(shell_args[0])[0]).lower()
        finished_options = False
        i = 1
        while i < len(shell_args):
            if finished_options or not shell_args[i].startswith("-"):
                all_shells_script_infos[shell_name].append((shell_args[i], None))
            elif shell_args[i] == "--":
                finished_options = True
            elif shell_args[i] in ("-o", "+o"):
                i += 1
            elif shell_args[i] == "-c":
                all_shells_script_infos[shell_name].append((None, shell_args[i + 1]))
                break
            i += 1
    result = 0
    for shell, script_infos in all_shells_script_infos.items():
        scripts_combined = []
        has_stdin = False
        filenames = []
        for script_file, script_text in script_infos:
            if script_file is not None:
                filenames.append(script_file)
            if script_text is not None:
                has_stdin = True
                if 'host/bin/external/com_github_google_flatbuffers/flatc' not in script_text:
                    scripts_combined.append("\n".join(["if test -t 0; then", script_text, "fi"]))
        if has_stdin:
            filenames.insert(0, "-")
        cmdargs = shellcheck_args + ["--shell=" + shell, "--external-sources"] + filenames
        cwd = bazel_info_query(b"execution_root", bazel)
        proc = subprocess.Popen(cmdargs, stdin=subprocess.PIPE, cwd=cwd)
        try:
            proc.communicate("\n".join(scripts_combined).encode("utf-8"))
        finally:
            proc.wait()
        result = result and proc.returncode
    return result


def main(program, command, *command_args):
    stdin = sys.stdin.buffer if sys.version_info[0] >= 3 else sys.stdin
    result = 0
    if command == run_shellcheck.__name__:
        result = run_shellcheck(stdin, *command_args)
    else:
        raise ValueError("Unrecognized command: " + command)
    return result


if __name__ == "__main__":
    sys.exit(main(*sys.argv) or 0)
