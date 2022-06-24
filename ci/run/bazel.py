#!/usr/bin/env python

import ast
import errno
import json
import os
import re
import subprocess
import stat
import sys

from collections import defaultdict, OrderedDict


def textproto_format(space, key, value, json_encoder):
    """Rewrites a key-value pair from textproto as JSON."""
    if value.startswith(b'"'):
        evaluated = ast.literal_eval(value.decode("utf-8"))
        value = json_encoder.encode(evaluated).encode("utf-8")
    return b'%s["%s", %s]' % (space, key, value)


def textproto_split(input_lines, json_encoder):
    """When given e.g. the output of "bazel aquery --output=textproto",
    yields each top-level item as a string formatted as JSON (if an encoder is
    given) or Python AST.
    The input MUST be formatted neatly line-by-line, as follows:
    actions {
        mnemonic: "Genrule"
        environment_variables {
            key: "CC"
            value: "clang"
        }
        ...
    }
    targets {
        id: "0"
        label: "//:target"
        rule_class_id: "0"
    }
    """
    outputs = []
    re_flags = re.M
    pat_open = re.compile(b"^(\\s*)([-\\w:]+)(\\s*){$", flags=re_flags)
    pat_line = re.compile(b"^(\\s*)([-\\w]+): (.*)$", flags=re_flags)
    pat_close = re.compile(b"}$", flags=re_flags)
    prev_comma = False
    prev_tail = b""
    for full_line in input_lines:
        pieces = re.split(b"(\\r|\\n)", full_line, 1)
        pieces[1:] = [b"".join(pieces[1:])]
        [line, tail] = pieces
        next_line = pat_open.sub(b'\\1["\\2",\\3[', line)
        outputs.append(
            b"" if not prev_comma else b"]" if next_line.endswith(b"}") else b","
        )
        next_line = pat_close.sub(b"]", next_line)
        next_line = pat_line.sub(
            lambda m: textproto_format(*(m.groups() + (json_encoder,))), next_line
        )
        outputs.append(prev_tail + next_line)
        if line == b"}":
            yield b"".join(outputs)
            del outputs[:]
        prev_comma = line != b"}" and (
            next_line.endswith(b"]") or next_line.endswith(b'"')
        )
        prev_tail = tail
    if len(outputs) > 0:
        yield b"".join(outputs)
        del outputs[:]


def textproto_parse(stream, encoding, json_encoder):
    for item in textproto_split(stream, json_encoder):
        yield json.loads(item.decode(encoding))


class Bazel(object):
    encoding = "utf-8"

    def __init__(self, program=None):
        if program is None:
            program = os.getenv("BAZEL_EXECUTABLE", "bazel")
        self.argv = (program,)
        self.extra_args = ("--show_progress=no",)

    def _call(self, command, *args):
        return subprocess.check_output(
            self.argv + (command,) + args[:1] + self.extra_args + args[1:],
            stdin=subprocess.PIPE,
        )

    def info(self, *args):
        result = OrderedDict()
        for line in self._call("info", *args).splitlines():
            (key, value) = line.split(b":", 1)
            if value.startswith(b" "):
                value = value[1:]
            result[key.decode(self.encoding)] = value.decode(self.encoding)
        return result

    def aquery(self, *args):
        out = self._call("aquery", "--output=jsonproto", *args)
        return json.loads(out.decode(self.encoding))


def parse_aquery_shell_calls(aquery_results):
    """Extracts and yields the command lines representing the genrule() rules
    from Bazel aquery results.
    """
    for action in aquery_results["actions"]:
        if action["mnemonic"] != "Genrule":
            continue
        yield action["arguments"]


def parse_aquery_output_artifacts(aquery_results):
    """Extracts and yields the file paths representing the output artifact
    from the provided Bazel aquery results.

    To understand the output of aquery command in textproto format, try:
        bazel aquery --include_artifacts=true --output=jsonproto \
            'mnemonic("Genrule", deps(//:*))'
    """
    fragments = {}
    for fragment in aquery_results["pathFragments"]:
        fragments[fragment["id"]] = fragment

    artifacts = {}
    for artifact in aquery_results["artifacts"]:
        artifacts[artifact["id"]] = artifact

    def _path(fragment_id):
        fragment = fragments[fragment_id]
        parent = _path(fragment["parentId"]) if "parentId" in fragment else []
        return parent + [fragment["label"]]

    for action in aquery_results["actions"]:
        for output_id in action["outputIds"]:
            path = os.path.join(*_path(artifacts[output_id]["pathFragmentId"]))
            yield path


def textproto2json(infile, outfile):
    """Translates the output of bazel aquery --output=textproto into JSON.
    Useful for later command-line manipulation.

    Args:
        infile: The binary input stream.
        outfile: The binary output stream.
    """
    json_encoder = json.JSONEncoder(indent=2)
    encoding = "utf-8"
    for obj in textproto_parse(infile, encoding, json_encoder):
        outfile.write((json_encoder.encode(obj) + "\n").encode(encoding))


def preclean(bazel_aquery):
    """Cleans up any genrule() outputs for the provided target(s).

    This is useful for forcing genrule actions to re-run, because the _true_
    outputs of those actions can include a larger set of files (e.g. files
    copied to the workspace) which Bazel is unable to detect changes to (or
    delete changes of).

    Usually, you would run this script along with 'git clean -f', to make sure
    Bazel re-copies outputs the next time a build occurs.
    """
    result = 0
    bazel = Bazel()
    aquery_results = bazel.aquery("--include_artifacts=true", bazel_aquery)
    for path in parse_aquery_output_artifacts(aquery_results):
        try:
            if sys.platform == "win32":
                os.chmod(path, stat.S_IWRITE)  # Needed to remove read-only bit
            os.remove(path)
        except IOError as ex:
            if ex.errno != errno.ENOENT:
                sys.stderr.write(str(ex) + "\n")
                result = result or ex.errno
    return result


def shellcheck(bazel_aquery, *shellcheck_argv):
    """Runs shellcheck with the provided argument(s) on all targets that match
    the given Bazel aquery.

    Args:
        bazel_aquery: A Bazel aquery expression (e.g. "//:*")
        shellcheck_argv: The command-line arguments to call for shellcheck.
            Note that the first entry should be the shellcheck program itself.
            If omitted, will simply call "shellcheck".

    Returns:
        The exit code of shellcheck.
    """
    bazel = Bazel()
    shellcheck_argv = list(shellcheck_argv) or ["shellcheck"]
    all_script_infos = defaultdict(lambda: [])
    aquery_results = bazel.aquery("--include_artifacts=false", bazel_aquery)
    shell_calls = list(parse_aquery_shell_calls(aquery_results))
    for shell_args in shell_calls:
        shname = os.path.basename(os.path.splitext(shell_args[0])[0]).lower()
        finished_options = False
        i = 1
        while i < len(shell_args):
            if finished_options or not shell_args[i].startswith("-"):
                all_script_infos[shname].append((shell_args[i], None))
            elif shell_args[i] == "--":
                finished_options = True
            elif shell_args[i] in ("-o", "+o"):
                i += 1
            elif shell_args[i] == "-c":
                all_script_infos[shname].append((None, shell_args[i + 1]))
                break
            i += 1

    result = 0
    bazel_execution_root = None
    for shell, script_infos in all_script_infos.items():
        scripts_combined = []
        has_stdin = False
        filenames = []
        for script_file, script_text in script_infos:
            if script_file is not None:
                filenames.append(script_file)
            if script_text is not None:
                has_stdin = True
                flatc = "host/bin/external/com_github_google_flatbuffers/flatc"
                if flatc not in script_text:
                    statements = ["if test -t 0; then", script_text, "fi"]
                    scripts_combined.append("\n".join(statements))
        if has_stdin:
            filenames.insert(0, "-")
        if shell.endswith("sh"):
            if bazel_execution_root is None:
                bazel_execution_root = bazel.info()["execution_root"]
            cwd = bazel_execution_root
            cmdargs = ["--shell=" + shell, "--external-sources"] + filenames
            cmdargs = shellcheck_argv + cmdargs
            proc = subprocess.Popen(cmdargs, stdin=subprocess.PIPE, cwd=cwd)
            try:
                proc.communicate("\n".join(scripts_combined).encode("utf-8"))
            finally:
                proc.wait()
            result = result or proc.returncode
    return result


def main(program, command, *command_args):
    result = 0
    if command == textproto2json.__name__:
        result = textproto2json(sys.stdin.buffer, sys.stdout.buffer, *command_args)
    elif command == shellcheck.__name__:
        result = shellcheck(*command_args)
    elif command == preclean.__name__:
        result = preclean(*command_args)
    else:
        raise ValueError("Unrecognized command: " + command)
    return result


if __name__ == "__main__":
    sys.exit(main(*sys.argv) or 0)
