import re
from typing import List

TRACEBACK_PATTERN = "Traceback (most recent call last)"


class LogAggregator:
    def __init__(self, log: str):
        self.log = log

    def compute_crash_pattern(self) -> str:
        stack_trace = LogAggregator._compute_stack_trace(self.log.splitlines())
        # truncate short enough to store in databases, but long enough to keep the
        # pattern unique
        return LogAggregator._compute_signature(stack_trace)[:4000]

    @staticmethod
    def _compute_signature(stack_trace: List[str]) -> str:
        """
        Compute signature pattern from stack trace, by remove factors such as date,
        time, temp directory, line numbers, etc. This help to aggregate similar logs
        into same bug patterns
        """
        massaged_trace = []
        for line in stack_trace:
            # remove any hashes that are more than 10 characters
            line = re.sub(r"[a-z0-9]{10,}", "", line.strip())
            # remove any numbers
            line = re.sub(r"\d", "", line)
            if line == "Traceback (most recent call last):":
                continue
            file_line = re.search(r'File "(.*)", (.*)', line)
            if file_line:
                # append the file's base name and caller information; the result string
                # is not something meaningful to human, we just need something that
                # uniquely represent the stack trace
                line = f'{file_line.group(1).split("/")[-1]}{file_line.group(2)}'
            massaged_trace.append(line)
        return "".join(massaged_trace)

    @staticmethod
    def _compute_stack_trace(logs: List[str]) -> List[str]:
        """
        Extract stack trace pattern from the logs. Stack trace pattern often matches
        the following:
        ERROR ...
        Traceback (most recent call last):
            File "...", line ..., in ...
            ...
        Exception: exception error
        """
        error_stacktrace = []
        stacktrace = []
        i = 0
        while i < len(logs):
            stack = []
            trace = error_stacktrace
            # Search for lines that are either
            # ... ERROR ...
            # or
            # ... ERROR ...
            # Traceback (most recent call last):
            if "ERROR" in logs[i]:
                stack.append(logs[i])
                next = i + 1
                if i + 1 < len(logs) and TRACEBACK_PATTERN in logs[i + 1]:
                    stack.append(logs[i + 1])
                    next = i + 2
            # Or if the line with ERROR does not exist, just search for the line with
            # Traceback (most recent call last):
            elif TRACEBACK_PATTERN in logs[i]:
                stack.append(logs[i])
                trace = stacktrace
                next = i + 1
            # Or else, skip this line and continue
            else:
                i = i + 1
                continue
            # If the line that contains ERROR, Traceback, etc. is found, scan the logs
            # until the line no longer has indentation. This is because stack trace
            # is always indented, and stops when the line is no longer indented
            while next < len(logs):
                if logs[next].startswith((" ", "\t")):
                    stack.append(logs[next])
                    next = next + 1
                else:
                    break
            # Finished capturing the entire stack trace
            if next < len(logs):
                stack.append(logs[next])
            if stack:
                trace.append(stack)
            i = next + 1

        # Favor stack trace that contains the ERROR keyword
        if error_stacktrace:
            return error_stacktrace[-1]

        # Otherwise any stack trace is fine
        if stacktrace:
            return stacktrace[-1]

        return []
