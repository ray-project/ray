"""
Tracing utils
"""
from .callers import _Result


class TagTracer(object):
    def __init__(self):
        self._tag2proc = {}
        self.writer = None
        self.indent = 0

    def get(self, name):
        return TagTracerSub(self, (name,))

    def format_message(self, tags, args):
        if isinstance(args[-1], dict):
            extra = args[-1]
            args = args[:-1]
        else:
            extra = {}

        content = " ".join(map(str, args))
        indent = "  " * self.indent

        lines = ["%s%s [%s]\n" % (indent, content, ":".join(tags))]

        for name, value in extra.items():
            lines.append("%s    %s: %s\n" % (indent, name, value))
        return lines

    def processmessage(self, tags, args):
        if self.writer is not None and args:
            lines = self.format_message(tags, args)
            self.writer("".join(lines))
        try:
            self._tag2proc[tags](tags, args)
        except KeyError:
            pass

    def setwriter(self, writer):
        self.writer = writer

    def setprocessor(self, tags, processor):
        if isinstance(tags, str):
            tags = tuple(tags.split(":"))
        else:
            assert isinstance(tags, tuple)
        self._tag2proc[tags] = processor


class TagTracerSub(object):
    def __init__(self, root, tags):
        self.root = root
        self.tags = tags

    def __call__(self, *args):
        self.root.processmessage(self.tags, args)

    def setmyprocessor(self, processor):
        self.root.setprocessor(self.tags, processor)

    def get(self, name):
        return self.__class__(self.root, self.tags + (name,))


class _TracedHookExecution(object):
    def __init__(self, pluginmanager, before, after):
        self.pluginmanager = pluginmanager
        self.before = before
        self.after = after
        self.oldcall = pluginmanager._inner_hookexec
        assert not isinstance(self.oldcall, _TracedHookExecution)
        self.pluginmanager._inner_hookexec = self

    def __call__(self, hook, hook_impls, kwargs):
        self.before(hook.name, hook_impls, kwargs)
        outcome = _Result.from_call(lambda: self.oldcall(hook, hook_impls, kwargs))
        self.after(outcome, hook.name, hook_impls, kwargs)
        return outcome.get_result()

    def undo(self):
        self.pluginmanager._inner_hookexec = self.oldcall
