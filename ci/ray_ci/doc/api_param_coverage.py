"""
Static, diff-scoped parameter-coverage check for Ray's public API surface.

Fails when a pull request adds a new ``@PublicAPI`` callable, or a new
parameter to an existing one, without a matching entry in the docstring
``Args:`` block. The intent is "no new debt": pre-existing undocumented
parameters are grandfathered, and only newly-undocumented parameters on the
changed public surface are reported.

This is the ``@PublicAPI``-aware, diff-scoped custom check (no baseline file)
that sits alongside ``cmd_check_api_discrepancy.py`` in the same doc-guard
family. Unlike that check, it works entirely from source via ``ast`` and needs
no Ray build or import environment: it parses the base-branch and working-tree
versions of the changed files and diffs their undocumented-parameter sets.

Only *presence* is checked. A parameter counts as documented if its name
appears in an ``Args:`` / ``Arguments:`` block (or, for ``__init__``, in the
class docstring, matching Ray's default ``autoclass_content="class"``); the
description prose is not inspected. Empty-description detection is out of scope
(it needs ``numpydoc.validate`` or a custom rule).

The parsing and docstring-inheritance logic mirrors the standing-gap audit in
anyscale/docs (``strategy/doc-infra-backlog/scripts/typeless-param-audit.py``);
this is its forward-looking, diff-scoped counterpart.
"""
import ast
import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Set, Tuple, Union

_FuncNode = Union[ast.FunctionDef, ast.AsyncFunctionDef]

# Google-style "Args:"/"Arguments:" section headers.
_GOOGLE_SECTION = re.compile(
    r"^\s*(Args|Arguments|Keyword Args|Keyword Arguments)\s*:\s*$", re.M
)
# A documented param line: "    name (type): ..." or "    name: ...".
_GOOGLE_PARAM = re.compile(r"^\s+([*]{0,2}[A-Za-z_]\w*)\s*(\(|:)")
# Section headers that end an Args block.
_NEXT_SECTION = re.compile(
    r"^\s*(Returns|Return|Yields|Raises|Examples?|Example|Note|Notes|Warning|"
    r"Warnings|See Also|References|Attributes|Todo)\s*:\s*$",
    re.M,
)


def documented_params(docstring: Optional[str]) -> Set[str]:
    """Return the set of parameter names documented in a docstring's Args section(s)."""
    if not docstring:
        return set()
    documented = set()
    in_args = False
    args_indent = None
    for line in docstring.splitlines():
        if _GOOGLE_SECTION.match(line):
            in_args = True
            args_indent = None
            continue
        if in_args:
            if line.strip() == "":
                continue
            if _NEXT_SECTION.match(line):
                in_args = False
                continue
            indent = len(line) - len(line.lstrip())
            if args_indent is None:
                args_indent = indent
            elif indent < args_indent:
                # Dedent below the first param's indent ends the Args block,
                # even without an explicit next-section header.
                in_args = False
                continue
            m = _GOOGLE_PARAM.match(line)
            if m and indent <= args_indent + 1:
                documented.add(m.group(1).lstrip("*"))
    return documented


def has_publicapi_decorator(
    node: Union[ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef]
) -> bool:
    """Whether a class/function node carries an ``@PublicAPI`` decorator.

    Matches bare ``@PublicAPI`` and called ``@PublicAPI(...)`` forms, and both
    ``PublicAPI`` (imported name) and ``annotations.PublicAPI`` (attribute)
    spellings. Only ``@PublicAPI`` counts; ``@DeveloperAPI`` and ``@Deprecated``
    are not part of the rendered public API surface.
    """
    for dec in node.decorator_list:
        target = dec.func if isinstance(dec, ast.Call) else dec
        name = None
        if isinstance(target, ast.Name):
            name = target.id
        elif isinstance(target, ast.Attribute):
            name = target.attr
        if name == "PublicAPI":
            return True
    return False


# Annotations that take a callable back out of the rendered public surface even
# when it sits on an ``@PublicAPI`` class.
_NON_PUBLIC_ANNOTATIONS = frozenset({"DeveloperAPI", "Deprecated"})


def has_non_public_annotation(node: _FuncNode) -> bool:
    """Whether a method carries its own non-public API annotation.

    A method of an ``@PublicAPI`` class inherits public scope from the class, but
    an explicit ``@DeveloperAPI`` or ``@Deprecated`` on the method overrides that:
    the callable is not part of the rendered public API surface, so its
    parameters are out of scope for this check.
    """
    for dec in node.decorator_list:
        target = dec.func if isinstance(dec, ast.Call) else dec
        name = None
        if isinstance(target, ast.Name):
            name = target.id
        elif isinstance(target, ast.Attribute):
            name = target.attr
        if name in _NON_PUBLIC_ANNOTATIONS:
            return True
    return False


def signature_params(func: _FuncNode) -> List[str]:
    """Return every named signature parameter, in order.

    Excludes ``self``/``cls`` and the ``*args``/``**kwargs`` catch-alls (which
    are not conventionally documented per-name). All remaining positional,
    positional-only, and keyword-only parameters are included regardless of
    whether they carry a type annotation, per the confirmed all-params scope.
    """
    a = func.args
    posonly = getattr(a, "posonlyargs", [])
    return [
        arg.arg
        for arg in (posonly + a.args + a.kwonlyargs)
        if arg.arg not in ("self", "cls")
    ]


def _base_names(classdef: ast.ClassDef) -> List[str]:
    """Simple names of a class's declared bases."""
    names = []
    for b in classdef.bases:
        if isinstance(b, ast.Name):
            names.append(b.id)
        elif isinstance(b, ast.Attribute):
            names.append(b.attr)
    return names


@dataclass
class ClassIndex:
    """Docstring-inheritance index, keyed by simple class name.

    Sphinx's default ``autodoc_inherit_docstrings=True`` means a method with no
    own docstring inherits its base's docstring, and ``__init__`` params are
    documented on the class docstring under ``autoclass_content="class"``. This
    index lets the check subtract out params that are documented on a base, so
    an override with no own docstring is not flagged when a base documents the
    parameter. Best-effort: base classes are resolved by simple name, so
    external, aliased, or dynamically-built bases are not seen.
    """

    bases: Dict[str, List[str]] = field(default_factory=lambda: defaultdict(list))
    # class -> {method -> documented-param set}, only for methods with an OWN
    # docstring (key present == has own doc, which stops getdoc's MRO walk).
    method_own: Dict[str, Dict[str, Set[str]]] = field(
        default_factory=lambda: defaultdict(dict)
    )
    class_doc: Dict[str, Set[str]] = field(default_factory=lambda: defaultdict(set))

    def inherited_method_params(
        self, class_name: str, method_name: str, _seen=None
    ) -> Set[str]:
        """Params an override with no own docstring recovers from a base method."""
        if _seen is None:
            _seen = set()
        for base in self.bases.get(class_name, []):
            if base in _seen:
                continue
            _seen.add(base)
            if method_name in self.method_own.get(base, {}):
                return set(self.method_own[base][method_name])
            got = self.inherited_method_params(base, method_name, _seen)
            if got:
                return got
        return set()

    def inherited_class_params(self, class_name: str, _seen=None) -> Set[str]:
        """Params a class with no own docstring recovers from a base class docstring."""
        if _seen is None:
            _seen = set()
        for base in self.bases.get(class_name, []):
            if base in _seen:
                continue
            _seen.add(base)
            if base in self.class_doc:
                return set(self.class_doc[base])
            got = self.inherited_class_params(base, _seen)
            if got:
                return got
        return set()


def build_class_index(files: Iterable[Tuple[str, str]]) -> ClassIndex:
    """Build a :class:`ClassIndex` from ``(path, source)`` pairs.

    Walks every class in every file (public or not) so docstring inheritance
    can be resolved. Same simple-name collisions across modules are merged
    (union of documented params, concatenated bases). Files that fail to parse
    are skipped.
    """
    index = ClassIndex()
    for _path, source in files:
        try:
            tree = ast.parse(source)
        except (SyntaxError, ValueError):
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.ClassDef):
                continue
            cn = node.name
            for b in _base_names(node):
                if b not in index.bases[cn]:
                    index.bases[cn].append(b)
            cdoc = ast.get_docstring(node)
            if cdoc:
                index.class_doc[cn] |= documented_params(cdoc)
            for sub in node.body:
                if isinstance(sub, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    mdoc = ast.get_docstring(sub)
                    if mdoc is not None:  # own docstring -> stops MRO walk
                        index.method_own[cn].setdefault(sub.name, set())
                        index.method_own[cn][sub.name] |= documented_params(mdoc)
    return index


@dataclass
class Callable_:
    """A public callable's coverage state at one tree revision."""

    qualname: str
    lineno: int
    signature: List[str]
    undocumented: Set[str]


def _undocumented_for_func(
    func: _FuncNode,
    qual: str,
    index: ClassIndex,
    class_doc_node: Optional[ast.ClassDef],
) -> Optional[Callable_]:
    """Coverage state for one function/method, or None if it is not in scope.

    Out of scope: private names other than ``__init__``, and callables with no
    signature parameters (nothing to document).
    """
    if func.name.startswith("_") and func.name != "__init__":
        return None
    sig = signature_params(func)
    if not sig:
        return None

    own_doc = ast.get_docstring(func)
    documented = documented_params(own_doc)
    if func.name == "__init__" and class_doc_node is not None:
        documented |= documented_params(ast.get_docstring(class_doc_node))

    # Docstring-inheritance recovery for methods of a class.
    if "." in qual:
        class_name = qual.split(".", 1)[0]
        if own_doc is None:
            documented |= index.inherited_method_params(class_name, func.name)
        if func.name == "__init__" and (
            class_doc_node is None or ast.get_docstring(class_doc_node) is None
        ):
            documented |= index.inherited_class_params(class_name)

    undocumented = {p for p in sig if p not in documented}
    return Callable_(
        qualname=qual, lineno=func.lineno, signature=sig, undocumented=undocumented
    )


def public_callables(source: str, index: ClassIndex) -> Dict[str, Callable_]:
    """Map ``qualname -> Callable_`` for the public callables defined in ``source``.

    Public callables are module-level functions decorated ``@PublicAPI`` and the
    methods of ``@PublicAPI`` classes, excluding methods that carry their own
    ``@DeveloperAPI`` or ``@Deprecated`` annotation. ``qualname`` is ``func`` for
    a module-level function and ``Class.method`` for a method, which is a stable
    key across revisions of the same file.
    """
    out: Dict[str, Callable_] = {}
    try:
        tree = ast.parse(source)
    except (SyntaxError, ValueError):
        return out

    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if has_publicapi_decorator(node):
                c = _undocumented_for_func(node, node.name, index, None)
                if c is not None:
                    out[c.qualname] = c
        elif isinstance(node, ast.ClassDef) and has_publicapi_decorator(node):
            for sub in node.body:
                if isinstance(sub, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    # A method inherits public scope from its class, but its own
                    # @DeveloperAPI/@Deprecated takes it back out. An explicit
                    # @PublicAPI on the method wins over both.
                    if has_non_public_annotation(sub) and not has_publicapi_decorator(
                        sub
                    ):
                        continue
                    c = _undocumented_for_func(
                        sub, f"{node.name}.{sub.name}", index, node
                    )
                    if c is not None:
                        out[c.qualname] = c
    return out


@dataclass
class Violation:
    """A newly-undocumented parameter set on one public callable."""

    path: str
    qualname: str
    lineno: int
    params: List[str]  # newly-undocumented, sorted


def new_violations_for_file(
    path: str,
    base_source: Optional[str],
    head_source: str,
    base_index: ClassIndex,
    head_index: ClassIndex,
) -> List[Violation]:
    """Newly-undocumented params in one file, comparing base to head.

    A parameter is a *new* violation when it is undocumented at head and was not
    already undocumented at base for the same callable. This grandfathers
    pre-existing gaps and fires only on new public callables, newly-added
    params, and doc entries removed from an existing param. ``base_source`` is
    ``None`` when the file did not exist at the base revision (every head gap is
    then new).
    """
    head = public_callables(head_source, head_index)
    base = public_callables(base_source, base_index) if base_source is not None else {}

    violations: List[Violation] = []
    for qual, head_c in head.items():
        base_c = base.get(qual)
        already = base_c.undocumented if base_c is not None else set()
        new_params = sorted(head_c.undocumented - already)
        if new_params:
            violations.append(
                Violation(
                    path=path,
                    qualname=qual,
                    lineno=head_c.lineno,
                    params=new_params,
                )
            )
    violations.sort(key=lambda v: (v.path, v.lineno, v.qualname))
    return violations
