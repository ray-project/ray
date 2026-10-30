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

A second, separate check reuses the same diff and scope for docstring
*presence*: it fails when a new or changed public callable has no docstring, or
annotates a non-``None`` return without a ``Returns:`` (or ``Yields:``) section.
See :func:`new_presence_violations_for_file`.
"""
import ast
import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Iterable, Iterator, List, Optional, Set, Tuple, Union

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
# A documented return: a Google "Returns:"/"Yields:" header, a NumPy underlined
# header, or a reST ":returns:"/":rtype:"/":yields:" field.
_RETURNS_SECTION = re.compile(
    r"^\s*(Returns?|Yields?)\s*:\s*$|^\s*(Returns?|Yields?)\s*\n\s*-{3,}"
    r"|:(returns?|rtype|yields?):",
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


def has_returns_section(docstring: Optional[str]) -> bool:
    """Whether a docstring documents a return or yield value."""
    return bool(docstring and _RETURNS_SECTION.search(docstring))


def _decorator_name(dec: ast.expr) -> Optional[str]:
    """Simple name of one decorator, in bare, called, and attribute forms."""
    target = dec.func if isinstance(dec, ast.Call) else dec
    if isinstance(target, ast.Name):
        return target.id
    if isinstance(target, ast.Attribute):
        return target.attr
    return None


def _decorator_names(
    node: Union[ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef]
) -> Set[str]:
    """Simple names of a node's decorators."""
    return {_decorator_name(dec) for dec in node.decorator_list} - {None}


def has_publicapi_decorator(
    node: Union[ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef]
) -> bool:
    """Whether a class/function node carries an ``@PublicAPI`` decorator.

    Matches bare ``@PublicAPI`` and called ``@PublicAPI(...)`` forms, and both
    ``PublicAPI`` (imported name) and ``annotations.PublicAPI`` (attribute)
    spellings. Only ``@PublicAPI`` counts; ``@DeveloperAPI`` and ``@Deprecated``
    are not part of the rendered public API surface.
    """
    return "PublicAPI" in _decorator_names(node)


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
    return bool(_decorator_names(node) & _NON_PUBLIC_ANNOTATIONS)


# Decorators that mark a callable as something other than a Python API whose
# parameters a docstring Args: block documents. Pydantic validators receive the
# field value from the framework (``v``), and Click commands document their
# options with ``help=`` on each ``@click.option``.
_NON_DOCSTRING_PARAM_DECORATORS = frozenset(
    {
        "field_validator",
        "model_validator",
        "validator",
        "root_validator",
        "command",
        "group",
        "setter",
    }
)


def has_non_docstring_param_decorator(node: _FuncNode) -> bool:
    """Whether a callable's parameters are documented somewhere other than Args:.

    Matches pydantic validators (``@field_validator``, ``@model_validator``, and
    the v1 ``@validator`` and ``@root_validator``) and Click commands and groups
    (``@click.command()``, ``@cli.command()``, ``@click.group()``), in bare,
    called, and attribute spellings. Also matches property setters
    (``@x.setter``), whose value is documented on the getter.
    """
    return bool(_decorator_names(node) & _NON_DOCSTRING_PARAM_DECORATORS)


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
    """Simple names of a class's declared bases.

    Unwraps subscripted bases so a generic parent (``class C(Base[T])``) still
    resolves for docstring inheritance.
    """
    names = []
    for b in classdef.bases:
        if isinstance(b, ast.Subscript):
            b = b.value
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
    # For the presence check, keyed ``func``, ``Class``, or ``Class.method``:
    # whether the node's own non-empty docstring has a Returns: section (key
    # present == has own doc), and the keys a ``@_copy_doc(target)`` decorator
    # copies the docstring from.
    docs: Dict[str, bool] = field(default_factory=dict)
    copies: Dict[str, List[str]] = field(default_factory=lambda: defaultdict(list))

    def resolve_doc(self, key: str, _seen=None) -> Optional[bool]:
        """Whether the docstring a callable renders with has a Returns: section.

        Follows ``@_copy_doc`` edges first, since the decorator replaces the
        docstring at import time, then the callable's own docstring, then its
        bases. Returns None when no docstring is found.
        """
        if _seen is None:
            _seen = set()
        if key in _seen:
            return None
        _seen.add(key)
        for target in self.copies.get(key, []):
            got = self.resolve_doc(target, _seen)
            if got is not None:
                return got
        if key in self.docs:
            return self.docs[key]
        return self.resolve_inherited_doc(key, _seen)

    def resolve_inherited_doc(self, key: str, _seen=None) -> Optional[bool]:
        """Like :meth:`resolve_doc`, but only from the bases of ``key``'s class."""
        if _seen is None:
            _seen = set()
        class_name, _, method = key.partition(".")
        for base in self.bases.get(class_name, []):
            got = self.resolve_doc(f"{base}.{method}" if method else base, _seen)
            if got is not None:
                return got
        return None

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


def _import_aliases(tree: ast.Module) -> Dict[str, str]:
    """Map ``asname -> name`` for a module's ``from x import name as asname``."""
    aliases = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if alias.asname:
                    aliases[alias.asname] = alias.name
    return aliases


def _copy_doc_targets(
    node: Union[_FuncNode, ast.ClassDef], aliases: Dict[str, str]
) -> List[str]:
    """Index keys that a ``@_copy_doc(target)`` decorator may copy from.

    ``ray.train.utils._copy_doc`` and ``ray.train.v2._internal.util._copy_doc``
    set ``__doc__`` from their target at import time, so the docstring Sphinx
    renders isn't in the decorated node's source. A ``Name.attr`` target is
    either a class method or a function in an imported module, so both keys are
    returned. Import aliases are resolved, so ``TrainV1Context.get_world_size``
    finds ``TrainContext.get_world_size``.
    """
    for dec in node.decorator_list:
        if not isinstance(dec, ast.Call) or not dec.args:
            continue
        if _decorator_name(dec) != "_copy_doc":
            continue
        target = dec.args[0]
        if isinstance(target, ast.Name):
            return [aliases.get(target.id, target.id)]
        if isinstance(target, ast.Attribute):
            owner = target.value
            if isinstance(owner, ast.Name):
                owner_name = aliases.get(owner.id, owner.id)
            elif isinstance(owner, ast.Attribute):
                owner_name = owner.attr
            else:
                return [target.attr]
            return [f"{owner_name}.{target.attr}", target.attr]
    return []


def _record_doc(
    index: ClassIndex,
    key: str,
    node: Union[_FuncNode, ast.ClassDef],
    aliases: Dict[str, str],
) -> None:
    doc = ast.get_docstring(node)
    if doc:
        index.docs[key] = index.docs.get(key, False) or has_returns_section(doc)
    for target in _copy_doc_targets(node, aliases):
        if target not in index.copies[key]:
            index.copies[key].append(target)


def build_class_index(files: Iterable[Tuple[str, str]]) -> ClassIndex:
    """Build a :class:`ClassIndex` from ``(path, source)`` pairs.

    Walks every class in every file (public or not) so docstring inheritance
    can be resolved, and every module-level function so ``@_copy_doc`` targets
    can be. Same simple-name collisions across modules are merged (union of
    documented params, concatenated bases). Files that fail to parse are
    skipped.
    """
    index = ClassIndex()
    for _path, source in files:
        try:
            tree = ast.parse(source)
        except (SyntaxError, ValueError):
            continue
        aliases = _import_aliases(tree)
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                _record_doc(index, node.name, node, aliases)
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
            _record_doc(index, cn, node, aliases)
            for sub in node.body:
                if isinstance(sub, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    mdoc = ast.get_docstring(sub)
                    if mdoc is not None:  # own docstring -> stops MRO walk
                        index.method_own[cn].setdefault(sub.name, set())
                        index.method_own[cn][sub.name] |= documented_params(mdoc)
                    _record_doc(index, f"{cn}.{sub.name}", sub, aliases)
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

    Out of scope: private names other than ``__init__``, pydantic validators and
    Click commands, and callables with no public signature parameters (nothing
    to document). A parameter whose name starts with ``_`` is private by
    convention and isn't counted.
    """
    if func.name.startswith("_") and func.name != "__init__":
        return None
    if has_non_docstring_param_decorator(func):
        return None
    sig = [p for p in signature_params(func) if not p.startswith("_")]
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


def _public_nodes(
    tree: ast.Module,
) -> Iterator[Tuple[str, Union[_FuncNode, ast.ClassDef], Optional[ast.ClassDef]]]:
    """Yield ``(qualname, node, owning_class)`` for the public API surface.

    That's module-level ``@PublicAPI`` functions and classes, and the methods of
    ``@PublicAPI`` classes, excluding methods that carry their own
    ``@DeveloperAPI`` or ``@Deprecated`` annotation. ``qualname`` is ``func`` or
    ``Class`` at module level and ``Class.method`` for a method, which is a
    stable key across revisions of the same file.
    """
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if has_publicapi_decorator(node):
                yield node.name, node, None
        elif isinstance(node, ast.ClassDef) and has_publicapi_decorator(node):
            yield node.name, node, None
            for sub in node.body:
                if isinstance(sub, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    # A method inherits public scope from its class, but its own
                    # @DeveloperAPI/@Deprecated takes it back out. An explicit
                    # @PublicAPI on the method wins over both.
                    if has_non_public_annotation(sub) and not has_publicapi_decorator(
                        sub
                    ):
                        continue
                    yield f"{node.name}.{sub.name}", sub, node


def public_callables(source: str, index: ClassIndex) -> Dict[str, Callable_]:
    """Map ``qualname -> Callable_`` for the public callables defined in ``source``.

    Public callables are the functions and methods :func:`_public_nodes` yields.
    """
    out: Dict[str, Callable_] = {}
    try:
        tree = ast.parse(source)
    except (SyntaxError, ValueError):
        return out

    for qual, node, class_node in _public_nodes(tree):
        if isinstance(node, ast.ClassDef):
            continue
        c = _undocumented_for_func(node, qual, index, class_node)
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


# --- Docstring and Returns: presence ------------------------------------------

MISSING_DOCSTRING = "docstring"
MISSING_RETURNS = "Returns: section"

# Autodoc renders a property's type from its return annotation, so a property
# doesn't need a Returns: section.
_PROPERTY_DECORATORS = frozenset({"property", "cached_property"})
# Return annotations that mean the callable never returns a value.
_NO_RETURN_ANNOTATIONS = frozenset({"NoReturn", "Never"})


def needs_returns_section(func: _FuncNode) -> bool:
    """Whether a function annotates a return value that its docstring should document.

    True for any return annotation other than ``None``, ``NoReturn``, or
    ``Never``, except on properties.
    """
    ret = func.returns
    if ret is None:
        return False
    if isinstance(ret, ast.Constant) and ret.value in (None, "None"):
        return False
    if isinstance(ret, ast.Name) and ret.id in _NO_RETURN_ANNOTATIONS:
        return False
    if isinstance(ret, ast.Attribute) and ret.attr in _NO_RETURN_ANNOTATIONS:
        return False
    return not (_decorator_names(func) & _PROPERTY_DECORATORS)


def _rendered_doc(
    node: Union[_FuncNode, ast.ClassDef],
    qual: str,
    index: ClassIndex,
    aliases: Dict[str, str],
) -> Tuple[bool, bool]:
    """``(has_docstring, has_returns_section)`` for the docstring a node renders with.

    Resolves ``@_copy_doc`` first, then the node's own docstring, then the
    docstring Sphinx inherits from a base (``autodoc_inherit_docstrings``). A
    ``@_copy_doc`` target the index can't resolve fails open: the docstring is
    copied at runtime from code this static check can't see, so both count as
    present.
    """
    if "_copy_doc" in _decorator_names(node):
        for target in _copy_doc_targets(node, aliases):
            got = index.resolve_doc(target)
            if got is not None:
                return True, got
        return True, True
    own = ast.get_docstring(node)
    if own:
        return True, has_returns_section(own)
    got = index.resolve_inherited_doc(qual)
    if got is None:
        return False, False
    return True, got


@dataclass
class Presence:
    """A public callable's docstring-presence state at one tree revision."""

    qualname: str
    lineno: int
    missing: Set[str]  # subset of {MISSING_DOCSTRING, MISSING_RETURNS}


def _presence_for(
    node: Union[_FuncNode, ast.ClassDef],
    qual: str,
    index: ClassIndex,
    aliases: Dict[str, str],
) -> Optional[Presence]:
    """Presence state for one public node, or None if it is not in scope.

    Out of scope: private names, including ``__init__``, whose parameters the
    class docstring documents; ``@overload`` stubs; and the pydantic
    validators, Click commands, and property setters that the parameter check
    also skips.

    A callable without a docstring also counts as missing its Returns: section
    when it needs one. Otherwise, adding a docstring without Returns: to a
    grandfathered callable would read as new debt.
    """
    if node.name.startswith("_"):
        return None
    if not isinstance(node, ast.ClassDef) and (
        has_non_docstring_param_decorator(node) or "overload" in _decorator_names(node)
    ):
        return None
    has_doc, has_returns = _rendered_doc(node, qual, index, aliases)
    missing = set()
    if not has_doc:
        missing.add(MISSING_DOCSTRING)
    if (
        not isinstance(node, ast.ClassDef)
        and needs_returns_section(node)
        and not has_returns
    ):
        missing.add(MISSING_RETURNS)
    return Presence(qualname=qual, lineno=node.lineno, missing=missing)


def public_presence(source: str, index: ClassIndex) -> Dict[str, Presence]:
    """Map ``qualname -> Presence`` for the public classes and callables in ``source``."""
    out: Dict[str, Presence] = {}
    try:
        tree = ast.parse(source)
    except (SyntaxError, ValueError):
        return out
    aliases = _import_aliases(tree)
    for qual, node, _class_node in _public_nodes(tree):
        p = _presence_for(node, qual, index, aliases)
        if p is not None:
            out[qual] = p
    return out


@dataclass
class PresenceViolation:
    """Newly-missing docstring parts on one public class or callable."""

    path: str
    qualname: str
    lineno: int
    missing: List[str]  # newly-missing parts, docstring first


def new_presence_violations_for_file(
    path: str,
    base_source: Optional[str],
    head_source: str,
    base_index: ClassIndex,
    head_index: ClassIndex,
) -> List[PresenceViolation]:
    """Newly-missing docstrings and Returns: sections in one file.

    Same diff as :func:`new_violations_for_file`: a part is newly missing when
    it is missing at head and was not already missing at base for the same
    callable. That covers new public callables, a docstring or Returns: section
    a PR removes, and a return annotation a PR adds to an existing callable.
    """
    head = public_presence(head_source, head_index)
    base = public_presence(base_source, base_index) if base_source is not None else {}

    violations: List[PresenceViolation] = []
    for qual, head_p in head.items():
        base_p = base.get(qual)
        already = base_p.missing if base_p is not None else set()
        new_missing = [
            m
            for m in (MISSING_DOCSTRING, MISSING_RETURNS)
            if m in head_p.missing - already
        ]
        if new_missing:
            violations.append(
                PresenceViolation(
                    path=path,
                    qualname=qual,
                    lineno=head_p.lineno,
                    missing=new_missing,
                )
            )
    violations.sort(key=lambda v: (v.path, v.lineno, v.qualname))
    return violations
