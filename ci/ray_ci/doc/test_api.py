import sys

import pytest

from ci.ray_ci.doc.api import (
    _SPHINX_AUTOCLASS_HEADER,
    _SPHINX_AUTOSUMMARY_HEADER,
    API,
    AnnotationType,
    CodeType,
)
from ci.ray_ci.doc.mock.mock_module import (
    InheritedAnnotation,
    MockClass,
    MockDeprecatedClass,
    MockDeprecatedSubclass,
    mock_function,
    mock_w00t,
)

_MOCK = "ci.ray_ci.doc.mock.mock_module"
_INTERNAL_MOCK = "ci.ray_ci.doc.mock._internal"


def _doc_api(name: str, code_type: CodeType = CodeType.FUNCTION) -> API:
    # Mimics a parsed doc-side entry: from_autosummary/from_autoclass always
    # stamp PUBLIC_API regardless of the object's real annotation.
    return API(
        name=name,
        annotation_type=AnnotationType.PUBLIC_API,
        code_type=code_type,
    )


def test_from_autosummary():
    test_data = [
        {
            "input": {
                "doc": (
                    f"{_SPHINX_AUTOSUMMARY_HEADER}\n"
                    "\t:toc\n"
                    "\n"
                    "\tfun_01\n"
                    "\t.. this is a comment\n"
                    "\tfun_02\n"
                    "something else"
                ),
                "module": "mymodule",
            },
            "output": [
                API(
                    name="mymodule.fun_01",
                    annotation_type=AnnotationType.PUBLIC_API,
                    code_type=CodeType.FUNCTION,
                ),
                API(
                    name="mymodule.fun_02",
                    annotation_type=AnnotationType.PUBLIC_API,
                    code_type=CodeType.FUNCTION,
                ),
            ],
        },
        {
            "input": {
                "doc": "invalid string",
                "module": "mymodule",
            },
            "output": [],
        },
    ]

    for test in test_data:
        assert str(
            API.from_autosummary(
                test["input"]["doc"],
                test["input"]["module"],
            )
        ) == str(test["output"])


def test_from_autoclasss():
    test_data = [
        # valid input, no module
        {
            "input": {
                "doc": f"{_SPHINX_AUTOCLASS_HEADER} myclass",
                "module": None,
            },
            "output": API(
                name="myclass",
                annotation_type=AnnotationType.PUBLIC_API,
                code_type=CodeType.CLASS,
            ),
        },
        # valid input, with module
        {
            "input": {
                "doc": f"{_SPHINX_AUTOCLASS_HEADER} myclass",
                "module": "mymodule",
            },
            "output": API(
                name="mymodule.myclass",
                annotation_type=AnnotationType.PUBLIC_API,
                code_type=CodeType.CLASS,
            ),
        },
        # invalid input
        {
            "input": {
                "doc": "invalid",
                "module": None,
            },
            "output": None,
        },
    ]

    for test in test_data:
        assert str(
            API.from_autoclass(
                test["input"]["doc"],
                test["input"]["module"],
            )
        ) == str(test["output"])


def test_get_canonical_name():
    api = API(
        name="ci.ray_ci.doc.mock.mock_function",
        annotation_type=AnnotationType.PUBLIC_API,
        code_type=CodeType.FUNCTION,
    )
    assert (
        api.get_canonical_name()
        == f"{mock_function.__module__}.{mock_function.__qualname__}"
    )


def test_is_private_name():
    test_data = [
        {
            "input": "a.b._private_function",
            "output": True,
        },
        {
            "input": "a.b._internal.public_function",
            "output": True,
        },
        {
            "input": "b.c.public_class",
            "output": False,
        },
    ]
    for test in test_data:
        assert (
            API(
                name=test["input"],
                annotation_type=AnnotationType.UNKNOWN,
                code_type=CodeType.FUNCTION,
            )._is_private_name()
            == test["output"]
        )


def test_is_public():
    assert not API(
        name="a.b._private_function",
        annotation_type=AnnotationType.PUBLIC_API,
        code_type=CodeType.FUNCTION,
    ).is_public()
    assert not API(
        name="a.b._internal.public_function",
        annotation_type=AnnotationType.PUBLIC_API,
        code_type=CodeType.FUNCTION,
    ).is_public()
    assert not API(
        name="a.b.public_function",
        annotation_type=AnnotationType.DEPRECATED,
        code_type=CodeType.FUNCTION,
    ).is_public()
    assert API(
        name="a.b.public_function",
        annotation_type=AnnotationType.PUBLIC_API,
        code_type=CodeType.FUNCTION,
    ).is_public()


def test_is_deprecated():
    assert not API(
        name="a.b._private_function",
        annotation_type=AnnotationType.PUBLIC_API,
        code_type=CodeType.FUNCTION,
    ).is_deprecated()

    assert API(
        name="a.b.function",
        annotation_type=AnnotationType.DEPRECATED,
        code_type=CodeType.FUNCTION,
    ).is_deprecated()


def test_split_good_and_bad_apis():
    good_apis, bad_apis = API.split_good_and_bad_apis(
        {
            "a.b.public_function": API(
                name="a.b.public_function",
                annotation_type=AnnotationType.PUBLIC_API,
                code_type=CodeType.FUNCTION,
            ),
            "a.b._private_function": API(
                name="a.b._private_function",
                annotation_type=AnnotationType.PUBLIC_API,
                code_type=CodeType.FUNCTION,
            ),
            "a.b.deprecated_function_01": API(
                name="a.b.deprecated_function_01",
                annotation_type=AnnotationType.PUBLIC_API,
                code_type=CodeType.FUNCTION,
            ),
            "a.b.deprecated_function_02": API(
                name="a.b.deprecated_function_02",
                annotation_type=AnnotationType.PUBLIC_API,
                code_type=CodeType.FUNCTION,
            ),
        },
        {"a.b.public_function"},
        {"a.b._private_function"},
    )

    assert good_apis == ["a.b.public_function"]
    assert bad_apis == ["a.b.deprecated_function_01", "a.b.deprecated_function_02"]


def test_resolve():
    # Resolves a function, a class, and a (non-annotated) method of a class.
    assert _doc_api(f"{_MOCK}.mock_w00t").resolve() is mock_w00t
    assert _doc_api(f"{_MOCK}.MockClass").resolve() is MockClass
    assert _doc_api(f"{_MOCK}.MockClass.mock_method").resolve() is MockClass.mock_method
    # A deleted / renamed / misspelled name does not resolve.
    assert _doc_api(f"{_MOCK}.does_not_exist").resolve() is None
    assert _doc_api(f"{_MOCK}.MockClass.no_such_method").resolve() is None
    assert _doc_api("ci.ray_ci.doc.no_such_submodule.thing").resolve() is None
    assert _doc_api("totally_missing_top_level_module.thing").resolve() is None
    # Malformed names must not crash (importlib.import_module("") raises
    # ValueError); they resolve to None.
    assert _doc_api("").resolve() is None
    assert _doc_api(".leading.dot").resolve() is None
    assert _doc_api(f"{_MOCK}..double.dot").resolve() is None


def test_introspect_annotation_type():
    assert API.introspect_annotation_type(MockClass) == AnnotationType.PUBLIC_API
    assert API.introspect_annotation_type(mock_function) == AnnotationType.DEPRECATED
    # Methods and other un-annotated objects resolve to UNKNOWN.
    assert (
        API.introspect_annotation_type(MockClass.mock_method) == AnnotationType.UNKNOWN
    )
    assert API.introspect_annotation_type(object()) == AnnotationType.UNKNOWN


def test_introspect_annotation_type_ignores_inherited_annotations():
    # `_annotated_type` is a plain class attribute, so an undecorated subclass
    # reads its base's value. Only an annotation the object owns counts, in
    # either direction: an inherited @Deprecated must not make a subclass read
    # as deprecated, and an inherited @PublicAPI must not make one read public.
    assert (
        API.introspect_annotation_type(MockDeprecatedClass) == AnnotationType.DEPRECATED
    )
    assert (
        API.introspect_annotation_type(MockDeprecatedSubclass) == AnnotationType.UNKNOWN
    )
    assert API.introspect_annotation_type(InheritedAnnotation) == AnnotationType.UNKNOWN


def test_canonical_name_of():
    # Classes and functions canonicalize to module.qualname; the object comes
    # from the same resolve() walk used to read the annotation.
    assert (
        API.canonical_name_of(mock_w00t, "ignored")
        == f"{mock_w00t.__module__}.{mock_w00t.__qualname__}"
    )
    assert (
        API.canonical_name_of(MockClass, "ignored")
        == f"{MockClass.__module__}.{MockClass.__qualname__}"
    )
    # Anything that is not a class or function keeps the documented name.
    assert API.canonical_name_of(object(), "some.documented.name") == (
        "some.documented.name"
    )


def test_split_resolvable_and_broken_doc_apis():
    api_in_docs = [
        # public, resolves -> accepted
        _doc_api(f"{_MOCK}.mock_w00t"),
        # public method, resolves, un-annotated -> accepted (not a false positive)
        _doc_api(f"{_MOCK}.MockClass.mock_method"),
        # does not resolve -> unresolved
        _doc_api(f"{_MOCK}.renamed_away"),
        # resolves to a @Deprecated object -> non_public. Note the doc-side
        # entry is stamped PUBLIC_API; the check must override it via live
        # introspection.
        _doc_api(f"{_MOCK}.mock_function"),
        # resolves but is whitelisted as an intentional doc entry -> skipped
        _doc_api(f"{_MOCK}.also_deprecated"),
    ]
    white_list_apis = {f"{_MOCK}.also_deprecated"}

    unresolved, non_public = API.split_resolvable_and_broken_doc_apis(
        api_in_docs, white_list_apis
    )

    assert unresolved == [f"{_MOCK}.renamed_away"]
    assert non_public == [f"{mock_function.__module__}.{mock_function.__qualname__}"]


def test_split_resolvable_accepts_subclass_of_deprecated_class():
    # Regression: documenting an undecorated subclass of a @Deprecated class is
    # legitimate -- the subclass was never deprecated. Reading the inherited
    # `_annotated_type` would flag it as "documented API resolves to a
    # deprecated object" and fail the check on a correct doc entry. The
    # directly-deprecated base is still flagged, so the rule keeps its teeth.
    unresolved, non_public = API.split_resolvable_and_broken_doc_apis(
        [
            _doc_api(f"{_MOCK}.MockDeprecatedSubclass", CodeType.CLASS),
            _doc_api(f"{_MOCK}.MockDeprecatedClass", CodeType.CLASS),
        ],
        set(),
    )

    assert unresolved == []
    assert non_public == [f"{_MOCK}.MockDeprecatedClass"]


def test_split_resolvable_flags_private_documented_name():
    # A documented name that resolves but is private-named is non-public.
    unresolved, non_public = API.split_resolvable_and_broken_doc_apis(
        [_doc_api(f"{_MOCK}._private_thing")], set()
    )
    # It does not resolve here (no such attribute), so it lands in unresolved;
    # the private-name rule is exercised through _check_team tests where the
    # name resolves. Guard the resolution-miss branch explicitly.
    assert unresolved == [f"{_MOCK}._private_thing"]
    assert non_public == []


def test_split_resolvable_exempts_override_hook():
    # A documented, underscore-named method tagged as an override hook is a
    # public extension point, so it is not flagged non-public. A sibling
    # underscore method with no marker still is -- the exemption must not weaken
    # detection of genuinely private symbols.
    unresolved, non_public = API.split_resolvable_and_broken_doc_apis(
        [
            _doc_api(f"{_MOCK}.MockClass._mock_forward"),
            _doc_api(f"{_MOCK}.MockClass._mock_private"),
        ],
        set(),
    )

    assert unresolved == []
    assert non_public == [f"{_MOCK}.MockClass._mock_private"]


def test_split_resolvable_exempts_public_reexport_of_private_module():
    # A class implemented in a private module but re-exported through a public
    # module's __all__ is public: the export is the contract, the implementation
    # path is not. Its sibling in the same private module, absent from __all__,
    # is still flagged -- the exemption must not weaken detection of genuinely
    # private symbols.
    unresolved, non_public = API.split_resolvable_and_broken_doc_apis(
        [
            _doc_api(f"{_MOCK}.MockReexportedClass", CodeType.CLASS),
            _doc_api(f"{_MOCK}.MockInternalOnlyClass", CodeType.CLASS),
        ],
        set(),
    )

    assert unresolved == []
    assert non_public == [f"{_INTERNAL_MOCK}.MockInternalOnlyClass"]


def test_split_resolvable_flags_reexport_documented_by_private_path():
    # The same object documented through its private canonical path instead of
    # its public re-export stays flagged. A private module's __all__ is not a
    # public contract, so it can't launder the name.
    unresolved, non_public = API.split_resolvable_and_broken_doc_apis(
        [_doc_api(f"{_INTERNAL_MOCK}.MockReexportedClass", CodeType.CLASS)],
        set(),
    )

    assert unresolved == []
    assert non_public == [f"{_INTERNAL_MOCK}.MockReexportedClass"]


def test_is_public_reexport():
    # Exported from a public module's __all__.
    assert API._is_public_reexport(
        f"{_MOCK}.MockReexportedClass",
        f"{_INTERNAL_MOCK}.MockReexportedClass",
    )
    # Importable from the same module but not exported.
    assert not API._is_public_reexport(
        f"{_MOCK}.MockInternalOnlyClass",
        f"{_INTERNAL_MOCK}.MockInternalOnlyClass",
    )
    # Documented through a private module path.
    assert not API._is_public_reexport(
        f"{_INTERNAL_MOCK}.MockReexportedClass",
        f"{_INTERNAL_MOCK}.MockReexportedClass",
    )
    # An underscore leaf stays private on either side of the re-export, so
    # __all__ membership can never promote one.
    assert not API._is_public_reexport(f"{_MOCK}._MockReexportedClass", "pkg.Thing")
    assert not API._is_public_reexport(
        f"{_MOCK}.MockReexportedClass", "pkg._internal._Thing"
    )
    # A parent that is a class, not a module, has no __all__ to read.
    assert not API._is_public_reexport(f"{_MOCK}.MockClass.mock_method", "pkg.Thing")
    # A name with no module part.
    assert not API._is_public_reexport("MockReexportedClass", "pkg.Thing")


def test_is_public_reexport_requires_a_real_export_list(monkeypatch):
    from ci.ray_ci.doc.mock import mock_module

    documented = f"{_MOCK}.MockReexportedClass"
    canonical = f"{_INTERNAL_MOCK}.MockReexportedClass"

    # A tuple is as valid a declaration as a list; Ray modules use both.
    monkeypatch.setattr(mock_module, "__all__", ("MockReexportedClass",))
    assert API._is_public_reexport(documented, canonical)

    # Anything that isn't a collection of names confers nothing. A bare string
    # is the case worth naming: a membership test against it would match a
    # substring and silently exempt a symbol nobody exported.
    for not_an_export_list in ("MockReexportedClass", None, 42):
        monkeypatch.setattr(mock_module, "__all__", not_an_export_list)
        assert not API._is_public_reexport(documented, canonical)


def test_find_duplicate_doc_apis():
    # mock_w00t appears twice, MockClass once. Names canonicalize first, so the
    # duplicate is reported under the canonical name.
    api_in_docs = [
        _doc_api(f"{_MOCK}.mock_w00t"),
        _doc_api(f"{_MOCK}.mock_w00t"),
        _doc_api(f"{_MOCK}.MockClass", CodeType.CLASS),
    ]
    canonical_w00t = f"{mock_w00t.__module__}.{mock_w00t.__qualname__}"

    assert API.find_duplicate_doc_apis(api_in_docs, set()) == [canonical_w00t]
    # An intentional-duplicate whitelist suppresses the report.
    assert API.find_duplicate_doc_apis(api_in_docs, {canonical_w00t}) == []


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
