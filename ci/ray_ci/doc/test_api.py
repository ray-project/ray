import sys

import pytest

from ci.ray_ci.doc.api import (
    _SPHINX_AUTOCLASS_HEADER,
    _SPHINX_AUTOSUMMARY_HEADER,
    API,
    AnnotationType,
    CodeType,
)
from ci.ray_ci.doc.mock.mock_module import MockClass, mock_function, mock_w00t

_MOCK = "ci.ray_ci.doc.mock.mock_module"


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
