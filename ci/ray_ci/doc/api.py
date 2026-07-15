import importlib
import inspect
import re
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple

_SPHINX_AUTOSUMMARY_HEADER = ".. autosummary::"
_SPHINX_AUTOCLASS_HEADER = ".. autoclass::"
# This is a special character used in autosummary to render only the api shortname, for
# example ~module.api_name will render only api_name
_SPHINX_AUTODOC_SHORTNAME = "~"


class AnnotationType(Enum):
    PUBLIC_API = "PublicAPI"
    DEVELOPER_API = "DeveloperAPI"
    DEPRECATED = "Deprecated"
    UNKNOWN = "Unknown"


class CodeType(Enum):
    CLASS = "Class"
    FUNCTION = "Function"


@dataclass
class API:
    name: str
    annotation_type: AnnotationType
    code_type: CodeType

    @staticmethod
    def from_autosummary(doc: str, current_module: Optional[str] = None) -> List["API"]:
        """
        Parse API from the following autosummary sphinx block.

        .. autosummary::
            :option_01
            :option_02

            api_01
            api_02
        """
        apis = []
        lines = doc.splitlines()
        if not lines:
            return apis

        if lines[0].strip() != _SPHINX_AUTOSUMMARY_HEADER:
            return apis

        for line in lines:
            if line == _SPHINX_AUTOSUMMARY_HEADER:
                continue
            if line.strip().startswith(":"):
                # option lines
                continue
            if line.strip().startswith(".."):
                # comment lines
                continue
            if not line.strip():
                # empty lines
                continue
            if not re.match(r"\s", line):
                # end of autosummary, \s means empty space, this line is checking if
                # the line is not empty and not starting with empty space
                break
            attribute = line.strip().removeprefix(_SPHINX_AUTODOC_SHORTNAME)
            api_name = f"{current_module}.{attribute}" if current_module else attribute
            apis.append(
                API(
                    name=api_name,
                    annotation_type=AnnotationType.PUBLIC_API,
                    code_type=CodeType.FUNCTION,
                )
            )

        return apis

    @staticmethod
    def from_autoclass(
        doc: str, current_module: Optional[str] = None
    ) -> Optional["API"]:
        """
        Parse API from the following autoclass sphinx block.

        .. autoclass:: api_01
        """
        doc = doc.strip()
        if not doc.startswith(_SPHINX_AUTOCLASS_HEADER):
            return None
        cls = (
            doc[len(_SPHINX_AUTOCLASS_HEADER) :]
            .strip()
            .removeprefix(_SPHINX_AUTODOC_SHORTNAME)
        )
        api_name = f"{current_module}.{cls}" if current_module else cls

        return API(
            name=api_name,
            annotation_type=AnnotationType.PUBLIC_API,
            code_type=CodeType.CLASS,
        )

    def get_canonical_name(self) -> str:
        """
        Some APIs have aliases declared in __init__.py file (see ray/data/__init__.py
        for example). This method converts the alias to full name. This is to make sure
        out analysis can be performed on the same set of canonial names.
        """
        tokens = self.name.split(".")

        # convert the name into a python object, by converting the module token by token
        attribute = importlib.import_module(tokens[0])
        for token in tokens[1:]:
            if not hasattr(attribute, token):
                # return as it is if the name seems malformed
                return self.name
            attribute = getattr(attribute, token)

        if inspect.isclass(attribute) or inspect.isfunction(attribute):
            return f"{attribute.__module__}.{attribute.__qualname__}"
        return self.name

    def resolve(self) -> Optional[object]:
        """
        Strictly resolve this API's name to the live object it refers to.

        Walks the dotted name token by token, importing submodules as needed.
        Returns the resolved object, or None if any token fails to resolve.

        Unlike get_canonical_name(), which swallows a resolution miss by
        returning the raw name string, this reports the miss as None. That is
        what lets the check catch a documented entry pointing at a deleted,
        renamed, or misspelled symbol -- the failure mode that today only the
        Sphinx render notices (as an autosummary import warning).
        """
        tokens = self.name.split(".")
        if not tokens[0]:
            # A malformed doc entry (empty or leading-dot name) is unresolvable;
            # importlib.import_module("") would otherwise raise ValueError.
            return None
        try:
            attribute = importlib.import_module(tokens[0])
        except (ImportError, ValueError):
            return None

        walked = tokens[0]
        for token in tokens[1:]:
            walked = f"{walked}.{token}"
            # Prefer importing the submodule over getattr. A package often
            # re-exports a same-named function into its parent namespace (for
            # example ray.util.placement_group, the function, shadows the
            # ray.util.placement_group submodule); getattr would then return the
            # function and the remaining tokens would fail to resolve. Importing
            # the dotted path first yields the module, matching how Sphinx
            # autosummary resolves the name.
            try:
                attribute = importlib.import_module(walked)
                continue
            except (ImportError, ValueError):
                pass
            if hasattr(attribute, token):
                attribute = getattr(attribute, token)
                continue
            return None
        return attribute

    @staticmethod
    def introspect_annotation_type(obj: object) -> AnnotationType:
        """
        Read an object's *live* annotation type from the module.

        from_autosummary/from_autoclass stamp every parsed doc-side entry as
        PUBLIC_API unconditionally; those fields are placeholders, not
        observations. The check must learn a documented name's real annotation
        from the object the name resolves to, which is what this reads from the
        ``_annotated_type`` attribute the @PublicAPI/@Deprecated decorators set.
        Objects that carry no annotation (for example methods of an annotated
        class) resolve to UNKNOWN.
        """
        annotated_type = getattr(obj, "_annotated_type", None)
        if annotated_type is None:
            return AnnotationType.UNKNOWN
        try:
            return AnnotationType(annotated_type.value)
        except (AttributeError, ValueError):
            return AnnotationType.UNKNOWN

    @staticmethod
    def canonical_name_of(obj: object, fallback_name: str) -> str:
        """
        Canonical name of an already-resolved object.

        Mirrors get_canonical_name()'s output rule (a class or function gives
        ``module.qualname``; anything else keeps the documented name) but takes
        the object resolve() found, so identity and annotation are derived from
        the same import-first walk. Computing identity with get_canonical_name()
        (a getattr-only walk) while reading the annotation off resolve()'s
        object can disagree when a name is shadowed -- e.g. a re-exported
        function sharing a dotted segment with a submodule -- so the two must
        not be combined.
        """
        if inspect.isclass(obj) or inspect.isfunction(obj):
            return f"{obj.__module__}.{obj.__qualname__}"
        return fallback_name

    def _is_private_name(self) -> bool:
        """
        Check if this API has a private name. Private names are those that start with
        underscores.
        """
        name_has_underscore = self.name.split(".")[-1].startswith("_")
        is_internal = "._internal." in self.name

        return name_has_underscore or is_internal

    def is_public(self) -> bool:
        """
        Check if this API is public. Public APIs are those that are annotated as public
        and not have private names.
        """
        return (
            self.annotation_type == AnnotationType.PUBLIC_API
            and not self._is_private_name()
        )

    def is_deprecated(self) -> bool:
        """
        Check if this API is deprecated. Deprecated APIs are those that are annotated as
        deprecated.
        """
        return self.annotation_type == AnnotationType.DEPRECATED

    @staticmethod
    def split_good_and_bad_apis(
        api_in_codes: Dict[str, "API"], api_in_docs: Set[str], white_list_apis: Set[str]
    ) -> Tuple[List[str]]:
        """
        Given the APIs in the codebase and the documentation, split the APIs into good
        and bad APIs. Good APIs are those that are public and documented, bad APIs are
        those that are public but NOT documented.
        """
        good_apis = []
        bad_apis = []

        for name, api in api_in_codes.items():
            if not api.is_public():
                continue

            if name in white_list_apis:
                continue

            if name in api_in_docs:
                good_apis.append(name)
            else:
                bad_apis.append(name)

        return good_apis, bad_apis

    @staticmethod
    def split_resolvable_and_broken_doc_apis(
        api_in_docs: List["API"], white_list_apis: Set[str]
    ) -> Tuple[List[str], List[str]]:
        """
        Classify each documented API by whether it points at a real, public
        object -- documented names must be a subset of the public code surface.

        Returns ``(unresolved, non_public)``:

        - ``unresolved``: documented names that do not import to a live object
          -- a deleted, renamed, or misspelled autosummary / autoclass entry.
          This is the breakage that today only the Sphinx render catches.
        - ``non_public``: documented names that resolve, but whose *live*
          annotation is non-public (``@Deprecated``) or whose canonical name is
          private (``_foo`` / ``._internal.``).

        Objects that resolve but carry no annotation are accepted -- the Sphinx
        autosummary import check only warns on import failure, and documented
        methods (``Dataset.map_batches``) are public by virtue of their
        annotated class even though the method itself is not decorated. The
        annotation is read live from the resolved object rather than trusting
        the placeholder fields stamped on the parsed doc-side API.
        """
        unresolved = []
        non_public = []

        for api in api_in_docs:
            # A doc entry may be white-listed by its documented (raw) name even
            # when it does not resolve, so honor that before resolving.
            if api.name in white_list_apis:
                continue

            obj = api.resolve()
            if obj is None:
                unresolved.append(api.name)
                continue

            # Identity and annotation both come from this single resolved
            # object; see canonical_name_of() for why they must not be split
            # across get_canonical_name()'s separate walk.
            canonical_name = API.canonical_name_of(obj, api.name)
            if canonical_name in white_list_apis:
                continue

            annotation_type = API.introspect_annotation_type(obj)
            resolved_api = API(
                name=canonical_name,
                annotation_type=annotation_type,
                code_type=api.code_type,
            )
            if resolved_api.is_deprecated() or resolved_api._is_private_name():
                non_public.append(canonical_name)

        return unresolved, non_public

    @staticmethod
    def find_duplicate_doc_apis(
        api_in_docs: List["API"], intentional_duplicate_apis: Set[str]
    ) -> List[str]:
        """
        Return the canonical names that appear in more than one autosummary /
        autoclass block across the walked doc surface, excluding names in
        ``intentional_duplicate_apis``.

        A documented API rendered from two places produces a Sphinx "duplicate
        object description" warning; today that is masked by a hardcoded log
        filter (the ``DuplicateObjectFilter`` in conf.py) seeded for the one
        intentional case, ``ray.actor.ActorMethod.bind``. Enforcing the
        invariant here lets the masking move to an explicit, reviewed allowlist.
        """
        counts = {}
        for api in api_in_docs:
            # Resolve names the same (import-first) way, so two doc
            # entries that name the same object collapse to one canonical key
            # even when one spelling goes through a shadowed segment.
            obj = api.resolve()
            canonical_name = (
                api.name if obj is None else API.canonical_name_of(obj, api.name)
            )
            counts[canonical_name] = counts.get(canonical_name, 0) + 1

        return sorted(
            name
            for name, count in counts.items()
            if count > 1 and name not in intentional_duplicate_apis
        )
