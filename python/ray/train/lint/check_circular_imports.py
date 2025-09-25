import argparse
import ast
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple


class ImportCollector(ast.NodeVisitor):
    """
    An AST node visitor that collects all module-level imports from a Python source file.
    It traverses the AST and records module-level import statements (`import ...` and `from ... import ...`) that are not
    inside function or class definitions, and that are not guarded by `if TYPE_CHECKING` or `if typing.TYPE_CHECKING`
    blocks.
    """

    def __init__(self, module_name: str, is_package: bool) -> None:
        self._module_name = module_name
        self._is_package = is_package
        self.imports: Set[str] = set()

    # --- private helpers ---

    def _is_type_checking_test(self, expr: ast.AST) -> bool:
        """Return True for `if TYPE_CHECKING` or `if typing.TYPE_CHECKING`."""
        if isinstance(expr, ast.Name) and expr.id == "TYPE_CHECKING":
            return True
        if (
            isinstance(expr, ast.Attribute)
            and isinstance(expr.value, ast.Name)
            and expr.value.id == "typing"
            and expr.attr == "TYPE_CHECKING"
        ):
            return True
        return False

    def _get_package_parts(self) -> List[str]:
        parts = self._module_name.split(".")
        return parts if self._is_package else parts[:-1]

    def _to_absolute_module(
        self, level: int, module_str: Optional[str]
    ) -> Optional[str]:
        """Construct the absolute module string from a relative import."""
        # Absolute import
        if level == 0:
            return module_str

        package_parts = self._get_package_parts()

        # If the relative import is out of bounds
        if level - 1 > len(package_parts):
            return None

        # Base parts based on the level
        base_module_parts = package_parts[: -((level - 1))]

        # Construct absolute module string
        abs_module_parts = (
            base_module_parts + module_str.split(".")
            if module_str
            else base_module_parts
        )
        return ".".join(abs_module_parts)

    def visit_If(self, node: ast.If) -> None:
        # If the test is not TYPE_CHECKING, visit statement body
        if not self._is_type_checking_test(node.test):
            for stmt in node.body:
                self.visit(stmt)

        # Also visit conditional branches
        for stmt in node.orelse:
            self.visit(stmt)

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            if alias.name:
                self.imports.add(alias.name)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        import_str = self._to_absolute_module(node.level or 0, node.module)
        if not import_str:
            return

        self.imports.add(import_str)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        # Skip function contents
        return

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        # Skip function contents
        return

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        # Skip class contents
        return


def collect_imports(module_name: str, is_package: bool, source_text: str) -> Set[str]:
    try:
        tree = ast.parse(source_text)
    except SyntaxError:
        return set()
    collector = ImportCollector(module_name, is_package)
    collector.visit(tree)
    return collector.imports


def get_base_dir() -> Path:
    """Return the filesystem path to the ray directory (file-relative)."""
    # This file lives at: .../python/ray/train/lint/check_circular_imports.py
    # So ray is three directories up from here.
    script_path = Path(__file__).resolve()
    return script_path.parent.parent.parent.parent


def to_module_name_and_is_package(py_file: Path, train_dir: Path) -> Tuple[str, bool]:
    """Returns tuple (dotted module name, is_package) for a given Python file."""
    file_path = py_file.relative_to(train_dir)
    module_path = file_path.with_suffix("")
    module_parts = ["ray", "train", *module_path.parts]
    is_package = module_parts[-1] == "__init__"
    if is_package:
        module_parts = module_parts[:-1]
    module_str = ".".join(module_parts)
    return module_str, is_package

    module_imports: Dict[str, Set[str]] = {}


def get_file_module_imports(
    train_dir: Path, files: List[Path], module_match_string: Optional[str] = None
) -> Dict[str, List[str]]:
    """
    Builds a mapping from each Python file under the given train_dir to its module-level imports.

    Args:
        train_dir: Path to the ray/train directory.
        files: Set of Python file Paths under train_dir.

    Returns:
        Dict mapping each file's dotted module name to a set of its module-level imports.
    """
    module_imports: Dict[str, List[str]] = {}
    # Construct mapping
    for py_file in files:
        try:
            module_name, is_package = to_module_name_and_is_package(py_file, train_dir)
            src = py_file.read_text(encoding="utf-8", errors="ignore")

            # Collect imports from python file source
            imports = collect_imports(module_name, is_package, src)
            module_imports[module_name] = [
                stmt
                for stmt in imports
                if module_match_string is None or module_match_string in stmt
            ]
        except Exception:
            continue
    return module_imports


def check_violations(module_to_imports: Dict[str, Set[str]]) -> List[str]:
    """Find circular import violations between v2 modules and v1 packages.

    A violation is reported only if ALL of the following are true for some
    v2 module "v2_mod" under package "<pkg>" (e.g., ray.train.v2.<pkg>.foo):
      1) v2_mod imports at least one v1 module from the same package
         (ray.train.<pkg>[...]).
      2) The v1 package module (ray.train.<pkg>) imports v2_mod.
      3) The v2 package module (ray.train.v2.<pkg>) does NOT import any v1
         module from the same package (ray.train.<pkg>[...]).

    Args:
        module_to_imports: Mapping of dotted module name -> set of dotted modules
            it imports at module level.

    Returns:
        List of human-readable violation messages.
    """

    violations: List[str] = []
    reported_pairs: Set[Tuple[str, str]] = set()  # (v2_module_name, v1_import_name)

    import pdb

    pdb.set_trace()
    for v2_module_name, imported_module_set in module_to_imports.items():
        # Skip non-v2 modules early
        # TODO: also skip v2 init files like ray.train.v2.pytorch.__init__.py
        if not is_v2_module(v2_module_name):
            continue

        # Determine the immediate package name under ray.train.v2 (e.g., "torch")
        # TODO: handle packages like ray.train.v2.pytorch.foo. How does this tell the diff betweeen
        # an init file and a regular module?
        package_name = extract_pkg_under_v2(v2_module_name)
        if not package_name:
            continue

        # Dotted names for the v1 and v2 package modules for this package
        v1_package_module = f"{RAY_TRAIN_DOTTED_PREFIX}.{package_name}"
        v2_package_module = f"{RAY_TRAIN_V2_DOTTED_PREFIX}.{package_name}"

        # (1) Collect all v1 imports from the corresponding v2 package
        # Example match: ray.train.tensorflow or ray.train.tensorflow.something
        # TODO: include all v1 imports
        v1_imports_from_same_package = [
            imported
            for imported in imported_module_set
            if imported.startswith(f"{v1_package_module}")
        ]
        # First rule, check if the v2 module imports from v1
        if not v1_imports_from_same_package:
            # No v1 same-package imports; cannot be a violation.
            continue

        # (2) Check if the v1 package module imports the specific v2 module.
        # TODO: check all intermediate v1 package modules
        v1_package_imports = module_to_imports.get(v1_package_module, set())
        v1_imports_v2_module = v2_module_name in v1_package_imports

        # (3) Check if the v2 init file imports any v1 package modules.
        v2_init_file_imports = module_to_imports.get(v2_package_module, set())
        v2_pkg_does_not_import_v1_pkg = not any(
            mod.startswith(f"{RAY_TRAIN_DOTTED_PREFIX}.{package_name}")
            for mod in v2_init_file_imports
        )

        if not (v1_imports_v2_module and v2_pkg_does_not_import_v1_pkg):
            continue

        # Report each unique (v2_module, v1_import) pair once
        for v1_import_name in v1_imports_from_same_package:
            pair_key = (v2_module_name, v1_import_name)
            if pair_key in reported_pairs:
                continue
            reported_pairs.add(pair_key)
            violations.append(
                "circular-import-train: "
                f"v2 module '{v2_module_name}' imports v1 module '{v1_import_name}', "
                f"while v1 package '{v1_package_module}' imports '{v2_module_name}', "
                f"and v2 package '{v2_package_module}' does not import '{v1_package_module}'."
            )
    return violations


def convert_to_file_paths(import_strings: List[str]) -> List[Path]:
    base_dir = get_base_dir()
    file_paths = []
    for import_string in import_strings:
        relative_path = import_string.replace(".", "/") + ".py"
        file_paths.append(base_dir / relative_path)
    return file_paths


def check_violations(init_file: Path, imports: List[str], train_dir: Path) -> List[str]:
    """Refactoring"""

    patch_files = convert_to_file_paths(imports)

    import pdb

    pdb.set_trace()

    patch_file_imports = get_file_module_imports(
        train_dir, patch_files, module_match_string="ray.train"
    )

    import pdb

    pdb.set_trace()

    for patch_module, patch_imports in patch_file_imports.items():
        # Filter out v2 imports
        patch_imports = [
            import_str
            for import_str in patch_imports
            if "ray.train.v2" not in import_str
        ]

        # If any of those v1 import go through the init file, then it is a violation
        pass


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--patch_dir", default="/Users/jasonli/ray/python/ray/train/v2")
    args = parser.parse_args()

    base_dir = get_base_dir()
    train_dir = base_dir / "ray/train"
    patch_dir = Path(args.patch_dir)

    # Parses each file into its module-level imports
    # module_import_dict = get_file_module_imports(train_dir, target_files)

    # Check for violations
    # violations = check_violations(module_import_dict)
    # if violations:
    #     print("\n".join(violations))
    #     return 1
    # return 0

    # Enumerate all the v1 init files, need to exclude the v2 init files
    train_init_files = [
        f for f in train_dir.rglob("__init__.py") if not f.is_relative_to(patch_dir)
    ]

    # Get the v2 imports of all the v1 init files
    dotted_module_prefix = str(patch_dir.relative_to(base_dir)).replace("/", ".")
    patching_imports = get_file_module_imports(
        train_dir, train_init_files, module_match_string=dotted_module_prefix
    )

    import pdb

    pdb.set_trace()

    # For each v1 init file, get the implementation files of the v2 imports
    # Assuming node.module is the implemenration file but if not there can be an edge case
    # when this is not the case, you need to look at node.names and find the implementation file
    for v1_init_file, v2_imports in patching_imports.items():
        check_violations(v1_init_file, v2_imports, train_dir)


if __name__ == "__main__":
    raise SystemExit(main())
