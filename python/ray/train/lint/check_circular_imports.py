"""
The Ray Train Circular Import Linter is designed to address the intricate issue of circular dependencies within the Ray Train framework. Circular import errors arise when two or more modules depend on each other, creating a loop that Python's import system cannot resolve. In the context of Ray Train, this problem is particularly pronounced due to the interdependencies between the `ray.train` and `ray.train.v2` modules.

The core challenge stems from the fact that `ray.train.v2` relies on `ray.train` for foundational functionality, while `ray.train` depends on `ray.train.v2` for enhanced features when the `RAY_TRAIN_V2_ENABLED` flag is set. This bidirectional dependency can lead to import errors, especially when v2 attributes are directly imported or during the deserialization of v2 attributes in train worker setups.

To mitigate these issues, the linter implements several key strategies:
1. **Import Resolution**: By enforcing the importing of `ray.train` v1 modules within `ray.train.v2` init files, the linter guides developers in resolving circular import errors.
2. **Comprehensive Detection**: The linter parses both the v1 and v2 train directories to detect potential circular imports. It ensures that for each v2 patch within the v1 `__init__.py` files, there are no circular imports in the overriding v2 files, or there is an import on the direct import path of the v2 file that suppresses the circular import.
3. **Extensibility**: The linter is designed to be extensible, allowing it to be adapted for other patching directories beyond Ray Train V2.

The decision to build a custom linter was driven by the need for a tool that could specifically address dependencies between two separate directories, a feature not available in existing libraries like pycycle. This custom approach not only allows for tailored solutions but also ensures a lightweight implementation.

### Example Violations:
- **Classic Case**:
  - `ray/train/a/init.py`: `from ray.train.v2.a.sub_module import foo`
  - `ray/train/v2/a/sub_module.py`: `import ray.train.a`
- **Reexport through package**:
  - `ray/train/a/init.py`: `from ray.train.v2.a import foo`
  - `ray/train/v2/a/init.py`: `from .submodule import foo`
  - `ray/train/v2/a/sub_module.py`: `import ray.train.a`

### Example Violation Suppression:
- **Typical Suppression**:
  - `ray/train/a/init.py`: `from ray.train.v2.a.submodule import foo`
  - `ray/train/v2/a/init.py`: `import ray.train.a` -> suppression import
  - `ray/train/v2/a/sub_module.py`: `import ray.train.a`
- **Early Suppression**:
  - `ray/train/a/init.py`: `from ray.train.v2.a.submodule import foo`
  - `ray/train/v2/init.py`: `import ray.train.a` -> suppression import
  - `ray/train/v2/a/sub_module.py`: `import ray.train.a`

By implementing these strategies, the Ray Train Circular Import Linter effectively identifies and helps developers resolve circular import issues, ensuring smoother operation and integration of the Ray Train framework.
"""

import argparse
import ast
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

TRAIN_PACKAGES = set()


def initialize_train_packages(base_train_dir: Path, patch_train_dir: Path) -> None:
    """Initialize the global TRAIN_PACKAGES set with all package directories."""
    global TRAIN_PACKAGES
    TRAIN_PACKAGES = set()

    # Collect all packages under both base and patch train dirs
    package_files = list(base_train_dir.rglob("__init__.py")) + list(
        patch_train_dir.rglob("__init__.py")
    )
    base_dir = get_base_dir()
    for init_file in package_files:
        relative_path = init_file.relative_to(base_dir)
        dotted_module = str(relative_path.parent).replace("/", ".")
        TRAIN_PACKAGES.add(dotted_module)


def is_package(module_str: str) -> bool:
    return module_str in TRAIN_PACKAGES


def does_overlap(main_module: str, module: str) -> bool:
    """Checks if the init file of module is on the import path of main_module"""
    return main_module.startswith(f"{module}.") or main_module == module


class Import:
    """
    Represents an import statement.
    For example, 'from X import A, B' has module 'X' and names ['A', 'B'].
    Also supports 'import X'.
    """

    def __init__(
        self, module: str, names: List[str] = None, is_package: bool = False
    ) -> None:
        self.is_package = is_package
        self.module = module
        self.names = names if names else []


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
        self.imports: Set[Import] = set()
        self.type_checking_imported = False

    # --- private helpers ---

    def _is_type_checking_test(self, expr: ast.AST) -> bool:
        """Return True for `if TYPE_CHECKING` or `if typing.TYPE_CHECKING`."""

        if (
            self.type_checking_imported
            and isinstance(expr, ast.Name)
            and expr.id == "TYPE_CHECKING"
        ):
            return True
        elif (
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
        base_module_parts = (
            package_parts if level == 1 else package_parts[: -(level - 1)]
        )

        # Construct absolute module string
        abs_module_parts = (
            base_module_parts + module_str.split(".")
            if module_str
            else base_module_parts
        )
        return ".".join(abs_module_parts)

    # --- parsing functions ---

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
                self.imports.add(
                    Import(module=alias.name, is_package=is_package(alias.name))
                )

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        import_str = self._to_absolute_module(node.level or 0, node.module)
        if not import_str:
            return

        names = [alias.name for alias in node.names]
        self.imports.add(
            Import(module=import_str, is_package=is_package(import_str), names=names)
        )
        if "TYPE_CHECKING" in names and import_str == "typing":
            self.type_checking_imported = True

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        # Skip function contents
        return

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        # Skip function contents
        return

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        # Skip class contents
        return


def collect_imports(
    module_name: str, is_package: bool, source_text: str
) -> Set[Import]:
    try:
        tree = ast.parse(source_text)
    except SyntaxError:
        print(f"Warning: Failed to parse {module_name} for circular imports")
        return set()
    collector = ImportCollector(module_name, is_package)
    collector.visit(tree)
    return collector.imports


def get_base_dir() -> Path:
    """Return the filesystem path to the ray python directory."""
    import ray

    package_dir = Path(os.path.dirname(ray.__file__)).parent
    return package_dir


def get_base_train_dir() -> Path:
    """Return the filesystem path to the ray train directory."""
    return get_base_dir() / "ray/train"


def to_module_name_and_is_package(py_file: Path) -> Tuple[str, bool]:
    """
    Convert a Python file path to its corresponding module name and determine if it is a package.

    Args:
        py_file: The path to the Python file.

    Returns:
        Tuple[str, bool]: A tuple containing the module name as a string and a boolean indicating
                          whether the module is a package (True if it is an __init__.py file).
    """
    file_path = py_file.relative_to(get_base_dir())
    module_path = file_path.with_suffix("")
    module_parts = module_path.parts
    is_package = module_parts[-1] == "__init__"
    if is_package:
        module_parts = module_parts[:-1]
    module_str = ".".join(module_parts)
    return module_str, is_package


def get_file_module_imports(
    files: List[Path], module_match_string: Optional[str] = None
) -> Dict[str, List[Import]]:
    """
    Collect and return the module-level imports for a list of Python files.

    Args:
        files: A list of Path objects representing Python files to analyze.
        module_match_string: An optional string to filter imports. Only imports
                             containing this string will be included in the result.

    Returns:
        A dictionary mapping module names to a list of their import statements.
        The module names are derived from the file paths, and the import statements
        are filtered based on the optional module_match_string.
    """
    module_imports: Dict[str, List[Import]] = {}

    # Collect the imports for each python file
    for py_file in files:
        try:
            module_name, is_package = to_module_name_and_is_package(py_file)
            src = py_file.read_text(encoding="utf-8", errors="ignore")
            imports = collect_imports(module_name, is_package, src)
            module_imports[module_name] = [
                stmt
                for stmt in imports
                if module_match_string is None or module_match_string in stmt.module
            ]
        except Exception:
            continue
    return module_imports


def convert_to_file_paths(imports: List[Import]) -> List[Path]:
    """
    Convert a list of import strings to a list of file paths.

    Args:
        imports: A list of Import objects

    Returns:
        A list of file paths.
    """
    base_dir = get_base_dir()
    file_paths = []
    for imp in imports:
        if imp.is_package:
            relative_path = imp.module.replace(".", "/") + "/__init__.py"
        else:
            relative_path = imp.module.replace(".", "/") + ".py"
        file_paths.append(base_dir / relative_path)
    return file_paths


def expand_to_include_reexports(import_map: Dict[str, List[Import]]) -> None:
    """
    Expands the set of imports for a given import map to include the modules resulting from reexports.
    So if in the base train module, there is "from x import a, b" and x is a package, then this function
    will explore the __init__.py of x and include the modules a and b were reexported from in the import map.
    """
    for module, base_imports in import_map.items():
        # Get only the package imports
        packages = [imp for imp in base_imports if imp.is_package]
        package_files = convert_to_file_paths(packages)
        reexports = get_file_module_imports(package_files)

        agg_reexports = []
        # Filter patch init file imports to those that only contain the right names
        for base_import in base_imports:
            if base_import.module in reexports:
                import_list = reexports[base_import.module]
                target_reexports = [
                    imp
                    for imp in import_list
                    if set(imp.names) & set(base_import.names)
                ]
                agg_reexports.extend(target_reexports)

        # Expand modules to include reexported modules
        import_map[module].extend(agg_reexports)


def check_violations(
    base_train_patching_imports: Dict[str, List[Import]], patch_dir: Path
) -> List[str]:
    """
    Check for circular import violations between base and patch train modules.

    Args:
        base_train_patching_imports: A dictionary mapping base train module names to their imports.
        patch_dir: The directory path containing patch train modules.

    Returns:
        A list of strings describing any circular import violations found.
    """
    violations: List[str] = []

    # Get the imports from the patch train init files
    patch_train_init_files = list(patch_dir.rglob("__init__.py"))
    patch_train_init_imports = get_file_module_imports(
        patch_train_init_files, module_match_string="ray.train"
    )

    # Expand the imports to include reexports
    expand_to_include_reexports(base_train_patching_imports)

    # Process each patch train init module
    for base_train_init_module, imports in base_train_patching_imports.items():

        # Get the imports from the patch train files
        patch_train_files = convert_to_file_paths(imports)
        patch_train_file_imports = get_file_module_imports(
            patch_train_files, module_match_string="ray.train"
        )

        for patch_module, imports in patch_train_file_imports.items():
            # Skip if the base train init module is in the import path of the patch module
            if does_overlap(patch_module, base_train_init_module):
                continue

            # Skip if the patch train module init file imports the base train init module
            patch_init_module = (
                ".".join(patch_module.split(".")[:-1])
                if not is_package(patch_module)
                else patch_module
            )
            patch_init_imports = patch_train_init_imports.get(patch_init_module, [])
            if any(
                does_overlap(imp.module, base_train_init_module)
                for imp in patch_init_imports
            ):
                continue

            for patch_import in imports:
                # If any of those v1 imports go through the init file, then it is a violation
                if does_overlap(patch_import.module, base_train_init_module):
                    violations.append(
                        f"circular-import-train: Circular import between {base_train_init_module} (importing {patch_module}) and {patch_module} (importing {patch_import.module}). Resolve by importing {base_train_init_module} in the __init__.py of {patch_init_module}."
                    )

    return violations


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--patch_dir",
        default="ray/train/v2",
        help="Path to the directory containing patching contents",
    )
    args = parser.parse_args()

    # Compute relevant paths
    base_dir = get_base_dir()
    base_train_dir = get_base_train_dir()
    patch_train_dir = base_dir / Path(args.patch_dir)

    # Initialize train packages
    initialize_train_packages(base_train_dir, patch_train_dir)

    # Enumerate all the base train init files, excluding the patch train init files
    base_train_init_files = [
        f
        for f in base_train_dir.rglob("__init__.py")
        if not f.is_relative_to(patch_train_dir)
    ]

    # Get the patch train imports of all the base train init files
    dotted_module_prefix = str(patch_train_dir.relative_to(base_dir)).replace("/", ".")
    patching_imports = get_file_module_imports(
        base_train_init_files, module_match_string=dotted_module_prefix
    )

    violations = check_violations(patching_imports, patch_train_dir)
    if violations:
        print("\n".join(violations))
        sys.exit(1)


if __name__ == "__main__":
    main()
