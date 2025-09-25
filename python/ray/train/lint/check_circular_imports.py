import argparse
import ast
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
import sys


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


def get_base_train_dir() -> Path:
    return get_base_dir() / "ray/train"


def to_module_name_and_is_package(py_file: Path) -> Tuple[str, bool]:
    """Returns tuple (dotted module name, is_package) for a given Python file."""
    file_path = py_file.relative_to(get_base_dir())
    module_path = file_path.with_suffix("")
    module_parts = module_path.parts
    is_package = module_parts[-1] == "__init__"
    if is_package:
        module_parts = module_parts[:-1]
    module_str = ".".join(module_parts)
    return module_str, is_package


def get_file_module_imports(files: List[Path], module_match_string: Optional[str] = None) -> Dict[str, List[str]]:
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
            module_name, is_package = to_module_name_and_is_package(py_file)
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


def convert_to_file_paths(import_strings: List[str]) -> List[Path]:
    base_dir = get_base_dir()
    file_paths = []
    for import_string in import_strings:
        relative_path = import_string.replace(".", "/") + ".py"
        file_paths.append(base_dir / relative_path)
    return file_paths


def check_violations(base_train_patching_imports: Dict[str, List[str]], patch_dir: Path) -> List[str]:
    """Refactoring"""
    violations: List[str] = []

    # Get the imports from the patch train init files
    patch_train_init_files = [f for f in patch_dir.rglob("__init__.py")]
    patch_train_init_imports = get_file_module_imports(patch_train_init_files, module_match_string="ray.train")

    # Process each init module 
    for base_train_init_module, imports in base_train_patching_imports.items():

        patch_train_files = convert_to_file_paths(imports)
        patch_train_file_imports = get_file_module_imports(patch_train_files, module_match_string="ray.train")


        for patch_module, imports in patch_train_file_imports.items():
            # Skip if the base train init module is in the import path of the patch module
            if base_train_init_module in patch_module:
                continue

            # If the patch train module init file imports the base train init module, violations are avoided
            patch_init_module = ".".join(patch_module.split(".")[:-1])
            if base_train_init_module in patch_train_init_imports.get(patch_init_module, set()):
                continue

            for patch_import in imports:
                if "ray.train.v2" in patch_import:
                    continue

                # If any of those v1 imports go through the init file, then it is a violation
                if base_train_init_module in patch_import:
                    violations.append(f"circular-import-train: Circular import between {base_train_init_module} (importing {patch_module}) and {patch_module} (importing {patch_import}). Resolve by importing {base_train_init_module} in the __init__.py of {patch_init_module}.")
    
    return violations


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--patch_dir", default="/Users/jasonli/ray/python/ray/train/v2")
    args = parser.parse_args()

    base_dir = get_base_dir()
    base_train_dir = get_base_train_dir()
    patch_train_dir = Path(args.patch_dir)

    # Enumerate all the v1 init files, need to exclude the v2 init files
    base_train_init_files = [
        f for f in base_train_dir.rglob("__init__.py") if not f.is_relative_to(patch_train_dir)
    ]

    # Get the v2 imports of all the v1 init files
    dotted_module_prefix = str(patch_train_dir.relative_to(base_dir)).replace("/", ".")
    patching_imports = get_file_module_imports(base_train_init_files, module_match_string=dotted_module_prefix)

    violations = check_violations(patching_imports, patch_train_dir)
    if violations:
        print("\n".join(violations))
        sys.exit(1)


if __name__ == "__main__":
    main()
