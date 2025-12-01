import textwrap
from pathlib import Path

import pytest

from ray.train.lint import check_circular_imports as cci


def test_import_collector_excludes_non_module_level_and_type_checking():
    source = textwrap.dedent(
        """
        import os
        from typing import TYPE_CHECKING
        from .submod import thing

        if TYPE_CHECKING:
            import foo

        def f():
            import json

        class C:
            import pkg
        """
    )

    imports = cci.collect_imports(
        module_name="pkg.module", is_package=False, source_text=source
    )
    imports = [imp.module for imp in imports]
    assert "os" in imports
    assert "pkg.submod" in imports
    assert "foo" not in imports
    assert "json" not in imports
    assert "pkg" not in imports


def test_to_module_name_and_is_package(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    # Create a fake python tree under tmp: tmp/python/ray/train/lint/{pkg}/...
    base_dir = tmp_path / "python"
    pkg_dir = base_dir / "ray" / "train" / "lint"
    pkg_dir.mkdir(parents=True, exist_ok=True)

    init_pkg = pkg_dir / "foo" / "__init__.py"
    init_pkg.parent.mkdir(parents=True, exist_ok=True)
    init_pkg.write_text("# pkg init")

    mod_file = pkg_dir / "bar.py"
    mod_file.write_text("# module file")

    monkeypatch.setattr(cci, "get_base_dir", lambda: base_dir)

    module_name, is_pkg = cci.to_module_name_and_is_package(init_pkg)
    assert module_name == "ray.train.lint.foo"
    assert is_pkg is True

    module_name, is_pkg = cci.to_module_name_and_is_package(mod_file)
    assert module_name == "ray.train.lint.bar"
    assert is_pkg is False


def test_get_file_module_imports_filters_by_prefix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    base_dir = tmp_path / "python"
    target_dir = base_dir / "ray" / "train" / "demo"
    target_dir.mkdir(parents=True, exist_ok=True)

    file_a = target_dir / "a.py"
    file_a.write_text(
        "\n".join(
            [
                "import os",
                "from ray.train.v2.torch import torch_trainer",
                "from some.other import mod",
            ]
        )
    )

    file_b = target_dir / "b.py"
    file_b.write_text("from ray.train import something")

    monkeypatch.setattr(cci, "get_base_dir", lambda: base_dir)
    cci.find_train_packages(base_dir, target_dir)

    result = cci.get_file_module_imports(
        [file_a, file_b], module_match_string="ray.train"
    )
    # Keys are dotted module names
    assert set(result.keys()) == {"ray.train.demo.a", "ray.train.demo.b"}
    # Imports were found
    assert len(result["ray.train.demo.a"]) == 1
    assert len(result["ray.train.demo.b"]) == 1
    # Only imports containing the prefix are kept
    assert result["ray.train.demo.a"][0].module == "ray.train.v2.torch"
    assert result["ray.train.demo.b"][0].module == "ray.train"


def test_check_standard_violations_reports_and_suppresses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    base_dir = tmp_path / "python"
    train_dir = base_dir / "ray" / "train"
    patch_dir = train_dir / "v2"
    v2_dir = train_dir / "v2" / "tensorflow"
    v1_pkg_dir = train_dir / "tensorflow"
    v2_dir.mkdir(parents=True, exist_ok=True)
    v1_pkg_dir.mkdir(parents=True, exist_ok=True)

    # Base v1 package init: imports a v2 module
    (v1_pkg_dir / "__init__.py").write_text(
        "from ray.train.v2.tensorflow.tensorflow_trainer import tensorflow_trainer\n"
    )

    # v2 module that (incorrectly) imports back into v1 package
    (v2_dir / "tensorflow_trainer.py").write_text(
        "from ray.train.tensorflow import something\n"
    )

    # Extra v2 module that should not be checked
    (v2_dir / "foo.py").write_text("from ray.train.tensorflow import something\n")

    # v2 package init WITHOUT importing v1 package (should trigger violation)
    (v2_dir / "__init__.py").write_text("# empty init\n")

    monkeypatch.setattr(cci, "get_base_dir", lambda: base_dir)
    cci.find_train_packages(base_dir, patch_dir)

    # Build mapping: base v1 init module -> imports of v2 it references
    base_v1_init = train_dir / "tensorflow" / "__init__.py"
    imports_map = cci.get_file_module_imports([base_v1_init])

    violations = cci.check_violations(imports_map, patch_dir=train_dir / "v2")
    assert len(violations) == 1

    # Now fix by having v2 package init import the v1 package init (suppresses violation)
    (v2_dir / "__init__.py").write_text("import ray.train.tensorflow\n")

    violations = cci.check_violations(imports_map, patch_dir=train_dir / "v2")
    assert violations == []


def test_check_no_violation_on_overlapping_import_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    base_dir = tmp_path / "python"
    train_dir = base_dir / "ray" / "train"
    patch_dir = train_dir / "v2"
    v2_dir = train_dir / "v2" / "tensorflow"
    v2_dir.mkdir(parents=True, exist_ok=True)

    # Circular dependency between ray.train and v2 module
    (v2_dir / "tensorflow_trainer.py").write_text("from ray.train import something\n")
    (train_dir / "__init__.py").write_text(
        "from ray.train.v2.tensorflow.tensorflow_trainer import TensorflowTrainer\n"
    )

    monkeypatch.setattr(cci, "get_base_dir", lambda: base_dir)
    cci.find_train_packages(base_dir, patch_dir)

    # Build mapping: base v1 init module -> imports of v2 it references
    base_v1_init = train_dir / "__init__.py"
    imports_map = cci.get_file_module_imports([base_v1_init])

    violations = cci.check_violations(imports_map, patch_dir=patch_dir)
    assert len(violations) == 0


def test_expand_to_exclude_reexports(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    base_dir = tmp_path / "python"
    train_dir = base_dir / "ray" / "train"
    patch_dir = train_dir / "v2"
    v2_dir = train_dir / "v2" / "tensorflow"
    v2_dir.mkdir(parents=True, exist_ok=True)

    # Import from v2 init file
    (train_dir / "__init__.py").write_text(
        "from ray.train.v2.tensorflow import TensorflowTrainer\n"
    )
    # Reexport tensorflow_trainer from v2 init file
    (v2_dir / "__init__.py").write_text(
        "from .tensorflow_trainer import TensorflowTrainer \n"
    )
    # Circular dependency with ray.train
    (v2_dir / "tensorflow_trainer.py").write_text("from ray.train import something\n")

    monkeypatch.setattr(cci, "get_base_dir", lambda: base_dir)
    cci.find_train_packages(base_dir, patch_dir)

    # Build mapping: base v1 init module -> imports of v2 it references
    base_v1_init = train_dir / "__init__.py"
    imports_map = cci.get_file_module_imports([base_v1_init])

    assert imports_map.keys()
    assert "ray.train" in imports_map.keys()
    assert isinstance(imports_map["ray.train"], list)
    assert imports_map["ray.train"]
    assert isinstance(imports_map["ray.train"][0], cci.Import)
    assert imports_map["ray.train"][0].module == "ray.train.v2.tensorflow"

    cci.expand_to_include_reexports(imports_map)
    assert len(imports_map["ray.train"]) == 2

    # The tensorflow trainer is not included in the imports_map
    trainer_module = "ray.train.v2.tensorflow.tensorflow_trainer"
    assert any(imp.module == trainer_module for imp in imports_map["ray.train"])


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
