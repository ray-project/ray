import collections
from pathlib import Path
import yaml

# ray/doc/source/examples/ -> ray
ray_root_path = (Path(__file__).parent / ".." / ".." / "..").resolve()
templates_catalog_path = Path(__file__).parent / "templates.yaml"

with open(templates_catalog_path, "r") as f:
    templates = yaml.safe_load(f)

invalid = collections.defaultdict(list)

required_fields = {"name", "path", "cluster_env", "small", "large"}
for i, template in enumerate(templates):
    name = template.get("name", i)

    missing_fields = set(template) - required_fields
    assert not missing_fields, f"Missing fields for {name}: {missing_fields}"

    rel_path = template["path"]
    if not (ray_root_path / rel_path).exists():
        invalid[name].append(rel_path)

    rel_path = template["cluster_env"]
    if not (ray_root_path / rel_path).exists():
        invalid[name].append(rel_path)

    required_per_size = {"compute_config"}
    sizes = ["small", "large"]

    for size in sizes:
        configs = template[size]
        missing = set(configs) - required_per_size
        assert not missing, f"Missing fields for {name} ({size}): {missing_fields}"

        rel_paths = list(configs["compute_config"].values())
        for rel_path in rel_paths:
            if not (ray_root_path / rel_path).exists():
                invalid[name].append(rel_path)

if invalid:
    print("VALIDATION FAILED!! Please fix the paths listed below:\n\n")

    for name, invalid_paths in invalid.items():
        print("Template Name:", name)
        for path in invalid_paths:
            print("-", path)
        print()
else:
    print("Success!")
