import collections
import os
from pathlib import Path
import yaml


def rel_path_to_doc(rel_path_to_ray) -> Path:
    return Path(rel_path_to_ray).relative_to("doc")


if __name__ == "__main__":
    # For CI, Bazel will set this environment variable as the "data" directory.
    test_data_dir = os.environ.get("TEST_SRCDIR")

    if test_data_dir:
        doc_path = Path(test_data_dir)
    else:
        # Otherwise, we're running from a Ray repo, and just use the
        # current file to get the doc directory.
        # ray/doc/source/examples/ -> ray/doc
        doc_path = (Path(__file__).parent / ".." / "..").resolve()

    templates_catalog_path = doc_path / "source/templates/templates.yaml"

    with open(templates_catalog_path, "r") as f:
        templates = yaml.safe_load(f)

    all_missing_providers = {}
    all_missing_fields = {}
    invalid_paths = collections.defaultdict(list)

    required_fields = {"title", "description", "path", "cluster_env", "compute_config"}
    required_cloud_providers = {"AWS", "GCP"}

    for i, (template_name, template_config) in enumerate(templates.items()):
        # ======= Schema check for templates.yaml ========
        missing_fields = required_fields - set(template_config)
        if missing_fields:
            all_missing_fields[template_name] = missing_fields
            continue

        # ======= Check template `path` ========
        # The yaml specifies relative paths to the ray root directory: doc/a/b/c
        rel_path_to_ray = template_config["path"]
        # Relative path to the ray/doc directory: -> a/b/c
        rel_path = rel_path_to_doc(rel_path_to_ray)
        if not (doc_path / rel_path).exists():
            invalid_paths[template_name].append(rel_path)

        # ======= Check template `cluster_env` ========
        cluster_env_rel_path = rel_path_to_doc(template_config["cluster_env"])
        if not (doc_path / cluster_env_rel_path).exists():
            invalid_paths[template_name].append(cluster_env_rel_path)

        # ======= Check template `compute_config` ========
        compute_config_per_provider = template_config["compute_config"]

        missing_providers = required_cloud_providers - set(compute_config_per_provider)
        if missing_providers:
            all_missing_providers[template_name] = missing_providers
            continue

        rel_paths = [
            rel_path_to_doc(path) for path in compute_config_per_provider.values()
        ]
        for rel_path in rel_paths:
            if not (doc_path / rel_path).exists():
                invalid_paths[template_name].append(rel_path)

    # ======= Print an informative error message. ========
    if all_missing_fields or all_missing_providers or invalid_paths:
        msg = "TEMPLATES VALIDATION FAILED!! Please fix the issues listed below:\n\n"

        if all_missing_fields:
            msg += "Please supply missing fields in `templates.yaml`:\n"
            for template_name, missing_fields in all_missing_fields.items():
                msg += f"- {template_name}: {missing_fields}\n"

        if all_missing_providers:
            msg += (
                "\nPlease supply paths to compute configs for these cloud providers "
                "in `templates.yaml`:\n"
            )
            for template_name, missing_providers in all_missing_providers.items():
                msg += f"- {template_name}: {missing_providers}\n"

        if invalid_paths:
            msg += "\nPlease fix invalid paths in `templates.yaml`:\n"
            for template_name, invalid_paths_for_template in invalid_paths.items():
                msg += f"- {template_name}:\n"
                msg += "\n".join([f"\t- {path}" for path in invalid_paths_for_template])
                msg += "\n"

        raise ValueError(msg)
    else:
        print("Success!")
