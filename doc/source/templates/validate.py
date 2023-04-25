import collections
import os
from pathlib import Path
import yaml


def get_doc_path() -> Path:
    # For CI, Bazel will set this environment variable as the "data" directory.
    test_data_dir = os.environ.get("TEST_SRCDIR")

    if test_data_dir:
        doc_path = Path(test_data_dir)
    else:
        # Otherwise, we're running from a Ray repo, and just use the
        # current file to get the doc directory.
        # ray/doc/source/examples/ -> ray/doc
        doc_path = (Path(__file__).parent / ".." / "..").resolve()

    return doc_path


def rel_path_to_doc(rel_path_to_ray) -> Path:
    return Path(rel_path_to_ray).relative_to("doc")


def validate_templates_yaml_schema(templates) -> dict:
    all_missing_fields = {}
    required_fields = {"title", "description", "path", "cluster_env", "compute_config"}

    for template_name, template_config in templates.items():
        # ======= Schema check for templates.yaml ========
        missing_fields = required_fields - set(template_config)
        if missing_fields:
            all_missing_fields[template_name] = missing_fields
            continue

    return all_missing_fields


def validate_template_paths(templates, invalid_paths) -> None:
    doc_path = get_doc_path()

    for template_name, template_config in templates.items():
        if "path" not in template_config:
            continue

        # The yaml specifies relative paths to the ray root directory: doc/a/b/c
        rel_path_to_ray = template_config["path"]
        # Relative path to the ray/doc directory: -> a/b/c
        rel_path = rel_path_to_doc(rel_path_to_ray)
        if not (doc_path / rel_path).exists():
            invalid_paths[template_name].append(rel_path)


def validate_cluster_envs(templates, invalid_paths, invalid_yamls) -> None:
    doc_path = get_doc_path()

    for template_name, template_config in templates.items():
        if "cluster_env" not in template_config:
            continue

        cluster_env_rel_path = rel_path_to_doc(template_config["cluster_env"])
        cluster_env_path = doc_path / cluster_env_rel_path
        if not cluster_env_path.exists():
            invalid_paths[template_name].append(cluster_env_rel_path)
        else:
            try:
                # Assert that the yaml file is properly formatted.
                with open(cluster_env_path, "r") as f:
                    yaml.safe_load(f)
            except yaml.parser.ParserError as e:
                invalid_yamls[template_name].append(str(e))


def validate_compute_configs(templates, invalid_paths, invalid_yamls) -> dict:
    doc_path = get_doc_path()
    required_cloud_providers = {"AWS", "GCP"}

    all_missing_providers = {}

    for template_name, template_config in templates.items():
        if "compute_config" not in template_config:
            continue

        compute_config_per_provider = template_config["compute_config"]

        missing_providers = required_cloud_providers - set(compute_config_per_provider)
        if missing_providers:
            all_missing_providers[template_name] = missing_providers
            continue

        rel_paths = [
            rel_path_to_doc(path) for path in compute_config_per_provider.values()
        ]
        for rel_path in rel_paths:
            compute_config_path = doc_path / rel_path
            if not compute_config_path.exists():
                invalid_paths[template_name].append(rel_path)
            else:
                try:
                    # Assert that the yaml file is properly formatted.
                    with open(compute_config_path, "r") as f:
                        yaml.safe_load(f)
                except yaml.parser.ParserError as e:
                    invalid_yamls[template_name].append(str(e))

    return all_missing_providers


if __name__ == "__main__":
    doc_path = get_doc_path()
    templates_catalog_path = doc_path / "source/templates/templates.yaml"

    with open(templates_catalog_path, "r") as f:
        templates = yaml.safe_load(f)

    invalid_paths = collections.defaultdict(list)
    invalid_yamls = collections.defaultdict(list)

    all_missing_fields = validate_templates_yaml_schema(templates)
    validate_template_paths(templates, invalid_paths)
    validate_cluster_envs(templates, invalid_paths, invalid_yamls)
    all_missing_providers = validate_compute_configs(
        templates, invalid_paths, invalid_yamls
    )

    # ======= Print an informative error message. ========
    if any([all_missing_fields, all_missing_providers, invalid_paths, invalid_yamls]):
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

        if invalid_yamls:
            msg += "\nPlease fix invalid configuration yamls:\n"
            for template_name, invalid_yamls_per_template in invalid_yamls.items():
                msg += f"- {template_name}:\n\n"
                msg += "\n\n".join(
                    f"{i + 1}. {invalid_yaml}"
                    for i, invalid_yaml in enumerate(invalid_yamls_per_template)
                )
                msg += "\n\n"

        raise ValueError(msg)
    else:
        print("Success!")
