#!/usr/bin/env python3
import argparse
import glob
import json
import logging
import os
import re
import shutil
import subprocess
from typing import Dict, List, Optional, Set, Tuple

import yaml

logger = logging.getLogger("ray_oss_analysis")


def _setup_logger(log_file: Optional[str] = None, enable_debug: bool = False) -> None:
    """
    Setup logger for the script.
    You can either use default console logger or enable additional file logger if needed by passing the log_file.
    Setting the log level to debug if enable_debug is true.
    """
    logger.setLevel(logging.DEBUG if enable_debug else logging.INFO)

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    if log_file:
        file_handler = logging.FileHandler(log_file, mode="w")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.info(f"Logging to file: {log_file}")

    logger.info(f"Log Level Set to {logging.getLevelName(logger.level)}")


# not being used since we moved to filtering only for source files to get c, cpp libraries
def _is_excluded_kind(kind_str: str) -> bool:
    """
    Check if the kind is excluded.
    """
    # split the kind_str by whitespace and get the first element as the kind
    kind = kind_str.split(" ")[0]
    # list of non-target rule kinds
    non_target_rule_kinds = [
        "config_setting",
        "pkg_files",
        "pkg_zip_impl",
        "int_flag",
        "string_flag",
        "bool_flag",
        "bind",
        "constraint_value",
        "constraint_setting",
        "GENERATED_FILE",
    ]
    python_rule_kinds = ["py_library", "py_binary", "py_test"]

    target_rule_kinds = non_target_rule_kinds + python_rule_kinds
    return kind in target_rule_kinds


def _is_build_tool(label: str) -> bool:
    """
    Check if the label is a build tool.
    """
    # list of build package labels that are present in dependencies but not part of the target code
    build_package_labels = [
        "bazel_tools",
        "local_config_python",
        "cython",
        "local_config_cc",
    ]
    return any(
        build_package_label in label for build_package_label in build_package_labels
    )


def _is_own_code(location: str) -> bool:
    """
    Check if it is own code or not.
    """
    if location is None:
        return False
    else:
        return location.startswith(os.getcwd())


def _is_cpp_code(location: str) -> bool:
    """
    Check if the label is C/C++ code.
    """
    # list of C/C++ file extensions
    cExtensions = [".c", ".cc", ".cpp", ".cxx", ".c++", ".h", ".hpp", ".hxx"]
    return any(location.endswith(ext) for ext in cExtensions)


def _get_dependency_info(line_json: Dict) -> Tuple[str, str, str, str]:
    """
    Get dependency info from the json line.
    """
    # earlier when we were getting all types of packages there was a need to get the type of the package,
    #  but now we are only getting source files and we are not interested in the type of the package
    # Yet we are keeping the code here, for future needs
    type = line_json["type"]
    match type:
        case "SOURCE_FILE":
            return (
                type,
                type,
                line_json["sourceFile"]["name"],
                _clean_path(line_json["sourceFile"]["location"]),
            )
        case "GENERATED_FILE":
            return (
                type,
                type,
                line_json["generatedFile"]["name"],
                _clean_path(line_json["generatedFile"]["location"]),
            )
        case "PACKAGE_GROUP":
            return type, type, line_json["packageGroup"]["name"], None
        case "RULE":
            return (
                type,
                line_json["rule"]["ruleClass"],
                line_json["rule"]["name"],
                line_json["rule"]["location"],
            )
        case _:
            return type, type, "unknown", "unknown"


def _clean_path(path: str) -> str:
    """
    Clean the path by removing location info.
    """
    # Remove location information (e.g., :line:column) from the path
    # Format is typically: /path/to/file.ext:line:column
    return path.split(":")[0]


def _get_package_name(label: str) -> Optional[str]:
    """
    Extract package name from bazel label.
    matches @repo//pkg:target to repo. regex breaks the string into groups and we return the first group.
    separated out so that this can be tested.
    Returns None if the label doesn't have an external package name (e.g., local targets like //pkg:target).
    """
    match = re.search(r"(?:@([^/]+))?//", label)
    if match:
        return match.group(1)
    return None


def _get_bazel_dependencies(
    package_name: str, bazel_command: str
) -> Tuple[Set[str], Set[str]]:
    """
    Returns the package names and file paths of the dependencies minus build tools and own code.
    Currently works only for c, cpp.
    """
    # package names of dependencies
    # file paths for actual files used

    package_names = set()
    file_paths = set()

    # works for c, cpp, not sure if the kind based filter works for other languages
    command = [
        bazel_command,
        "query",
        "--output=streamed_jsonproto",
        f"kind('source file', deps({package_name}))",
    ]

    logger.debug(f"Running command: {command}")
    lines = subprocess.check_output(command, text=True).splitlines()
    logger.debug(f"Found {len(lines)} dependencies")
    for line in lines:
        line_json = json.loads(line)
        type, kind, label, location = _get_dependency_info(line_json)
        logger.debug(f"Dependency type: {type},  Label: {label}, Location: {location}")

        if _is_build_tool(label) or _is_own_code(location):
            logger.debug(f"Skipping dependency: {line} because it is a bad kind")
            continue
        elif _is_cpp_code(location):
            file_paths.add(location)
            package_name = _get_package_name(label)
            if package_name is not None:
                package_names.add(package_name)

    return package_names, file_paths


def _copy_single_file(source: str, destination: str) -> None:
    """
    Copy a single file from source to destination.
    """
    # Create parent directories if they don't exist
    os.makedirs(os.path.dirname(destination), exist_ok=True)
    # Copy the file
    try:
        shutil.copy(source, destination)
    except FileNotFoundError:
        logger.warning(f"File not found, skipping: {source}")


def _copy_files(file_paths: Set[str], output_folder: str) -> None:
    """
    Copy files to output folder.
    """
    for file_path in file_paths:
        logger.debug(f"Copying file: {file_path}")
        destination = os.path.join(output_folder, file_path.split("external/")[-1])

        _copy_single_file(file_path, destination)


def _copy_licenses(
    package_names: Set[str], bazel_output_base: str, output_folder: str
) -> None:
    """
    Copy licenses to output folder.
    """
    for package_name in package_names:
        license_paths = _expand_license_files(
            os.path.join(bazel_output_base, "external", package_name)
        )
        for license_path in license_paths:
            _copy_single_file(
                license_path,
                os.path.join(output_folder, license_path.split("external/")[-1]),
            )


def _askalono_crawl(path: str) -> List[Dict]:
    """
    Crawl licenses using askalono.
    """
    license_text = subprocess.check_output(
        ["askalono", "--format=json", "crawl", path],
        text=True,
    ).strip()
    licenses = [json.loads(license_text) for license_text in license_text.splitlines()]
    cleaned_licenses = [license for license in licenses if "error" not in license]
    error_licenses = [license for license in licenses if "error" in license]
    for error_license in error_licenses:
        logger.debug(
            f"License Crawl failed for {error_license['path']}: {error_license['error']}"
        )
    return cleaned_licenses


def _expand_license_files(path: str) -> List[str]:
    """
    Expand license files using glob patterns.
    """
    patterns = [
        "**/[Ll][Ii][Cc][Ee][Nn][Ss][Ee]*",  # LICENSE
        "**/[Cc][Oo][Pp][Yy][Ii][Nn][Gg]*",  # COPYING
        "**/[Nn][Oo][Tt][Ii][Cc][Ee]*",  # NOTICE
        "**/[Cc][Oo][Pp][Yy][Rr][Ii][Gg][Hh][Tt]*",  # COPYRIGHT
        "**/[Rr][Ee][Aa][Dd][Mm][Ee]*",  # README
    ]
    all_paths = set()
    for pattern in patterns:
        full_pattern = os.path.join(path, pattern)
        matching_paths = glob.glob(full_pattern, recursive=True)
        logger.debug(f"Pattern {full_pattern} matched {len(matching_paths)} files")
        # Only include files, not directories
        all_paths.update(p for p in matching_paths if os.path.isfile(p))
    return list(all_paths)


def _expand_and_crawl(path: str) -> List[Dict]:
    """
    Given a path, crawl licenses using askalono.
    Works recursively for all licenses in the path.
    """
    license_paths = _expand_license_files(path)
    all_licenses = []
    for license_path in license_paths:
        licenses = _askalono_crawl(license_path)
        all_licenses.extend(licenses)
    return all_licenses


def _get_askalono_results(dependencies: Set[str], bazel_output_base: str) -> List[Dict]:
    """
    Get askalono results for all dependencies.
    """
    license_info = []
    for dependency in dependencies:
        dependency_path = os.path.join(bazel_output_base, "external", dependency)
        license_json = _askalono_crawl(dependency_path)
        if not license_json:
            logger.warning(
                f"No license text found for {dependency}, trying to crawl licenses and copying files manually"
            )
            license_json = _expand_and_crawl(dependency_path)
        if not license_json:
            logger.warning(f"No license text found for {dependency}")
            license_info.append(
                {
                    "dependency": dependency,
                    "path": "unknown",
                    "license": "unknown",
                }
            )
            continue
        for license in license_json:
            license_info.append(
                {
                    "dependency": dependency,
                    "path": license["path"].split("external/")[-1],
                    "license": license["result"]["license"]["name"],
                }
            )
    return license_info


def _generate_fossa_deps_file(askalono_results: List[Dict], output_folder: str) -> None:
    """
    Generate fossa dependencies file from askalono results.
    """
    # Group licenses and file paths by dependency
    dependency_data = {}
    for result in askalono_results:
        logger.debug("generating fossa deps file: result: %s", result)
        dep = result["dependency"]
        license_name = result["license"]
        license_file_path = result.get("path", "N/A")

        if dep not in dependency_data:
            dependency_data[dep] = {"licenses": set(), "file_licenses": []}

        dependency_data[dep]["licenses"].add(license_name)
        dependency_data[dep]["file_licenses"].append(
            f"{license_file_path.split('external/')[-1]}: {license_name}"
        )

    # Create custom dependencies with aggregated licenses
    custom_dependencies = []
    for dependency, data in sorted(dependency_data.items()):
        licenses = data["licenses"]
        file_licenses = data["file_licenses"]
        logger.debug(
            f"generating fossa deps file: Dependency: {dependency}, Licenses: {licenses}"
        )

        # Build description with file paths and licenses
        description_parts = ["generated by ray_oss_analysis.py, askalono scan results."]
        if file_licenses:
            description_parts.append("License files: " + "; ".join(file_licenses))

        custom_dependencies.append(
            {
                "name": dependency,
                "license": " or ".join(sorted(licenses)),
                "version": "Non-versioned",
                "metadata": {
                    "description": " ".join(description_parts),
                },
            }
        )

    fossa_deps_file = {"custom-dependencies": custom_dependencies}

    # Write to YAML file
    with open(os.path.join(output_folder, "fossa_deps.yaml"), "w") as file:
        yaml.dump(fossa_deps_file, file, indent=4, sort_keys=False)


def _change_working_directory() -> None:
    """
    Change working directory to the workspace in case being executed as bazel py_binary.
    """
    workspace = os.environ.get("BUILD_WORKING_DIRECTORY")
    if workspace:
        os.chdir(workspace)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OSS Analysis Combo Tool")

    parser.add_argument("-o", "--output", help="Output folder path", required=True)
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose output"
    )
    parser.add_argument(
        "-cmd", "--bazel-cmd", help="Bazel command bazel/bazelisk", default="bazel"
    )
    parser.add_argument(
        "-p", "--package", help="Bazel package", default="//:gen_ray_pkg"
    )
    parser.add_argument("--log-file", help="Log file path")
    parser.add_argument(
        "--copy-files-for-fossa",
        action="store_true",
        help="Copy files for fossa analysis on top of askalono",
    )

    parser.formatter_class = argparse.RawTextHelpFormatter
    parser.description = """
Ray OSS Analysis Tool - Analyze Ray's open source components
current status: scans only c, cpp libraries are scanned and scanned via askalono
    """
    parser.epilog = """
Examples:
    %(prog)s --output oss_analysis -cmd bazelisk # if bazel is not present or you would prefer bazelisk
    %(prog)s --output oss_analysis # if bazel is present
    %(prog)s --verbose
    """

    args = parser.parse_args()

    _change_working_directory()

    _setup_logger(args.log_file, args.verbose)
    bazel_output_base = subprocess.check_output(
        [args.bazel_cmd, "info", "output_base"], text=True
    ).strip()
    package_names, file_paths = _get_bazel_dependencies(args.package, args.bazel_cmd)

    logger.info(f"Found {len(file_paths)} file paths")
    logger.info(f"Found {len(package_names)} package names")

    if args.copy_files_for_fossa:
        _copy_files(file_paths, args.output)
        _copy_licenses(package_names, bazel_output_base, args.output)

    askalono_results = _get_askalono_results(package_names, bazel_output_base)
    with open(os.path.join(args.output, "askalono_results.json"), "w") as file:
        json.dump(askalono_results, file, indent=4)
    _generate_fossa_deps_file(askalono_results, args.output)
