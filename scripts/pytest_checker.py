import json
import re
import sys
from pathlib import Path


def check_file(file_contents: str) -> bool:
    return bool(re.search(r"^if __name__ == \"__main__\":", file_contents, re.M))


def parse_json(data: str) -> dict:
    return json.loads(data)


def treat_path(path: str) -> Path:
    path = path[2:].replace(":", "/")
    return Path(path)


def get_paths_from_parsed_data(parsed_data: dict) -> list:
    paths = []
    for rule in parsed_data["query"]["rule"]:
        name = rule["@name"]
        if "label" in rule and rule["label"]["@name"] == "main":
            paths.append((name, treat_path(rule["label"]["@value"])))
        else:
            list_args = {e["@name"]: e for e in rule["list"]}
            label = list_args["srcs"]["label"]
            if isinstance(label, dict):
                paths.append((name, treat_path(label["@value"])))
            else:
                # list
                string_name = next(
                    x["@value"] for x in rule["string"] if x["@name"] == "name"
                )
                main_path = next(
                    x["@value"] for x in label if string_name in x["@value"]
                )
                paths.append((name, treat_path(main_path)))
    return paths


def main(data: str):
    print("Checking files for the pytest snippet...")
    parsed_data = parse_json(data)
    paths = get_paths_from_parsed_data(parsed_data)

    bad_paths = []
    for name, path in paths:
        # Special case for myst doc checker
        if "test_myst_doc" in str(path):
            continue

        print(f"Checking test '{name}' | file '{path}'...")
        try:
            with open(path, "r") as f:
                if not check_file(f.read()):
                    print(f"File '{path}' is missing the pytest snippet.")
                    bad_paths.append(path)
        except FileNotFoundError:
            print(f"File '{path}' is missing.")
            bad_paths.append((path, "path is missing!"))
    if bad_paths:
        formatted_bad_paths = "\n".join([str(x) for x in bad_paths])
        raise RuntimeError(
            'Found py_test files without `if __name__ == "__main__":` snippet:'
            f"\n{formatted_bad_paths}\n"
            "If this is intentional, please add a `no_main` tag to bazel BUILD "
            "entry for those files."
        )


if __name__ == "__main__":
    # Expects a json
    # Invocation from workspace root:
    # bazel query 'kind(py_test.*, tests(python/...) intersect
    # attr(tags, "\bteam:ml\b", python/...) except attr(tags, "\bno_main\b",
    # python/...))' --output xml | xq | python scripts/pytest_checker.py
    data = sys.stdin.read()
    main(data)
