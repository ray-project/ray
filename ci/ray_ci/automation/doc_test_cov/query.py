import subprocess
import json
from typing import Dict, List

class BazelQuery:

    @staticmethod
    def get_all_test_targets(ray_path: str) -> List[str]: # update this to be a SET
        """
        Get all test targets in the workspace using bazel query.
        """
        result = subprocess.run(
            ["bazel", "query", "\"//doc/...\""],
            cwd=ray_path,
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.strip().split("\n")

    @staticmethod
    def get_files_for_targets(targets: List[str], ray_path: str) -> List[str]:
        """
        Get all .rst,.md,.ipynb files for the given targets.
        """
        files_for_targets = {}
        for target in targets:
            try:
                cmd = f"bazel query 'filter(\"\\.rst$|\\.md$|\\.ipynb$|\\.py$\", deps({target}, 1))'"
                result = subprocess.run(
                    cmd,
                cwd=ray_path,
                capture_output=True,
                text=True,
                shell=True
                )
                files_for_targets[target] = (result.stdout.strip().split("\n"))
            except subprocess.CalledProcessError as e:
                print(f"Error: {e}")
                continue

        return files_for_targets

    @staticmethod
    def parse_bazel_json(log_files: str, targets: List[str]) -> Dict[str, str]:
        """
        Parse bazel test log file to find executed tests and their status.
        Returns: Dict[target_name, status]
        """
        executed_tests = {}
        print("Looking for these targets:")
        for target in targets:
            print(f"  {target}")

        for log_file in log_files:
            if "metadata" not in log_file:
                print(f"\nParsing log file: {log_file}")
                print(f"Found {len(executed_tests)} executed tests")
                with open(log_file, "r") as f:
                    # Convert the file content into a JSON array
                    content = f.read()
                    json_array = "[" + content.replace("}\n{", "},{") + "]"
                    try:
                        data_array = json.loads(json_array)
                        print(f"\nSuccessfully parsed {len(data_array)} JSON objects")

                        for data in data_array:
                            if "testSummary" in data and "testSummary" in data["id"]:
                                summary = data["testSummary"]
                                label_summary = data["id"]["testSummary"]
                                if "label" in label_summary and "overallStatus" in summary:
                                    target = label_summary["label"]
                                    status = summary["overallStatus"]
                                    print(f"Found test result: {target} = {status}")
                                    executed_tests[target] = status
                    except json.JSONDecodeError as e:
                        print(f"Failed to parse JSON array: {str(e)}")
                        return {}

        print(f"\nFound {len(executed_tests)} total test results:")
        for target, status in executed_tests.items():
            print(f"  {target} = {status}")

        print("\nFiltering for matching targets...")
        filtered_tests = {}

        for label, status in executed_tests.items():
            print(f" label: {label}")
            if label in targets:
                filtered_tests[label] = status
        print(f"\nAfter filtering, found {len(filtered_tests)} matching target tests:")
        return filtered_tests, executed_tests

    @staticmethod
    def output_test_coverage(filtered_tests: Dict[str, str], executed_tests: Dict[str, str], targets: List[str], target_file_map: Dict[str, List[str]], bk_build_url: str):
        """
        Get test coverage for the executed tests that match the targets.
        """
        file_list = {}
        # Write results to a file
        with open("results/test_results.txt", "w") as f:
            f.write("Test Results Summary\n")
            f.write("===================\n\n")
            f.write(f"BK Build URL: {bk_build_url}\n\n")
            f.write("--------------------\n")
            f.write("\nTest Coverage Summary:\n")
            f.write("--------------------\n")
            f.write(f"ALL TARGETS: {len(target_file_map)}\n")
            for target in targets:
                f.write(f"{target}\n")
            f.write("--------------------\n")
            f.write("\nTESTED TARGETS:\n")
            untested_targets = []
            for target in targets:
                if target in filtered_tests:
                    f.write(f"{target}: TESTED : {filtered_tests[target]}\n")
                    for file in target_file_map[target]:
                        file_list[file] = "TESTED"
                        f.write(f"      {file}\n")
                else:
                    untested_targets.append(target)
            f.write("\nUNTESTED TARGETS:\n")
            f.write("--------------------\n")
            for target in untested_targets:
                f.write(f"{target} : NOT TESTED\n")
                for file in target_file_map[target]:
                    file_name = file.split(".")[0]
                    if file_name not in executed_tests:
                        file_list[file] = "NOT TESTED"
                        f.write(f"      {file}\n")
            f.write("--------------------\n")
            f.write(f"Total Bazel targets: {len(targets)}\n")
            f.write(f"Tested Bazel Targets: {len(filtered_tests)}\n")
            f.write(f"Untested Bazel Targets: {len(untested_targets)}\n")
            f.write(f"Test coverage per target: {len(filtered_tests) / len(targets) * 100:.2f}%\n")

            not_tested_count = sum(1 for status in file_list.values() if status == "NOT TESTED")
            f.write(f"Test coverage per file: {(len(file_list.keys()) - not_tested_count) / len(file_list.keys()) * 100:.2f}%\n")
        with open("results/file_list.txt", "w") as f:
            for key in file_list.keys():
                f.write(f"{key} : {file_list[key]}\n")
        print("\nResults have been written to test_results.txt")
