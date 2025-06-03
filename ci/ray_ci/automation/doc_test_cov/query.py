import subprocess
import json
from typing import Dict, List
from test_results import TestResults
import gc
class Query:

    @staticmethod
    def get_all_test_targets(ray_path: str) -> List[str]: # update this to be a SET
        """
        Get all test targets in the workspace using bazel query.
        """
        cmd = "bazel query 'kind(\".*_test rule\", //doc/...)'"
        result = subprocess.run(
            cmd,
            cwd=ray_path,
            capture_output=True,
            text=True,
            shell=True
        )
        return result.stdout.strip().split("\n")

    @staticmethod
    def get_files_for_targets(test_results: TestResults, ray_path: str):
        """
        Get all source files for the given targets.
        """

        for target in test_results.targets:
            try:
                #cmd = f"bazel query 'filter(\"\\.rst$|\\.md$|\\.ipynb$|\\.py$\", deps({target}, 1))'"
                cmd = f"bazel query 'kind(\"source file\", filter(\"source/.*\", deps({target})))'"
                result = subprocess.run(
                    cmd,
                    cwd=ray_path,
                    capture_output=True,
                    text=True,
                    shell=True
                )
                target.set_files(result.stdout.strip().split("\n"))
            except subprocess.CalledProcessError as e:
                print(f"Error: {e}")

    @staticmethod
    def get_bazel_file_location_for_targets(test_results: TestResults, ray_path: str):
        """
        Get all bazel file locations for the given targets.
        """
        for target in test_results.targets:
            try:
                cmd = f"bazel query '{target.target_name}' --output=location"
                result = subprocess.run(
                    cmd,
                    cwd=ray_path,
                    capture_output=True,
                    text=True,
                    shell=True
                )
                target.bazel_file_location = (result.stdout.strip().split("\n"))
            except subprocess.CalledProcessError as e:
                print(f"Error: {e}")
                continue

    @staticmethod
    def get_bazel_file_location_for_targets_mc(test_results: TestResults, ray_path: str):
        """
        Get all bazel file locations for the given targets with memory optimizations.
        """
        # Add --heap_size limit to bazel query
        BAZEL_HEAP_SIZE = "2048m"  # Limit bazel's heap to 2GB

        for target in test_results.targets:
            try:
                # Add memory limiting flags and make query more specific
                cmd = (
                    f"bazel --host_jvm_args=-Xmx{BAZEL_HEAP_SIZE} "
                    f"query --heap_size={BAZEL_HEAP_SIZE} "
                    f"--loading_phase_threads=1 "
                    f"--keep_going "
                    f"'{target.target_name}' --output=location"
                )

                # Use Popen for better memory control
                process = subprocess.Popen(
                    cmd,
                    cwd=ray_path,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    shell=True,
                    text=True,
                    bufsize=1
                )

                # Read output line by line instead of all at once
                output = []
                for line in process.stdout:
                    output.append(line.strip())

                process.stdout.close()
                process.wait()

                if output:
                    target.bazel_file_location = output

                # Clear memory immediately
                del output

            except subprocess.CalledProcessError as e:
                print(f"Error querying target {target.target_name}: {e}")
                continue
            except Exception as e:
                print(f"Unexpected error for target {target.target_name}: {e}")
                continue

            # Force garbage collection periodically
            if hasattr(target, "target_name") and target.target_name.endswith("0"):
                gc.collect()

    # @staticmethod
    # def parse_bazel_results(log_files: str, test_result: TestResults):
    #     """
    #     Parse bazel test log file to find executed tests and their status.
    #     """
    #     executed_tests = {}
    #     for log_file in log_files:
    #         if "metadata" not in log_file:
    #             print(f"\nParsing log file: {log_file}")
    #             print(f"Found {len(executed_tests)} executed tests")
    #             with open(log_file, "r") as f:
    #                 # Convert the file content into a JSON array
    #                 content = f.read()
    #                 json_array = "[" + content.replace("}\n{", "},{") + "]"
    #                 try:
    #                     data_array = json.loads(json_array)
    #                     print(f"\nSuccessfully parsed {len(data_array)} JSON objects")

    #                     for data in data_array:
    #                         if "testSummary" in data and "testSummary" in data["id"]:
    #                             summary = data["testSummary"]
    #                             label_summary = data["id"]["testSummary"]
    #                             if "label" in label_summary and "overallStatus" in summary:
    #                                 target = label_summary["label"]
    #                                 status = summary["overallStatus"]
    #                                 executed_tests[target] = status
    #                 except json.JSONDecodeError as e:
    #                     print(f"Failed to parse JSON array: {str(e)}")
    #                     return {}
    #                 with open("bazel_events/bazel_events.json", "w") as f:
    #                     f.write(json.dumps(executed_tests))
    #                 executed_tests.clear()


    #     print(f"\nFound {len(executed_tests)} total test results:")

    @staticmethod
    def read_test_results_from_file(test_result: TestResults):
        with open("bazel_events/bazel_events.json", "r") as f:
            content = f.read()
            data = json.loads(content)
            for target in test_result.targets:
                if target.target_name in data.keys():
                    target.status = data[target.target_name]
                    target.tested = True
                else:
                    target.status = "NOT TESTED"
                    target.tested = False

    @staticmethod
    def write_to_file(executed_tests: dict):
        """Helper function to write tests to file"""
        with open("bazel_events/bazel_events.json", "a") as f:
            json.dump(executed_tests, f, indent=4)

    @staticmethod
    def parse_bazel_results_eff(log_files: str):
        """
        Parse bazel test log files and create a single JSON object containing all results.
        """
        all_executed_tests = {}
        total_files = len(log_files)

        print(f"\nProcessing {total_files} log files...")

        for i, log_file in enumerate(log_files, 1):
            if "metadata" in log_file:
                continue

            print(f"\nParsing file {i}/{total_files}: {log_file}")

            try:
                with open(log_file, "r") as f:
                    content = f.read()
                    json_array = "[" + content.replace("}\n{", "},{") + "]"
                    data_array = json.loads(json_array)
                    print(f"Successfully parsed {len(data_array)} JSON objects")

                    for data in data_array:
                        if "testSummary" in data and "testSummary" in data["id"]:
                            summary = data["testSummary"]
                            label_summary = data["id"]["testSummary"]
                            if "label" in label_summary and "overallStatus" in summary:
                                target = label_summary["label"]
                                status = summary["overallStatus"]
                                all_executed_tests[target] = status

            except json.JSONDecodeError as e:
                print(f"Warning: Failed to parse JSON in {log_file}: {str(e)}")
                continue
            except Exception as e:
                print(f"Warning: Error processing {log_file}: {str(e)}")
                continue

        # Write final results to a single JSON file
        if all_executed_tests:
            try:
                with open("bazel_events/bazel_events.json", "w") as f:
                    json.dump(all_executed_tests, f, indent=4)
                print(f"\nSuccessfully wrote {len(all_executed_tests)} test results to file")
            except Exception as e:
                print(f"\nError writing results to file: {str(e)}")
                return {}

        all_executed_tests.clear()

    @staticmethod
    def get_file_refs_for_targets_old(test_results: TestResults, ray_path: str):
        """
        Get file references in the doc/ directory.
        """
        for target in test_results.targets:
            for file in target.files:
                print(f"processing {len(target.files)} files for target: {target.target_name}")
                file_name = file.file_name.lstrip("//").split(":")[-1].split("/")[-1]
                # Use find to get all matching files and show matching lines with filename
                cmd = f"find {ray_path}/doc -type f -name '*.rst' -o -name '*.md' -o -name '*.html' -o -name '*.txt' | xargs grep -H '{file_name}'"
                result = subprocess.run(["bash", "-c", cmd],
                    cwd=ray_path,
                    capture_output=True,
                    text=True)

                stdout = result.stdout.strip()
                file.file_refs = stdout.split("\n") if stdout else []

    @staticmethod
    def filter_out_targets_without_doc_builds(test_results: TestResults) -> List[str]:
        """
        Filter out targets that don't have a generated file in doc/_build.
        """
        for target in test_results.targets:
            for file in target.files:
                paths = file.file_refs
                if any("doc/_build" in path for path in paths):
                    target.active = True
                    break

    @staticmethod
    def output_test_coverage(filtered_tests: Dict[str, str], executed_tests: Dict[str, str], targets: List[str], target_file_map: Dict[str, List[str]], bk_build_url: str, bk_job_names: List[str], bazel_file_locations_for_targets: Dict[str, str], ray_path: str):
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
            f.write(f"BK Job Names: {bk_job_names}\n\n")
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
                        f.write(f"  {file}\n")
                else:
                    untested_targets.append(target)
            f.write("\nUNTESTED TARGETS:\n")
            f.write("--------------------\n")
            filtered_target_file_map = {k: v for k, v in target_file_map.items() if k in untested_targets}
            file_ref_map = Query.get_file_references(filtered_target_file_map, ray_path)
            target_to_file_map = Query.filter_out_targets_without_doc_builds(untested_targets, filtered_target_file_map, file_ref_map)
            for target in untested_targets:
                if target in target_to_file_map:
                    f.write(f"{target} : NOT TESTED\n")
                    f.write(f"bazel file location: {bazel_file_locations_for_targets[target]}\n")
                    for file in filtered_target_file_map[target]:
                        file_name = file.split(".")[0]
                        if file_name not in executed_tests:
                            file_list[file] = "NOT TESTED"
                            f.write(f"      {file}\n")
                            for ref in file_ref_map[file]:
                                f.write(f"              {ref}\n")
            f.write("--------------------\n")
            f.write(f"Total Bazel targets: {len(targets)}\n")
            f.write(f"Tested Bazel Targets: {len(filtered_tests)}\n")
            f.write(f"Untested Bazel Targets: {len(filtered_target_file_map.keys())}\n")

            f.write(f"Test coverage per target: {len(filtered_tests) / len(targets) * 100:.2f}%\n")
            not_tested_count = sum(1 for status in file_list.values() if status == "NOT TESTED")
            f.write(f"Total files: {len(file_list.keys())}\n")
            f.write(f"Tested files: {len(file_list.keys()) - not_tested_count}\n")
            f.write(f"Test coverage per file: {(len(file_list.keys()) - not_tested_count) / len(file_list.keys()) * 100:.2f}%\n")
        print("\nResults have been written to test_results.txt")

    @staticmethod
    def get_file_references_for_active_targets(test_results: TestResults, ray_path: str):
        """
        Get file references in the doc/ directory.
        """
        for target in test_results.targets:
            if not target.tested:
                for file in target.files:
                    file_path = file.file_name.lstrip("//").split(":")[-1].split("/")[-1]    # Use find to get all matching files and show matching lines with filename
                    cmd = f"find {ray_path}/doc -type f -name '*.rst' -o -name '*.md' -o -name '*.html' -o -name '*.txt' | xargs grep -H '{file_path}'"
                    result = subprocess.run(["bash", "-c", cmd],
                        cwd=ray_path,
                        capture_output=True,
                        text=True)
                    stdout = result.stdout.strip()
                    file.file_refs = stdout.split("\n") if stdout else []

    @staticmethod
    def get_file_refs_for_targets(test_results: TestResults, ray_path: str):
        """
        Get file references with batch processing for very large codebases.
        """
        BATCH_SIZE = 50  # Adjust based on your needs

        def process_batch(file_names):
            patterns = "|".join(file_names)
            cmd = (
                f"find {ray_path}/doc "
                f"-type f \\( -name '*.rst' -o -name '*.md' -o -name '*.html' -o -name '*.txt' \\) "
                f"-exec grep -H '{patterns}' {{}} \\;"
            )

            refs_map = {name: [] for name in file_names}

            try:
                process = subprocess.Popen(
                    ["bash", "-c", cmd],
                    cwd=ray_path,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    bufsize=1
                )

                while True:
                    line = process.stdout.readline()
                    if not line and process.poll() is not None:
                        break
                    if line:
                        line = line.strip()
                        for file_name in file_names:
                            if file_name in line:
                                refs_map[file_name].append(line)

                process.stdout.close()
                process.wait()

            except Exception as e:
                print(f"Error processing batch: {e}")

            return refs_map

        # Collect unique file names
        search_files = set()
        for target in test_results.targets:
            for file in target.files:
                file_name = file.file_name.lstrip("//").split(":")[-1].split("/")[-1]
                search_files.add(file_name)

        # Process in batches
        search_files = list(search_files)
        all_refs = {}

        for i in range(0, len(search_files), BATCH_SIZE):
            batch = search_files[i:i + BATCH_SIZE]
            print(f"Processing batch {i//BATCH_SIZE + 1}/{(len(search_files) + BATCH_SIZE - 1)//BATCH_SIZE}")

            batch_refs = process_batch(batch)
            all_refs.update(batch_refs)

            # Force cleanup
            del batch_refs
            gc.collect()

        # Assign references back to files
        for target in test_results.targets:
            for file in target.files:
                file_name = file.file_name.lstrip("//").split(":")[-1].split("/")[-1]
                file.file_refs = all_refs.get(file_name, [])