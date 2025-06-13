import os
import argparse
from s3 import S3DataSource
from query import Query
from buildkite import BuildKiteClient
from test_results import TestResults
from parser import DocParser
import json
from test_results import DocFile
from typing import List

S3_BUCKET = "ray-travis-logs"
BK_ORGANIZATION = "ray-project"
BK_PIPELINE = "postmerge"
BK_HOST = "https://api.buildkite.com/v2"

def cleanup():
    os.system("rm -f results/*")
    os.system("rm -f bazel_events/*")

def main():
    # cleanup()
    parser = argparse.ArgumentParser(description="Analyze Bazel test execution")
    parser.add_argument("--ray-path", help="path to ray repo")
    parser.add_argument("--bk-api-token", help="buildkite api token")
    parser.add_argument("--look-back-hours", default=6, type=int, help="look back hours for nightly builds (default: 6)")
    parser.add_argument("--offline", action="store_true", help="run offline mode")
    args = parser.parse_args()

    doc_parser = DocParser(args.ray_path)
    # literal include snippets
    literalinclude_doc_files = doc_parser.find_code_snippets(search_string_rst=".. literalinclude::", search_string_md="```{literalinclude}")
    doc_parser.save_doc_file_snippets(literalinclude_doc_files, "results/literal_include_doc_files.json")
    print(f"Found {len(literalinclude_doc_files)} literal include snippets")

    # test code snippets
    testcode_doc_files = doc_parser.find_code_snippets(search_string_rst=".. testcode::", search_string_md="```{testcode}")
    doc_parser.save_doc_file_snippets(testcode_doc_files, "results/test_code_doc_files.json")
    print(f"Found {len(testcode_doc_files)} test code snippets")

    # code block snippets
    codeblock_doc_files = doc_parser.find_code_snippets(search_string_rst=".. code-block::", search_string_md="")
    doc_parser.save_doc_file_snippets(codeblock_doc_files, "results/code_block_doc_files.json")
    print(f"Found {len(codeblock_doc_files)} code block snippets")

    python_doc_files = doc_parser.find_code_snippets(search_string_rst="", search_string_md="```python")
    doc_parser.save_doc_file_snippets(python_doc_files, "results/python_doc_files.json")
    print(f"Found {len(python_doc_files)} python snippets")

    doctest_doc_files = doc_parser.find_code_snippets(search_string_rst=".. doctest::", search_string_md="")
    doc_parser.save_doc_file_snippets(doctest_doc_files, "results/doctest_doc_files.json")
    print(f"Found {len(doctest_doc_files)} doctest snippets")

    java_doc_files = doc_parser.find_code_snippets(search_string_rst="", search_string_md="```java")
    doc_parser.save_doc_file_snippets(java_doc_files, "results/java_doc_files.json")
    print(f"Found {len(java_doc_files)} java snippets")

    shell_doc_files = doc_parser.find_code_snippets(search_string_rst="", search_string_md="```shell")
    doc_parser.save_doc_file_snippets(shell_doc_files, "results/shell_doc_files.json")
    print(f"Found {len(shell_doc_files)} shell snippets")

    sh_doc_files = doc_parser.find_code_snippets(search_string_rst="", search_string_md="```sh", strict=True)
    doc_parser.save_doc_file_snippets(sh_doc_files, "results/sh_doc_files.json")
    print(f"Found {len(sh_doc_files)} sh snippets")

    bash_doc_files = doc_parser.find_code_snippets(search_string_rst="", search_string_md="```bash")
    doc_parser.save_doc_file_snippets(bash_doc_files, "results/bash_doc_files.json")
    print(f"Found {len(bash_doc_files)} bash snippets")

    test_results = TestResults()
    # Get all doctest jobs for the run
    bk_client = BuildKiteClient(BK_HOST, args.bk_api_token, BK_ORGANIZATION, BK_PIPELINE)
    print("Init Buildkite client")
    # list all recent runs for postmerge ran by release-automation
    bk_jobs = bk_client.get_nightly_bk_builds_for_pipeline(args.look_back_hours)
    print(f"Found {len(bk_jobs)} builds for the last {args.look_back_hours} hours")
    # get release-automation bk builds
    release_automation_bk_builds = bk_client.get_release_automation_bk_builds(bk_jobs)
    if len(release_automation_bk_builds) == 0:
        print("No release-automation bk builds found")
        return
    print(f"Found {len(release_automation_bk_builds)} release-automation bk jobs")

    # prompt user for build number
    for build in release_automation_bk_builds:
        print(f"Build {build["number"]}: {build["web_url"]}")
    user_selected_build_no = int(input("Which build do you want to run test coverage for (build number ex: 1234): "))

    user_selected_build = next(
        (build for build in release_automation_bk_builds if build["number"] == user_selected_build_no),
        None
    )

    if user_selected_build is None:
        raise ValueError(f"Build number {user_selected_build_no} not found")

    commit = user_selected_build["commit"]

    # get doc test bk job ids
    job_ids_to_names = bk_client.get_doc_test_jobs_for_build(user_selected_build)
    print(f"Found {len(job_ids_to_names)} doc test bk jobs")

    test_results.set_buildkite_metadata(user_selected_build["web_url"], job_ids_to_names, commit)

    # Get all available test targets
    print("Querying available test targets...")
    #Query targets in //doc/..
    test_results.set_targets(Query.get_all_test_targets(args.ray_path, "//doc/..."))

    #Query targets in //java/...
    test_results.set_targets(Query.get_all_test_targets(args.ray_path, "//java/..."))

    #Query targets in //python/...
    test_results.set_targets(Query.get_all_test_targets(args.ray_path, "//python/..."))

    # # List and Get all bazel events from s3
    s3_source = S3DataSource(S3_BUCKET, commit, job_ids_to_names.keys())
    s3_source.list_all_bazel_events()
    log_files = s3_source.download_all_bazel_events(f"{os.getcwd()}/bazel_events")

    # Parse log files for executed tests and store results in test_result
    Query.parse_bazel_results(log_files)
    Query.read_test_results_from_file(test_results)
    print(f"Found {len(test_results.targets)} targets")

    # get bazel file location for bazel targets
    Query.get_bazel_file_location_for_targets(test_results, args.ray_path)
    print("done getting bazel file location for targets")

    # get all source files for bazel targets
    Query.get_files_for_targets(test_results, args.ray_path)
    print("done getting files for targets")

    # #filter out targets that don't have a generated file in doc/_build (don't need)
    # Query.filter_out_targets_without_doc_builds(test_results)
    # print("done filtering out targets that don't have a generated file in doc/_build")
    # print(f"len(test_results.targets): {len(test_results.targets)}")

    test_results.calculate_test_coverage()
    print("done calculating test coverage")

    print(f"len(test_results.targets): {len(test_results.targets)}")
    test_results.save_test_results()
    print("done saving test results")

    doc_parser.assign_testing_info_to_code_snippets(literalinclude_doc_files, test_results.targets)
    print("done assigning testing info to code snippets")

    if args.offline:
        test_file = []
        with open("results/literal_include_doc_files_with_testing_info.json", "r") as f:
            content = f.read()
            test_file = json.loads(content)
            doc_files = []
            # doc_parser.save_doc_file_snippets(literalinclude_doc_files, "results/literal_include_doc_files_with_testing_info.json")
            # print("done saving doc file snippets with testing info")
            for file in test_file:
                doc_file = DocFile.from_dict(DocFile,file)
                doc_files.append(doc_file)

    doc_parser.save_doc_files_to_csv(literalinclude_doc_files)
    print("done saving doc files to csv")


if __name__ == "__main__":
    main()
