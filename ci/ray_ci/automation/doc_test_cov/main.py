import os
import argparse
from s3 import S3DataSource
from query import Query
from buildkite import BuildKiteClient
from test_results import TestResults

S3_BUCKET = "ray-travis-logs"
BK_ORGANIZATION = "ray-project"
BK_PIPELINE = "postmerge"
BK_HOST = "https://api.buildkite.com/v2"

def cleanup():
    os.system("rm -f results/*")
    os.system("rm -f bazel_events/*")

def main():
    cleanup()
    parser = argparse.ArgumentParser(description="Analyze Bazel test execution")
    parser.add_argument("--ray-path", help="path to ray repo")
    parser.add_argument("--bk-api-token", help="buildkite api token")
    parser.add_argument("--look-back-hours", default=6, type=int, help="look back hours for nightly builds (default: 6)")
    parser.add_argument("--offline", action="store_true", help="run offline mode")
    args = parser.parse_args()

    test_results = TestResults()
    # Get all doctest jobs for the run
    bk_client = BuildKiteClient(BK_HOST, args.bk_api_token, BK_ORGANIZATION, BK_PIPELINE)
    print("init bk client")
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
    print(args.ray_path)
    test_results.set_targets(Query.get_all_test_targets(args.ray_path))

    # # List and Get all bazel events from s3
    s3_source = S3DataSource(S3_BUCKET, commit, job_ids_to_names.keys())
    s3_source.list_all_bazel_events()
    log_files = s3_source.download_all_bazel_events(f"{os.getcwd()}/bazel_events")

    # Parse log files for executed tests and store results in test_result
    Query.parse_bazel_results_eff(log_files)
    Query.read_test_results_from_file(test_results)
    print(f"Found {len(test_results.targets)} targets")

    # get bazel file location for bazel targets
    Query.get_bazel_file_location_for_targets(test_results, args.ray_path)
    print("done getting bazel file location for targets")

    # get all source files for bazel targets
    Query.get_files_for_targets(test_results, args.ray_path)
    print("done getting files for targets")

    # get all file refs for bazel target files
    Query.get_file_references_for_active_targets(test_results, args.ray_path)
    print("done getting file refs for targets")

    #filter out targets that don't have a generated file in doc/_build
    Query.filter_out_targets_without_doc_builds(test_results)
    print("done filtering out targets that don't have a generated file in doc/_build")
    test_results.calculate_test_coverage()
    print("done calculating test coverage")
    print(f"number of targets: {len(test_results.targets)}")
    print(f"number of tested files for first target: {len(test_results.targets[0].files)}")
    # print(f"number of file refs for first target: {len(test_results.targets[0].files[0].file_refs)}")
    for target in test_results.targets:
        if not target.tested:
            print(f"target: {target.target_name}")
            print(f"target files: {target.files}")
            print(f"target file refs: {target.files[0].file_refs}")
    test_results.save_test_results()
    print("done saving test results")

if __name__ == "__main__":
    main()
