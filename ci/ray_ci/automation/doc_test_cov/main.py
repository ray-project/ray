import os
import argparse
from s3 import S3DataSource
from query import Query
from buildkite import BuildKiteClient

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
    doc_test_bk_job_ids,doc_test_bk_job_names = bk_client.get_doc_test_jobs_for_build(user_selected_build)
    print(f"Found {doc_test_bk_job_ids} doc test bk jobs")

    # Get all available test targets
    print("Querying available test targets...")
    print(args.ray_path)
    all_targets = Query.get_all_test_targets(args.ray_path)
    print(f"Found {len(all_targets)} total test targets")

    # get all rst,md,ipynb files for target
    files_for_targets = Query.get_files_for_targets(all_targets, args.ray_path)
    bazel_file_locations_for_targets = Query.get_bazel_file_location_for_targets(all_targets, args.ray_path)

    # # List and Get all bazel events from s3
    s3_source = S3DataSource(S3_BUCKET, commit, doc_test_bk_job_ids)
    s3_source.list_all_bazel_events()
    log_files = s3_source.download_all_bazel_events(f"{os.getcwd()}/bazel_events")

    # Parse log file for executed tests
    filtered_tests, executed_tests = Query.parse_bazel_json(log_files, all_targets)
    print(f"Found {len(executed_tests)} executed tests")
    print(f"Found {len(filtered_tests)} filtered tests")
    print(f"Found {len(all_targets)} total targets")
    Query.output_test_coverage(filtered_tests, executed_tests, all_targets, files_for_targets, user_selected_build["web_url"], doc_test_bk_job_names, bazel_file_locations_for_targets, args.ray_path)

if __name__ == "__main__":
    main()
