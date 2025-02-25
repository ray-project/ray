import os
import requests
import zipfile
import io
import time
import argparse
from datetime import datetime

# GitHub API configuration
GITHUB_API_URL = "https://api.github.com"
REPO_OWNER = "antgroup"
REPO_NAME = "ant-ray"
WORKFLOW_NAME = "ray-ci.yml"
ARTIFACTS_DIR = "artifacts"

# Default values
DEFAULT_NUM_RUNS = 20
MAIN_BRANCH_ONLY = (
    True  # Set to True to only download artifacts from commits in main branch
)
DEFAULT_MAX_RUNS_TO_CHECK = 50  # Default maximum number of workflow runs to check

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")

headers = {}
if GITHUB_TOKEN:
    headers["Authorization"] = f"token {GITHUB_TOKEN}"
else:
    print("Warning: No GitHub token found. API rate limits may apply.")


def get_workflow_id(workflow_name):
    """Get the workflow ID for the specified workflow name."""
    url = f"{GITHUB_API_URL}/repos/{REPO_OWNER}/{REPO_NAME}/actions/workflows"
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    workflows = response.json()["workflows"]
    for workflow in workflows:
        if workflow["name"] == workflow_name or workflow["path"].endswith(
            workflow_name
        ):
            return workflow["id"]

    raise ValueError(f"Workflow '{workflow_name}' not found")


def get_main_branch_commits():
    """Get all commits in the main branch."""
    print("Fetching commit history for main branch...")
    main_history_url = f"{GITHUB_API_URL}/repos/{REPO_OWNER}/{REPO_NAME}/commits"
    params = {"sha": "main", "per_page": 100}  # Get up to 100 commits
    history_response = requests.get(main_history_url, headers=headers, params=params)
    history_response.raise_for_status()

    commits = history_response.json()
    # Store SHA values for direct matching
    commit_shas = {commit["sha"] for commit in commits}
    print(f"Fetched {len(commit_shas)} commits from main branch")
    return commit_shas


def get_associated_pr(commit_sha):
    """Get the PR associated with a commit."""
    url = f"{GITHUB_API_URL}/repos/{REPO_OWNER}/{REPO_NAME}/commits/{commit_sha}/pulls"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        prs = response.json()
        if prs:
            return prs[0]  # Return the first associated PR
    return None


def is_pr_merged_to_main(pr):
    """Check if a PR was merged to the main branch."""
    if not pr or pr["merged_at"] is None:
        return False

    # Check if the PR was merged to main
    if pr["base"]["ref"] == "main":
        return True

    return False


def is_commit_in_main(commit_sha, main_branch_commits):
    """
    Check if a commit is in the main branch.
    First checks direct SHA match, then looks for associated PR.
    """
    # Direct SHA match (fastest check)
    if commit_sha in main_branch_commits:
        print(f"  - Direct SHA match for {commit_sha[:8]}")
        return True

    # Check if this commit is associated with a PR merged to main
    pr = get_associated_pr(commit_sha)
    if pr and is_pr_merged_to_main(pr):
        print(f"  - Commit {commit_sha[:8]} is from PR #{pr['number']} merged to main")
        return True

    return False


def get_recent_workflow_runs(
    workflow_id,
    num_runs=20,
    main_branch_only=False,
    max_runs_to_check=DEFAULT_MAX_RUNS_TO_CHECK,
):
    """Get the recent workflow runs for the specified workflow ID."""
    url = f"{GITHUB_API_URL}/repos/{REPO_OWNER}/{REPO_NAME}/actions/workflows/{workflow_id}/runs"

    # If we need to filter by main branch, get the commit history once
    main_branch_commits = None
    if main_branch_only:
        main_branch_commits = get_main_branch_commits()

    filtered_runs = []
    page = 1
    per_page = 100  # Fetch more runs per request to minimize API calls
    total_runs_checked = 0

    while len(filtered_runs) < num_runs and total_runs_checked < max_runs_to_check:
        params = {"per_page": per_page, "page": page}

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()

        runs = response.json()["workflow_runs"]

        # If no more runs are available, break the loop
        if not runs:
            break

        for run in runs:
            total_runs_checked += 1

            # Skip runs that are not completed
            if run["status"] != "completed":
                continue

            # Check if this commit is in main branch if required
            if main_branch_only:
                print(f"Checking if commit {run['head_sha'][:8]} is in main branch...")
                if not is_commit_in_main(run["head_sha"], main_branch_commits):
                    print(f"  - Not in main branch, skipping")
                    continue
                else:
                    print(f"  - Commit is in main branch")

            filtered_runs.append(run)
            if len(filtered_runs) >= num_runs:
                break

            if total_runs_checked >= max_runs_to_check:
                break

        page += 1
        # Add a small delay to avoid hitting rate limits
        time.sleep(0.5)

    if len(filtered_runs) < num_runs:
        print(
            f"Warning: Only found {len(filtered_runs)} matching runs after checking {total_runs_checked} workflow runs"
        )

    return filtered_runs[:num_runs]


def get_run_artifacts(run_id):
    """Get all artifacts for a specific workflow run."""
    url = f"{GITHUB_API_URL}/repos/{REPO_OWNER}/{REPO_NAME}/actions/runs/{run_id}/artifacts"
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    return response.json()["artifacts"]


def download_artifact(artifact, run_id, run_date, commit_sha):
    """Download and extract an artifact."""
    artifact_name = artifact["name"]
    artifact_id = artifact["id"]

    # Create a folder structure based on run date, ID and commit SHA
    date_str = run_date.strftime("%Y-%m-%d")
    run_folder = os.path.join(
        ARTIFACTS_DIR, f"{date_str}_run_{run_id}_{commit_sha[:8]}"
    )
    os.makedirs(run_folder, exist_ok=True)

    # Download the artifact
    download_url = f"{GITHUB_API_URL}/repos/{REPO_OWNER}/{REPO_NAME}/actions/artifacts/{artifact_id}/zip"
    print(
        f"Downloading artifact: {artifact_name} (ID: {artifact_id}, Commit: {commit_sha[:8]})"
    )

    response = requests.get(download_url, headers=headers, stream=True)

    if response.status_code == 200:
        # Extract the zip file
        artifact_folder = os.path.join(run_folder, artifact_name)
        os.makedirs(artifact_folder, exist_ok=True)

        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
            zip_ref.extractall(artifact_folder)

        print(f"Successfully downloaded and extracted: {artifact_name}")
        return True
    else:
        print(f"Failed to download artifact {artifact_name}: {response.status_code}")
        return False


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Fetch artifacts from GitHub Actions workflow runs."
    )
    parser.add_argument(
        "--num-artifacts",
        type=int,
        default=DEFAULT_NUM_RUNS,
        help=f"Number of artifacts to fetch (default: {DEFAULT_NUM_RUNS})",
    )
    parser.add_argument(
        "--max-runs",
        type=int,
        default=DEFAULT_MAX_RUNS_TO_CHECK,
        help=f"Maximum number of workflow runs to check (default: {DEFAULT_MAX_RUNS_TO_CHECK})",
    )
    args = parser.parse_args()

    num_runs = args.num_artifacts
    max_runs_to_check = args.max_runs
    print(
        f"Will fetch artifacts from {num_runs} workflow runs (checking up to {max_runs_to_check} runs)"
    )

    # Create artifacts directory if it doesn't exist
    os.makedirs(ARTIFACTS_DIR, exist_ok=True)

    try:
        # Get workflow ID
        workflow_id = get_workflow_id(WORKFLOW_NAME)
        print(f"Found workflow ID: {workflow_id}")

        print(
            f"Looking for {num_runs} workflow runs (checking up to {max_runs_to_check} recent runs)"
        )
        # Get recent workflow runs (filtered as needed)
        workflow_runs = get_recent_workflow_runs(
            workflow_id, num_runs, MAIN_BRANCH_ONLY, max_runs_to_check
        )
        print(f"Found {len(workflow_runs)} filtered workflow runs")

        # Process each workflow run
        for run in workflow_runs:
            run_id = run["id"]
            run_date = datetime.strptime(run["created_at"], "%Y-%m-%dT%H:%M:%SZ")
            run_status = run["status"]
            run_conclusion = run["conclusion"]
            commit_sha = run["head_sha"]

            print(
                f"\nProcessing run {run_id} (Created: {run_date}, Status: {run_status}, Conclusion: {run_conclusion}, Commit: {commit_sha[:8]})"
            )

            # Get artifacts for this run
            artifacts = get_run_artifacts(run_id)
            print(f"Found {len(artifacts)} artifacts for run {run_id}")

            # Download each artifact
            for artifact in artifacts:
                download_artifact(artifact, run_id, run_date, commit_sha)

            # Add a small delay to avoid hitting rate limits
            time.sleep(1)

        print("\nArtifact download process completed!")

    except Exception as e:
        print(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()
