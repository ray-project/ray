import os
import json
import glob
import re
from datetime import datetime, timedelta
from collections import defaultdict
import html


def find_all_test_reports():
    """Find all test report JSON files in the artifacts directory."""
    return glob.glob("artifacts/**/test-reports-*/*.json", recursive=True)


def extract_run_info(file_path):
    """Extract run date and commit hash from the file path."""
    # Extract run date from the path (format: artifacts/YYYY-MM-DD_run_ID/...)
    date_match = re.search(r"artifacts/(\d{4}-\d{2}-\d{2})_run_(\d+)", file_path)
    if date_match:
        run_date = date_match.group(1)
        run_id = date_match.group(2)
    else:
        # Fallback to using file modification time
        file_stat = os.stat(file_path)
        run_date = datetime.fromtimestamp(file_stat.st_mtime).strftime("%Y-%m-%d")
        run_id = "unknown"

    # Extract commit hash from the path
    commit_match = re.search(r"test-reports-([a-f0-9]+)", file_path)
    commit_hash = commit_match.group(1) if commit_match else "unknown"

    return {"date": run_date, "run_id": run_id, "commit_hash": commit_hash}


def load_test_data():
    """Load all test data from JSON files and organize by test case."""
    all_files = find_all_test_reports()

    # Skip summary.html files
    all_files = [f for f in all_files if not f.endswith("summary.html")]

    # Group files by run (date + run_id + commit_hash)
    runs_by_key = defaultdict(list)
    for file_path in all_files:
        run_info = extract_run_info(file_path)
        key = f"{run_info['date']}_{run_info['run_id']}_{run_info['commit_hash']}"
        runs_by_key[key].append({"file_path": file_path, "run_info": run_info})

    # Sort runs by date (newest first)
    sorted_runs = sorted(runs_by_key.items(), key=lambda x: x[0], reverse=True)

    # Take only the most recent 20 runs (or fewer if not available)
    recent_runs = sorted_runs[:20]

    # First pass: collect all test names across all runs
    all_test_names = set()
    for _, files in recent_runs:
        for file_info in files:
            file_path = file_info["file_path"]
            try:
                with open(file_path, "r") as f:
                    data = json.load(f)

                label = data.get(
                    "label", os.path.basename(file_path).replace(".json", "")
                )

                # Add all test cases to our set of known tests
                for test_case in data.get("test_cases", []):
                    test_name = f"{label}::{test_case.get('name', 'unknown')}"
                    all_test_names.add(test_name)
            except Exception as e:
                print(f"Error processing {file_path}: {str(e)}")

    # If we have no test data, print a message and return empty data
    if not all_test_names:
        print("No test data found in the artifacts directory.")
        return {}, []

    # Second pass: build test history for each test
    test_history = {test_name: [] for test_name in all_test_names}
    run_metadata = []

    for run_key, files in recent_runs:
        run_info = files[0]["run_info"]
        run_date = run_info["date"]
        run_id = run_info["run_id"]
        commit_hash = run_info["commit_hash"]

        run_metadata.append(
            {"date": run_date, "run_id": run_id, "commit_hash": commit_hash}
        )

        # Track which tests failed in this run
        failed_tests = set()

        # Process each file in this run to find failing tests
        for file_info in files:
            file_path = file_info["file_path"]

            try:
                with open(file_path, "r") as f:
                    data = json.load(f)

                label = data.get(
                    "label", os.path.basename(file_path).replace(".json", "")
                )

                # Process each test case in the file (these are failures)
                for test_case in data.get("test_cases", []):
                    test_name = f"{label}::{test_case.get('name', 'unknown')}"
                    failed_tests.add(test_name)

                    test_history[test_name].append(
                        {
                            "run_date": run_date,
                            "run_id": run_id,
                            "commit_hash": commit_hash,
                            "status": "fail",
                            "error": test_case.get("error", ""),
                            "traceback": test_case.get("traceback", ""),
                        }
                    )
            except Exception as e:
                print(f"Error processing {file_path}: {str(e)}")

        # For all known tests not found in failed_tests, mark them as passing
        for test_name in all_test_names:
            if test_name not in failed_tests:
                test_history[test_name].append(
                    {
                        "run_date": run_date,
                        "run_id": run_id,
                        "commit_hash": commit_hash,
                        "status": "pass",
                        "error": "",
                        "traceback": "",
                    }
                )

    # Sort each test's history by date (oldest first)
    for test_name in test_history:
        test_history[test_name] = sorted(
            test_history[test_name], key=lambda x: x["run_date"]
        )

    return test_history, run_metadata


def calculate_flakiness(test_history):
    """Calculate flakiness score and failure rate for each test."""
    test_results = {}

    for test_name, history in test_history.items():
        if not history:
            continue

        # Count transitions between pass and fail
        transitions = 0
        last_status = None

        for run in history:
            if last_status is not None and run["status"] != last_status:
                transitions += 1
            last_status = run["status"]

        # Calculate failure rate
        failure_count = sum(1 for run in history if run["status"] == "fail")
        total_runs = len(history)
        failure_rate = failure_count / total_runs if total_runs > 0 else 0

        # Calculate flakiness score (higher means more flaky)
        # Flakiness is a combination of transitions and failure rate
        flakiness_score = (transitions / (total_runs - 1)) if total_runs > 1 else 0

        # Include all tests, not just flaky ones
        test_results[test_name] = {
            "name": test_name,
            "transitions": transitions,
            "failure_rate": failure_rate,
            "flakiness_score": flakiness_score,
            "total_runs": total_runs,
            "failure_count": failure_count,
            "history": history,
        }

    # Sort by failure rate (highest to lowest), then by name
    return sorted(test_results.values(), key=lambda x: (-x["failure_rate"], x["name"]))


def generate_html_report(all_tests, run_metadata):
    """Generate an HTML report for all tests with color-coded history bars."""
    html_output = (
        """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Test History Report</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        :root {
            --bs-body-bg: #f8fafc;
            --bs-body-color: #1e293b;
        }
        
        body { 
            background-color: var(--bs-body-bg);
            color: var(--bs-body-color);
            padding: 2rem;
        }
        
        .card {
            border: none;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            background-color: white;
            border-radius: 0.5rem;
            margin-bottom: 0.5rem;
        }
        
        .history-bar {
            display: flex;
            height: 20px;
            border-radius: 4px;
            overflow: visible;
            margin: 10px 0;
            background-color: #e2e8f0;
            padding: 0 1px;
        }
        
        .history-segment {
            width: 16px;
            height: 16px;
            margin: 2px 1px;
            border-radius: 2px;
            cursor: pointer;
            position: relative;
        }
        
        .history-segment.pass {
            background-color: #22c55e;
        }
        
        .history-segment.fail {
            background-color: #ef4444;
        }
        
        .history-segment:hover::before {
            content: attr(data-tooltip);
            position: absolute;
            bottom: 25px;
            left: 50%;
            transform: translateX(-50%);
            background-color: #334155;
            color: white;
            padding: 8px 12px;
            border-radius: 6px;
            white-space: pre-line;
            text-align: center;
            z-index: 1000;
            font-size: 13px;
            min-width: 180px;
            box-shadow: 0 3px 10px rgba(0,0,0,0.2);
        }
    </style>
</head>
<body>
    <div class="container-fluid">
        <h1 class="mb-4">Test History Report</h1>
        <p class="text-muted">Generated on: """
        + datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        + """</p>
        
        <div class="card mb-4">
            <div class="card-body">
                <h5 class="card-title">Summary</h5>
                <p>Analyzed """
        + str(len(run_metadata))
        + """ runs, showing """
        + str(len(all_tests))
        + """ tests.</p>
                <p>Runs analyzed (oldest to newest):</p>
                <div class="row">
    """
    )

    # Add run metadata (oldest to newest)
    sorted_metadata = sorted(run_metadata, key=lambda x: x["date"])
    for i, run in enumerate(sorted_metadata):
        html_output += f"""
                    <div class="col-md-3 mb-2">
                        <small>{i+1}. {run['date']} (Run {run['run_id'][:8]}...)</small>
                    </div>
        """

    html_output += """
                </div>
            </div>
        </div>
        
        <h2 class="mb-3">Test History</h2>
        <div class="mb-4">
            <div class="row">
                <div class="col-md-8">
                    <input type="text" class="form-control" id="searchInput" placeholder="Search tests...">
                </div>
            </div>
        </div>
        
        <div id="all-tests">
    """

    # Add each test
    for test in all_tests:
        test_name = test["name"]
        history = test["history"]
        failure_rate = test["failure_rate"] * 100  # Convert to percentage

        html_output += f"""
            <div class="card test-card" data-test-name="{html.escape(test_name)}" data-failure-rate="{failure_rate}">
                <div class="card-body py-2">
                    <div class="d-flex justify-content-between align-items-center">
                        <h6 class="mb-0">{html.escape(test_name)}</h6>
                        <span class="badge bg-{"danger" if failure_rate > 0 else "success"}">{failure_rate:.1f}% failures</span>
                    </div>
                    <div class="history-bar">
        """

        # Add history segments (oldest to newest, left to right)
        sorted_history = sorted(history, key=lambda x: x["run_date"])
        for run in sorted_history:
            status = run["status"]
            run_id = run["run_id"]
            commit_hash = run["commit_hash"]

            # Create a more detailed tooltip with date, run ID, status, and commit hash
            tooltip = f"{run['run_date']} (Run {run_id[:8]}): {status.upper()}\nCommit: {commit_hash[:8]}"

            # Create GitHub workflow URL - only if we have a valid run ID
            if run_id and run_id != "unknown" and run_id.isdigit():
                onclick_attr = f"onclick=\"window.open('https://github.com/antgroup/ant-ray/actions/runs/{run_id}', '_blank')\""
            else:
                onclick_attr = ""

            html_output += f"""
                        <div class="history-segment {status}" 
                             data-tooltip="{tooltip}"
                             {onclick_attr}></div>
            """

        html_output += """
                    </div>
                </div>
            </div>
        """

    html_output += """
        </div>
    </div>
    
    <script>
        document.getElementById('searchInput').addEventListener('keyup', function() {
            filterTests();
        });
        
        function filterTests() {
            const searchValue = document.getElementById('searchInput').value.toLowerCase();
            const testCards = document.querySelectorAll('.test-card');
            
            testCards.forEach(card => {
                const testName = card.getAttribute('data-test-name').toLowerCase();
                
                if (testName.includes(searchValue)) {
                    card.style.display = 'block';
                } else {
                    card.style.display = 'none';
                }
            });
        }
    </script>
    
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
    """

    return html_output


def main():
    print("Loading test data from artifacts...")
    test_history, run_metadata = load_test_data()

    if not test_history:
        print(
            "No test data found. Please make sure the artifacts directory contains test reports."
        )
        return

    print(f"Found {len(test_history)} unique tests across {len(run_metadata)} runs")

    print("Calculating flakiness scores...")
    all_tests = calculate_flakiness(test_history)

    print("Generating HTML report...")
    html_report = generate_html_report(all_tests, run_metadata)

    # Write the report to a file
    output_file = "flaky_report.html"
    with open(output_file, "w") as f:
        f.write(html_report)

    print(f"Report generated: {output_file}")


if __name__ == "__main__":
    main()
