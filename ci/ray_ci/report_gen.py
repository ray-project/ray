import os
import shutil
from pathlib import Path
from textwrap import dedent
import re
import json


BASE_DIR = os.getenv("BAZEL_DIR")
COMMIT_HASH = os.getenv("COMMIT_HASH")
REPORT_DIR = Path(f"{BASE_DIR}/reports/")
LOG_SOURCE_DIR = Path(f"{BASE_DIR}/artifacts/bazel_event_logs/")
FAILED_LOGS_DIR = Path(f"{BASE_DIR}/artifacts/failed_test_logs/")
REPORT_NAME = os.getenv("REPORT_LABEL")

MODERN_CSS = """
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
<style>
    :root {
        --bs-body-bg: #0f172a;
        --bs-body-color: #e2e8f0;
        --bs-primary: #3b82f6;
        --bs-border-color: #334155;
    }
    body { 
        background-color: var(--bs-body-bg);
        color: var(--bs-body-color);
    }
    .card {
        background-color: #1e293b;
        border: 1px solid var(--bs-border-color);
        box-shadow: 0 1px 3px rgba(0,0,0,0.2);
    }
    .list-group-item {
        background-color: #1e293b;
        color: #e2e8f0;
        border-color: var(--bs-border-color);
    }
    /* Fixed navigation styling */
    .nav-sidebar {
        width: 280px;
        position: fixed;
        left: 0;
        top: 0;
        bottom: 0;
        overflow-y: auto;
        padding: 1rem;
        background-color: #1e293b;
        border-right: 1px solid var(--bs-border-color);
        padding-right: 0.5rem;
        z-index: 1000;
    }
    .main-content {
        margin-left: 280px;
        padding: 2rem;
        min-height: 100vh;
        overflow-y: auto;
        height: 100vh;
    }
    /* Search bar styling */
    .search-container {
        position: sticky;
        top: 0;
        z-index: 1020;
        background-color: var(--bs-body-bg);
        padding: 1rem 0;
        margin: -1rem -0.5rem 1rem;
    }
    
    /* Search input styling */
    .search-container input {
        background-color: #1e293b;
        border-color: var(--bs-border-color);
        color: white;
    }
    
    .search-container input::placeholder {
        color: rgba(255, 255, 255, 0.6);
    }
    /* Active state styling */
    .list-group-item.active {
        background-color: #1e40af !important;
        border-color: #3b82f6 !important;
        color: white !important;
        border-left: 4px solid var(--bs-primary);
    }
    /* Improved code blocks */
    pre {
        background-color: #0f172a;
        color: #7dd3fc;
        padding: 1rem;
        border-radius: 0.375rem;
        border: 1px solid var(--bs-border-color);
        overflow-x: auto;
    }
    /* Table styling */
    .table {
        color: #e2e8f0;
    }
    .table td {
        border-color: var(--bs-border-color);
    }
    /* Updated text colors */
    .text-primary {
        color: white !important;
    }
    .text-muted {
        color: #94a3b8 !important;
    }
    
    /* Scrollable log list */
    .log-list {
        max-height: 400px;
        overflow-y: auto;
        background-color: #1e293b;
        border-radius: 0.375rem;
        border: 1px solid var(--bs-border-color);
    }
    .log-list::-webkit-scrollbar {
        width: 8px;
        background-color: #1e293b;
    }
    .log-list::-webkit-scrollbar-thumb {
        background-color: #334155;
        border-radius: 4px;
    }

    /* Ensure white text in code blocks and tables */
    pre {
        color: white !important;
    }
    .environment-table {
        color: white !important;
    }
    
    /* Hover states for log links */
    .log-list a {
        color: white !important;
        text-decoration: none;
    }
    .log-list a:hover {
        color: var(--bs-primary) !important;
    }

    /* Tab navigation styling */
    .nav-tabs .nav-link {
        color: white !important;
        border-color: var(--bs-border-color);
    }
    .nav-tabs .nav-link:hover {
        color: var(--bs-primary) !important;
        border-color: var(--bs-border-color);
    }
    .nav-tabs .nav-link.active {
        color: var(--bs-primary) !important;
        background-color: #1e293b;
        border-color: var(--bs-border-color);
    }

    /* Navigation text overflow prevention */
    #test-case-nav a,
    #error-list a {
        white-space: normal !important;
        word-wrap: break-word !important;
        word-break: break-word !important;
        padding: 8px 12px !important;
        line-height: 1.4 !important;
    }

    /* Ensure proper spacing in nav sidebar */
    .nav-sidebar .list-group-item {
        border-radius: 0;
        margin-bottom: 2px;
    }

    /* Common Errors list styling */
    #error-list {
        background-color: #1e293b;
        border-radius: 0.375rem;
        border: 1px solid var(--bs-border-color);
        margin-bottom: 1rem;
    }

    /* Test Cases navigation styling */
    #test-case-nav {
        max-height: calc(100vh - 400px);
        overflow-y: auto;
    }
</style>
"""

# rename all log files to remove colons
def rename_log_files():
    """Rename log files to remove invalid characters for GitHub artifact uploads."""
    for log_file in FAILED_LOGS_DIR.glob("*.zip"):
        # Replace all invalid characters
        safe_name = log_file.name
        safe_name = safe_name.replace("::", "/")
        safe_name = safe_name.replace('"', "_")
        safe_name = safe_name.replace("<", "_")
        safe_name = safe_name.replace(">", "_")
        safe_name = safe_name.replace("|", "_")
        safe_name = safe_name.replace("*", "_")
        safe_name = safe_name.replace("?", "_")
        safe_name = safe_name.replace("\r", "")
        safe_name = safe_name.replace("\n", "")
        safe_name = safe_name.replace("[", "_")
        safe_name = safe_name.replace("]", "_")
        safe_name = safe_name.replace("{", "_")
        safe_name = safe_name.replace("}", "_")
        safe_name = safe_name.replace(":", "_")

        dest_path = FAILED_LOGS_DIR / safe_name
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(log_file, dest_path)


def get_bazel_failures():
    """Parse Bazel logs to find compilation failures."""
    failed_tests = {}
    log_files = Path(f"{BASE_DIR}/artifacts/bazel_event_logs/").glob("bazel_log*")
    cnt = 0

    for log_file in log_files:
        try:
            with open(log_file, "r") as f:
                for line in f:
                    event = json.loads(line)

                    def search(label, value, res):
                        for key, child in value.items():
                            if key == label and isinstance(child, str):
                                res.append(child)
                            elif isinstance(child, dict):
                                search(label, child, res)

                    labels = []
                    stderrs = []
                    search("stderr", event, stderrs)
                    search("label", event, labels)
                    label = None
                    if len(labels) > 0:
                        label = labels[0]
                    stderr = "\n".join(stderrs)
                    if len(stderrs) > 0:
                        if label is not None:
                            failed_tests[label] = stderr
                        elif "error generated" in stderr:
                            failed_tests["Error " + str(cnt)] = "\n".join(stderrs)
                            cnt += 1

        except Exception as e:
            print(f"Error reading {log_file}: {str(e)}")
    return failed_tests


def generate_html_report():
    """Generate JSON results file only."""
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    # Collect test results data
    test_results = {
        "commit_hash": COMMIT_HASH,
        "label": REPORT_NAME,
        "test_cases": [],
        "error_counts": {},
    }

    # Process test summaries
    test_summaries_dir = Path(f"{BASE_DIR}/artifacts/test-summaries/")
    for idx, test_file in enumerate(test_summaries_dir.glob("*.txt")):
        if "000_header.txt" in test_file.name:
            continue

        try:
            content = test_file.read_text()
            test_name = test_file.name.replace("bazel-out::", "").replace("::", "/")
            pos = test_name.find("com_github_ray_project_ray")
            test_name = test_name[pos + len("com_github_ray_project_ray/") :]

            # Extract error pattern
            error_pattern = "No error"
            error_lines = [
                line.strip() for line in content.splitlines() if line.startswith("E   ")
            ]
            if error_lines:
                error_line = error_lines[-1]
                error_pattern = re.sub(r"\b\w+Error\b", "", error_line).strip(" :")
                if not error_pattern:
                    error_pattern = error_line.split(":")[-1].strip()

                test_results["error_counts"][error_pattern] = (
                    test_results["error_counts"].get(error_pattern, 0) + 1
                )

            test_results["test_cases"].append(
                {
                    "id": f"test-case-{idx}",
                    "name": test_name,
                    "content": content,
                    "error_pattern": error_pattern,
                    "label": REPORT_NAME,
                }
            )

        except Exception as e:
            print(f"Error processing {test_file}: {str(e)}")
            continue

    # Process Bazel failures
    bazel_failures = get_bazel_failures()
    for idx, (label, content) in enumerate(bazel_failures.items()):
        error_pattern = content.split("ERROR:")[0].strip()
        test_results["error_counts"][error_pattern] = (
            test_results["error_counts"].get(error_pattern, 0) + 1
        )

        test_results["test_cases"].append(
            {
                "id": f"bazel-failure-{idx}",
                "name": label,
                "content": content,
                "error_pattern": error_pattern,
                "label": REPORT_NAME,
            }
        )

    # Save JSON results
    json_path = REPORT_DIR / f"{REPORT_NAME}.json"
    with open(json_path, "w") as f:
        json.dump(test_results, f, indent=2)


def move_generated_report():
    """Copy log files to report directory."""
    if not FAILED_LOGS_DIR.exists():
        return
    shutil.copytree(
        FAILED_LOGS_DIR, REPORT_DIR / f"{REPORT_NAME}_logs", dirs_exist_ok=True
    )


def generate_summary_report(labels):
    """Generate a summary report combining multiple test reports."""
    all_results = []

    # Load JSON results for each label
    for label in labels:
        json_path = REPORT_DIR / f"{label}.json"
        if not json_path.exists():
            print(f"Warning: Results for label {label} not found at {json_path}")
            continue

        with open(json_path) as f:
            results = json.load(f)
            # Ensure each test case has the correct label
            for test_case in results["test_cases"]:
                test_case["label"] = label
            all_results.append(results)

    # Combine results
    combined_results = {
        "commit_hash": COMMIT_HASH,
        "test_cases": [],
        "error_counts_by_label": {
            label: {} for label in labels
        },  # Track errors by label
    }

    for results in all_results:
        # Get the label from the results object
        label = None
        if results["test_cases"]:
            label = results["test_cases"][0]["label"]

        if not label or label not in labels:
            continue

        combined_results["test_cases"].extend(results["test_cases"])

        # Track errors by label only
        for error, count in results["error_counts"].items():
            combined_results["error_counts_by_label"][label][error] = (
                combined_results["error_counts_by_label"][label].get(error, 0) + count
            )

    # Generate test cases HTML
    test_cases_html = []
    for test_case in combined_results["test_cases"]:
        test_cases_html.append(
            f"""
            <div class="card test-card mb-4" id="{test_case['id']}" 
                 data-error="{test_case['error_pattern']}"
                 data-label="{test_case['label']}">
                <div class="card-body">
                    <h5 class="card-title text-danger">{test_case['name']}</h5>
                    <div class="mb-3">
                        <span class="badge bg-danger">Failed</span>
                        <span class="badge bg-secondary ms-2">{test_case['label']}</span>
                    </div>
                    <pre>{test_case['content']}</pre>
                </div>
            </div>
        """
        )

    # Generate HTML with navigation
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Summary Test Report</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
        <style>
            :root {{
                --bs-body-bg: #f8fafc;
                --bs-body-color: #1e293b;
                --bs-primary: #3b82f6;
            }}
            
            body {{ 
                background-color: var(--bs-body-bg);
                color: var(--bs-body-color);
            }}
            
            .nav-top {{
                position: sticky;
                top: 0;
                z-index: 1030;
                background-color: white;
                padding: 1rem;
                border-bottom: 1px solid #e2e8f0;
                box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            }}
            
            .nav-top .nav-link {{
                color: #64748b;
                padding: 0.5rem 1rem;
                margin: 0 0.25rem;
                border-radius: 0.375rem;
            }}
            
            .nav-top .nav-link:hover {{
                background-color: #f1f5f9;
                color: #1e293b;
            }}
            
            .nav-top .nav-link.active {{
                background-color: #3b82f6;
                color: white;
            }}
            
            .nav-sidebar {{
                width: 280px;
                position: fixed;
                left: 0;
                top: 72px;  /* Height of nav-top */
                bottom: 0;
                overflow-y: auto;
                padding: 1rem;
                background-color: white;
                border-right: 1px solid #e2e8f0;
            }}
            
            .main-content {{
                margin-left: 280px;
                padding: 2rem;
                min-height: 100vh;
            }}
            
            .card {{
                border: none;
                box-shadow: 0 1px 3px rgba(0,0,0,0.1);
                background-color: white;
                border-radius: 0.5rem;
                margin-bottom: 1rem;
            }}
            
            .search-container {{
                margin-bottom: 1.5rem;
            }}
            
            pre {{
                background-color: #f8fafc;
                padding: 1rem;
                border-radius: 0.375rem;
                border: 1px solid #e2e8f0;
            }}
            
            .list-group-item {{
                border-color: #e2e8f0;
            }}
            
            .list-group-item:hover {{
                background-color: #f1f5f9;
            }}
            
            .error-section {{
                margin-bottom: 1.5rem;
            }}
            
            .error-section-title {{
                font-size: 1.1rem;
                font-weight: 600;
                margin-bottom: 0.75rem;
                padding-bottom: 0.5rem;
                border-bottom: 1px solid #e2e8f0;
            }}
            
            .error-label-badge {{
                font-size: 0.8rem;
                padding: 0.25rem 0.5rem;
                border-radius: 0.25rem;
                margin-left: 0.5rem;
            }}
            
            /* Collapsible sections */
            .error-section-title {{
                cursor: pointer;
                user-select: none;
            }}
            
            .error-section-title::after {{
                content: '▼';
                float: right;
                transition: transform 0.3s;
                font-size: 0.8rem;
            }}
            
            .error-section-title.collapsed::after {{
                transform: rotate(-90deg);
            }}
            
            .error-section .list-group {{
                transition: max-height 0.3s ease-out;
                overflow: hidden;
            }}
            
            .error-section-title.collapsed + .list-group {{
                max-height: 0 !important;
            }}
        </style>
    </head>
    <body>
        <!-- Top Navigation -->
        <nav class="nav-top">
            <div class="d-flex justify-content-between align-items-center">
                <div class="d-flex align-items-center">
                    <h4 class="me-4 mb-0">Test Reports</h4>
                    <div class="nav nav-pills" id="test-tabs">
                        {''.join(f'<a class="nav-link {label == labels[0] and "active" or ""}" data-label="{label}" href="#">{label}</a>' for label in labels)}
                    </div>
                </div>
            </div>
        </nav>

        <!-- Sidebar with Errors by Label -->
        <nav class="nav-sidebar">
            <h5 class="mb-3">Errors</h5>
            
            <!-- Errors by Label Sections -->
            {generate_error_summaries_by_label(combined_results['error_counts_by_label'], labels)}
        </nav>

        <!-- Main Content -->
        <main class="main-content">
            <!-- Search Bar -->
            <div class="search-container">
                <input type="text" class="form-control" placeholder="Search test cases..." 
                       id="searchInput" onkeyup="filterTests()">
            </div>

            <!-- Test Cases -->
            <div id="test-cases-section">
                {''.join(test_cases_html)}
            </div>
        </main>

        <script>
            let allTestCards = [];
            const errorVisitState = {{}};
            let currentActiveLabel = '';
            let labelChangeTriggeredByError = false;
            
            document.addEventListener('DOMContentLoaded', () => {{
                allTestCards = Array.from(document.querySelectorAll('.test-card'));
                document.querySelectorAll('.error-item').forEach(initializeErrorItem);
                
                // Initialize tab navigation
                document.querySelectorAll('#test-tabs .nav-link').forEach(tab => {{
                    tab.addEventListener('click', (e) => {{
                        e.preventDefault();
                        
                        // Update active state
                        document.querySelectorAll('#test-tabs .nav-link').forEach(t => 
                            t.classList.remove('active'));
                        tab.classList.add('active');
                        
                        // Filter test cases by label
                        const label = tab.getAttribute('data-label');
                        currentActiveLabel = label;
                        filterTestsByLabel(label);
                        
                        // Only toggle error sections if this label change wasn't triggered by an error click
                        if (!labelChangeTriggeredByError) {{
                            toggleErrorSectionsByLabel(label);
                        }}
                        
                        // Reset the flag
                        labelChangeTriggeredByError = false;
                    }});
                }});
                
                // Initialize collapsible sections
                document.querySelectorAll('.error-section-title').forEach(title => {{
                    title.addEventListener('click', () => {{
                        title.classList.toggle('collapsed');
                    }});
                }});
                
                // Show initial label's test cases and error sections
                currentActiveLabel = '{labels[0]}';
                filterTestsByLabel(currentActiveLabel);
                toggleErrorSectionsByLabel(currentActiveLabel);
            }});
            
            function toggleErrorSectionsByLabel(activeLabel) {{
                // Expand sections for active label, collapse others
                document.querySelectorAll('.error-section').forEach(section => {{
                    const sectionLabel = section.getAttribute('data-label');
                    const sectionTitle = section.querySelector('.error-section-title');
                    
                    if (sectionLabel === activeLabel) {{
                        // Expand this section
                        sectionTitle.classList.remove('collapsed');
                    }} else {{
                        // Collapse other sections
                        sectionTitle.classList.add('collapsed');
                    }}
                }});
            }}
            
            function filterTestsByLabel(label) {{
                allTestCards.forEach(card => {{
                    const cardLabel = card.getAttribute('data-label');
                    card.style.display = (cardLabel === label) ? 'block' : 'none';
                }});
            }}
            
            function filterTests() {{
                const filter = document.getElementById('searchInput').value.toLowerCase();
                const activeLabel = document.querySelector('#test-tabs .nav-link.active')
                    .getAttribute('data-label');
                
                allTestCards.forEach(card => {{
                    const text = card.textContent.toLowerCase();
                    const cardLabel = card.getAttribute('data-label');
                    card.style.display = (text.includes(filter) && cardLabel === activeLabel) 
                        ? 'block' : 'none';
                }});
            }}
            
            function initializeErrorItem(errorItem) {{
                const errorPattern = errorItem.getAttribute('data-error');
                const errorLabel = errorItem.getAttribute('data-label') || null;
                
                const matchingCards = allTestCards.filter(card => {{
                    const cardError = card.getAttribute('data-error');
                    const cardLabel = card.getAttribute('data-label');
                    
                    // If error item has a label, match only cards with that label
                    if (errorLabel) {{
                        return cardError === errorPattern && cardLabel === errorLabel;
                    }}
                    
                    // Otherwise match all cards with the error pattern
                    return cardError === errorPattern;
                }});
                
                errorItem.addEventListener('click', (e) => {{
                    e.preventDefault();
                    const stateKey = errorLabel ? errorPattern + '-' + errorLabel : errorPattern;
                    const state = errorVisitState[stateKey] || {{ currentIndex: -1 }};
                    state.currentIndex = (state.currentIndex + 1) % matchingCards.length;
                    errorVisitState[stateKey] = state;
                    
                    // If the error is from a different label than the current active one,
                    // switch to that label first
                    if (errorLabel && errorLabel !== currentActiveLabel) {{
                        // Set flag to indicate label change was triggered by error click
                        labelChangeTriggeredByError = true;
                        
                        // Find and click the tab for this label
                        const targetTab = document.querySelector('#test-tabs .nav-link[data-label="' + errorLabel + '"]');
                        if (targetTab) {{
                            targetTab.click();
                        }}
                    }}
                    
                    // Now scroll to the target card
                    const targetCard = matchingCards[state.currentIndex];
                    targetCard.scrollIntoView({{ behavior: 'smooth', block: 'start' }});
                    
                    const badge = errorItem.querySelector('.badge');
                    if (badge) {{
                        badge.textContent = state.currentIndex + 1 + '/' + matchingCards.length;
                    }}
                }});
            }}
        </script>
        
        <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
    </body>
    </html>
    """

    summary_path = REPORT_DIR / "summary.html"
    summary_path.write_text(dedent(html_content))
    print(f"Summary report generated at: {summary_path}")


def generate_error_summaries_from_dict(error_counts):
    """Generate error summaries HTML from a dictionary of error counts."""
    if not error_counts:
        return ""

    sorted_errors = sorted(error_counts.items(), key=lambda x: x[1], reverse=True)
    return "\n".join(
        f'<a href="#" class="list-group-item list-group-item-action error-item d-flex justify-content-between align-items-center py-2 text-truncate" data-error="{error}">'
        f'<span class="text-truncate">{error}</span><span class="badge bg-danger ms-2">{count}</span></a>'
        for error, count in sorted_errors
    )


def generate_error_summaries_by_label(error_counts_by_label, labels):
    """Generate error summaries HTML organized by label."""
    if not error_counts_by_label:
        return ""

    html_sections = []

    # Ensure we process labels in the order they were provided
    for label in labels:
        error_counts = error_counts_by_label.get(label, {})
        if not error_counts:
            continue

        sorted_errors = sorted(error_counts.items(), key=lambda x: x[1], reverse=True)
        error_items = "\n".join(
            f'<a href="#" class="list-group-item list-group-item-action error-item d-flex justify-content-between align-items-center py-2 text-truncate" data-error="{error}" data-label="{label}">'
            f'<span class="text-truncate">{error}</span><span class="badge bg-danger ms-2">{count}</span></a>'
            for error, count in sorted_errors
        )

        html_sections.append(
            f"""
        <div class="error-section" data-label="{label}">
            <div class="error-section-title">{label}</div>
            <div class="list-group">
                {error_items}
            </div>
        </div>
        """
        )

    return "\n".join(html_sections)


def clean_base_dir():
    for item in Path(BASE_DIR).iterdir():
        if item.name == "reports":
            continue
        if item.is_file():
            item.unlink()
        elif item.is_dir():
            shutil.rmtree(item)


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "summary":
        if len(sys.argv) < 3:
            print("Error: Must provide labels when using summary option")
            print("Usage: python report_gen.py summary label1 label2 ...")
            sys.exit(1)
        labels = sys.argv[2:]
        generate_summary_report(labels)
    else:
        generate_html_report()
        rename_log_files()
        move_generated_report()
        clean_base_dir()
        print(f"Report generated at: {REPORT_DIR}/{REPORT_NAME}.json")
