from typing import List
import subprocess
import json
from test_results import CodeSnippet, DocFile, BazelTarget
import pandas as pd
import copy

class DocParser:
    def __init__(self, ray_path: str):
        self.ray_path = ray_path

    def find_code_snippets(self, search_string_rst: str, search_string_md: str, strict: bool = False) -> List[DocFile]:
        """
        Get literal include snippets in the doc/ directory.
        Searches for both RST style (.. <string>::) and Markdown style (```{<string>})
        """
        doc_files = []

        if search_string_rst:
            # Search for RST style literalinclude
            end_anchor = "$" if strict else ""
            rst_cmd = f"find {self.ray_path}/doc -type d -path '*/venv/*' -prune -o \\( -type f -name '*.rst' \\) -print | xargs grep -H '^[[:space:]]*{search_string_rst}{end_anchor}'"
            rst_result = subprocess.run(["bash", "-c", rst_cmd],
                                cwd=self.ray_path,
                                capture_output=True,
                                text=True)

        if search_string_md:
            # Search for Markdown style literalinclude
            end_anchor = "$" if strict else ""
            md_cmd = f"find {self.ray_path}/doc -type d -path '*/venv/*' -prune -o \\( -type f -name '*.md' \\) -print | xargs grep -H '^[[:space:]]*{search_string_md}{end_anchor}'"
            md_result = subprocess.run(["bash", "-c", md_cmd],
                                cwd=self.ray_path,
                                capture_output=True,
                                text=True)

        # Combine results
        rst_stdout = rst_result.stdout.strip() if search_string_rst else ""
        md_stdout = md_result.stdout.strip() if search_string_md else ""

        # Combine and deduplicate files
        rst_files = rst_stdout.split("\n") if rst_stdout else []
        md_files = md_stdout.split("\n") if md_stdout else []

        print(f"len of rst_result: {len(rst_files)}")
        print(f"len of md_result: {len(md_files)}")

        # Process rst snippets
        for snippet in rst_files:
            if not snippet:  # Skip empty lines
                continue
            line = snippet.split(":")
            if len(line) >= 3:  # Ensure we have enough parts to process
                file_path = line[0].strip()
                snippet_type = line[1].strip() if len(line) > 1 else ""
                ref_to_file = line[3].strip() if len(line) > 2 else ""
                found = self.check_for_existing_code_snippets(doc_files, file_path.replace(self.ray_path, ""), ref_to_file, snippet_type)
                if not found:
                    doc_files.append(DocFile(file_path=file_path.replace(self.ray_path, ""), snippets=[CodeSnippet(snippet_type=snippet_type, ref_to_file=ref_to_file)]))

        # Process md snippets
        for snippet in md_files:
            if not snippet:  # Skip empty lines
                continue
            line = snippet.split(":")
            if len(line) >= 2:  # Ensure we have enough parts to process
                fp = line[1].split()
                file_path = line[0].strip()
                snippet_type = fp[0].strip() if len(fp) >= 1 else ""
                ref_to_file = fp[1].strip() if len(fp) >= 2 else ""
                found = self.check_for_existing_code_snippets(doc_files, file_path.replace(self.ray_path, ""), ref_to_file, snippet_type)
                if not found:
                    doc_files.append(DocFile(file_path=file_path.replace(self.ray_path, ""), snippets=[CodeSnippet(snippet_type=snippet_type, ref_to_file=ref_to_file)]))

        return doc_files

    def check_for_existing_code_snippets(self, doc_files: List[DocFile], file_path: str, ref_to_file: str, snippet_type: str) -> bool:
        """
        Get code block snippets in the doc/ directory.
        """
        for doc_file in doc_files:
            if file_path == doc_file.file_path:
                for snippet in doc_file.code_snippets:
                    if ref_to_file in snippet.ref_to_file:
                        return True
                doc_file.code_snippets.append(CodeSnippet(snippet_type=snippet_type, ref_to_file=ref_to_file))
                return True
        return False

    def save_doc_file_snippets(self, file_paths: List[DocFile], output_path: str) -> None:
        """Save a list of strings to a file, one string per line.
        Args:
            strings: List of strings to save
            output_path: Path to the output file
        """
        with open(output_path, "w") as f:
            json.dump([s.to_dict() for s in file_paths], f, indent=4)

    def assign_testing_info_to_code_snippets(self, doc_files: List[DocFile], targets: List[BazelTarget]) -> None:
        """
        Assign testing info to code snippets.
        """
        for doc_file in doc_files:
            for snippet in doc_file.code_snippets:
                self._find_and_assign_target(snippet, targets)

    def _find_and_assign_target(self, snippet, targets):
        for target in targets:
            for file in target.files:
                file_name = "/".join(file.file_name.lstrip("//").replace(":", "/").split("/")[-2:])
                if file_name in snippet.ref_to_file:
                    filtered_target = self.filter_target_to_single_file(target, file.file_name)
                    snippet.testing_info.append(filtered_target)
                    return

    def filter_target_to_single_file(self, target, filename):
        filtered_target = copy.deepcopy(target)
        filtered_target.files = [file for file in target.files if file.file_name == filename]
        return filtered_target

    def save_doc_files_to_json(self, docfiles: List[DocFile], filename: str = "results/json/final_test_results.json"):
        with open(filename, "w") as f:
            json.dump([docfile.to_dict() for docfile in docfiles], f, indent=4)

    def save_doc_files_to_csv(self, docfiles: List[DocFile], filename: str = "results/csv/final_test_results.csv"):
        df = pd.DataFrame([docfile.to_dict() for docfile in docfiles])
        df.to_csv(filename, index=False)

        rows = []
        for docfile in docfiles:
            for snippet in docfile.code_snippets:
                test_infos = snippet.testing_info or [None]
                for test_info in test_infos:
                    files = test_info.files if test_info and test_info.files else [None]

                    for file in files:
                        row = {
                            "file_path": docfile.file_path,
                            "snippet_type": snippet.snippet_type,
                            "ref_to_file": snippet.ref_to_file,
                            "target_name": test_info.target_name if test_info else None,
                            "tested": test_info.tested if test_info else None,
                            "status": test_info.status if test_info else None,
                            "bazel_file_location": test_info.bazel_file_location if test_info else None,
                            "file_name": file.file_name if file else None,
                        }
                        rows.append(row)

        df = pd.DataFrame(rows)
        df.to_csv(filename, index=False)

    def calculate_test_coverage(self,doc_files: List[DocFile]) -> dict:
        """
        Calculate test coverage for code snippets in documentation files.

        Args:
            doc_files: List of DocFile objects containing code snippets

        Returns:
            dict: Coverage statistics with total, tested, untested counts and percentage
        """
        total_snippets = 0
        tested_snippets = 0

        for doc_file in doc_files:
            for snippet in doc_file.code_snippets:
                total_snippets += 1

                # Check if snippet is tested
                if self.is_snippet_tested(snippet):
                    tested_snippets += 1

        untested_snippets = total_snippets - tested_snippets
        coverage_percentage = (tested_snippets / total_snippets * 100) if total_snippets > 0 else 0

        return {
            "total_snippets": total_snippets,
            "tested_snippets": tested_snippets,
            "untested_snippets": untested_snippets,
            "coverage_percentage": round(coverage_percentage, 2)
        }

    def is_snippet_tested(self, snippet: CodeSnippet) -> bool:
        """
        Determine if a code snippet is tested.

        Returns True if:
        - Has testing_info AND at least one testing_info.tested = True

        Returns False if:
        - No testing_info OR empty testing_info OR all testing_info.tested = False
        """
        if snippet.testing_info is None or not snippet.testing_info:
            return False

        for testing_info in snippet.testing_info:
            if testing_info and testing_info.tested:
                return True

        return False
