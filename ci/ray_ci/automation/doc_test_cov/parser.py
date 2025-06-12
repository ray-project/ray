from typing import List
import subprocess
import json
from test_results import CodeSnippet, DocFile, BazelTarget
import pandas as pd
from pandas import json_normalize

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
            rst_cmd = f"find {self.ray_path}/doc -type d -path '*/venv/*' -prune -o \\( -type f -name '*.rst' -o -name '*.txt' \\) -print | xargs grep -H '^[[:space:]]*{search_string_rst}{end_anchor}'"
            rst_result = subprocess.run(["bash", "-c", rst_cmd],
                                cwd=self.ray_path,
                                capture_output=True,
                                text=True)

        if search_string_md:
            # Search for Markdown style literalinclude
            end_anchor = "$" if strict else ""
            md_cmd = f"find {self.ray_path}/doc -type d -path '*/venv/*' -prune -o \\( -type f -name '*.md' -o -name '*.txt' \\) -print | xargs grep -H '^[[:space:]]*{search_string_md}{end_anchor}'"
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
                for target in targets:
                    for file in target.files:
                        file_name = file.file_name.lstrip("//").split(":")[-1].split("/")[-1]
                        snippet_file_name = snippet.ref_to_file.lstrip("//").split(":")[-1].split("/")[-1]
                        if snippet_file_name == file_name:
                            snippet.testing_info.append(target)

    def save_doc_files_to_csv(self, docfiles: List[DocFile], filename: str = "results/csv/final_test_results.csv"):
        df = pd.DataFrame([docfile.to_dict() for docfile in docfiles])
        df.to_csv(filename, index=False)
        #df.to_csv("results/csv/final_test_results_2.csv", index=False, columns=["file_path", "snippet_type", "ref_to_file", "testing_info"])
        new_df = pd.concat([pd.DataFrame(doc.to_dict() for doc in docfiles),
                json_normalize(doc.to_dict()["code_snippets"] for doc in docfiles),
                json_normalize(doc.to_dict()["code_snippets"]["testing_info"] for doc in docfiles if doc.to_dict()["code_snippets"]["testing_info"])],
                axis=1)
        new_df.to_csv("results/csv/final_test_results_3_new.csv", index=False)
