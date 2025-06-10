from typing import List
import subprocess
import json
from test_results import CodeSnippet, DocFile

class Parser:
    def __init__(self, ray_path: str):
        self.ray_path = ray_path

    def find_code_snippets(self, search_string_rst: str, search_string_md: str) -> List[DocFile]:
        """
        Get literal include snippets in the doc/ directory.
        Searches for both RST style (.. <string>::) and Markdown style (```{<string>})
        """
        doc_files = []

        if search_string_rst:
            # Search for RST style literalinclude
            rst_cmd = f"find {self.ray_path}/doc -type d -path '*/venv/*' -prune -o \\( -type f -name '*.rst' -o -name '*.txt' \\) -print | xargs grep -H '^[[:space:]]*{search_string_rst}'"
            rst_result = subprocess.run(["bash", "-c", rst_cmd],
                                cwd=self.ray_path,
                                capture_output=True,
                                text=True)

        if search_string_md:
            # Search for Markdown style literalinclude
            md_cmd = f"find {self.ray_path}/doc -type d -path '*/venv/*' -prune -o \\( -type f -name '*.md' -o -name '*.txt' \\) -print | xargs grep -H '^[[:space:]]*{search_string_md}'"
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
        with open(output_path, 'w') as f:
            json.dump([s.to_dict() for s in file_paths], f, indent=4)
