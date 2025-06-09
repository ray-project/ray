from typing import List
import subprocess
import json
from test_results import CodeSnippet

class Parser:
    def __init__(self, ray_path: str):
        self.ray_path = ray_path

    # def parse_doc_files_for_literal_include_snippets(self) -> List[str]:
    #     """ parse the doc files for literal include snippets
    #     """
    #     literal_include_snippets = {}
    #     for file in self.ray_path:
    #         with open(file, "r") as f:
    #             content = f.read()
    #             if "literalinclude" in content:
    #                 print(file)

    def find_literal_include_snippets(self) -> List[CodeSnippet]:
        """
        Get literal include snippets in the doc/ directory.
        Searches for both RST style (.. literalinclude::) and Markdown style (```{literalinclude})
        """
        li_snippets = []
        list_of_files = []

        # Search for RST style literalinclude
        rst_cmd = f"find {self.ray_path}/doc -type f -name '*.rst' -o -name '*.txt' | xargs grep -H '.. literalinclude::'"
        rst_result = subprocess.run(["bash", "-c", rst_cmd],
                            cwd=self.ray_path,
                            capture_output=True,
                            text=True)

        # Search for Markdown style literalinclude
        md_cmd = f"find {self.ray_path}/doc -type f -o -name '*.md' -o -name '*.txt' | xargs grep -H '```{{literalinclude}}'"
        md_result = subprocess.run(["bash", "-c", md_cmd],
                            cwd=self.ray_path,
                            capture_output=True,
                            text=True)

        # Combine results
        rst_stdout = rst_result.stdout.strip()
        md_stdout = md_result.stdout.strip()


        # Combine and deduplicate files
        rst_files = rst_stdout.split("\n") if rst_stdout else []
        md_files = md_stdout.split("\n") if md_stdout else []
        list_of_files = list(set(rst_files + md_files))

        print(f"cmd: {md_cmd}")
        print(f"len of md_result: {len(md_files)}")

        # Process rst snippets
        for snippet in rst_files:
            if not snippet:  # Skip empty lines
                continue
            line = snippet.split(":")
            if len(line) >= 3:  # Ensure we have enough parts to process
                file_path = line[0].strip()
                file_type = line[1].strip() if len(line) > 1 else ""
                ref_to_file = line[3].strip() if len(line) > 2 else ""
                li_snippets.append(CodeSnippet(file_path=file_path, file_type=file_type, ref_to_file=ref_to_file))

        # Process md snippets
        for snippet in md_files:
            if not snippet:  # Skip empty lines
                continue
            line = snippet.split(":")
            # print(f"length of line: {len(line)}")
            if len(line) >= 2:  # Ensure we have enough parts to process
                fp = line[1].split()
                file_path = line[0].strip()
                file_type = fp[0].strip() if len(fp) >= 1 else ""
                ref_to_file = fp[1].strip() if len(fp) >= 2 else ""
                li_snippets.append(CodeSnippet(file_path=file_path, file_type=file_type, ref_to_file=ref_to_file))

        return li_snippets, list_of_files

    def parse_doc_files_for_code_block_snippets(self,) -> List[str]:
        """
        Get code block snippets in the doc/ directory.
        """

        li_snippets = []
        list_of_files = []
        cmd = f"find {self.ray_path}/doc -type f -name '*.rst' -o -name '*.md' -o -name '*.html' -o -name '*.txt' | xargs grep -H '.. literalinclude::'"
        result = subprocess.run(["bash", "-c", cmd],
                            cwd=self.ray_path,
                            capture_output=True,
                            text=True)
        stdout = result.stdout.strip()
        list_of_files = stdout.split("\n") if stdout else []
        snippets = stdout.split("\n") if stdout else []
        for snippet in snippets:
            line = snippet.split(":")
            li_snippets.append(CodeSnippet(file_path=line[0].strip(), file_type=line[1].strip(), ref_to_file=line[3].strip()))
        return li_snippets, list_of_files

    def save_code_snippets_to_file(self, file_paths: List[CodeSnippet], output_path: str) -> None:
        """Save a list of strings to a file, one string per line.
        Args:
            strings: List of strings to save
            output_path: Path to the output file
        """
        with open(output_path, 'w') as f:
            json.dump([s.to_dict() for s in file_paths], f, indent=4)

    def save_snippets_to_file(self, file_paths: List[str], output_path: str) -> None:
        """Save a list of strings to a file, one string per line.
        Args:
            strings: List of strings to save
            output_path: Path to the output file
        """
        with open(output_path, 'w') as f:
            for s in file_paths:
                f.write(f"{s}\n")