from typing import List, Dict
import json
import pandas as pd

class TestResults:
    def __init__(self):
        self.build_url = ""
        self.buildkite_jobs = []
        self.targets = List[BazelTarget]
        self.stats = {}

    def to_dict(self) -> Dict:
        """Convert the test results to a dictionary for JSON serialization"""
        return {
            "build_url": self.build_url,
            "buildkite_jobs": self.buildkite_jobs,
            "targets": [target.to_dict() for target in self.targets],
            "stats": self.stats
        }

    def set_buildkite_metadata(self, build_url: str, buildkite_jobs: Dict[str, str], commit: str):
        self.build_url = build_url
        self.buildkite_jobs = buildkite_jobs
        self.commit = commit

    def set_targets(self, targets: List[str]):
        self.targets= [BazelTarget(target) for target in targets]

    def add_targets(self, targets: List[str]):
        self.targets += [BazelTarget(target) for target in targets]

    def set_tested_targets(self, tested_targets: Dict[str, List[str]]):
        self.tested_targets = tested_targets

    def set_untested_targets(self, untested_targets: Dict[str, List[str]]):
        self.untested_targets = untested_targets

    def save_test_results(self, filename: str = "results/test_results.json"):
        """Save test results to a JSON file"""
        print(f"saving test results to {filename}")
        with open(filename, "w") as f:
            json.dump(self.to_dict(), f, indent=4)

    def calculate_test_coverage(self):
        tested_targets, tested_files, untested_targets, untested_files = 0, 0, 0, 0
        for target in self.targets:
            if target.active and target.tested:
                tested_targets += 1
                tested_files += len(target.files)
            elif target.active and not target.tested:
                untested_targets += 1
                untested_files += len(target.files)

        self.stats = {
                "tested_targets": tested_targets,
                "tested_files": tested_files,
                "untested_targets": untested_targets,
                "untested_files": untested_files,
                # "test_coverage_per_target": tested_targets / (tested_targets + untested_targets) * 100,
                # "test_coverage_per_file": tested_files / (tested_files + untested_files) * 100
            }

    def output_untested_targets(self, filename: str = "results/untested_targets.json"):
        self.targets = [target for target in self.targets if not target.tested and target.active]
        with open(filename, "w") as f:
            json.dump(self.to_dict(), f, indent=4)

    def output_results_csv_format(self, filename: str = "results/untested_targets.csv"):
        df = pd.DataFrame([target.to_dict() for target in self.targets if not target.tested and target.active])
        df.to_csv(filename, index=False)

class BazelTarget:
    def __init__(self, target: str):
        self.target_name = target
        self.tested = False
        self.status = ""
        self.active = False
        self.files = List[BazelFile]
        self.bazel_file_location = ""

    def to_dict(self) -> Dict:
        return {
            "target_name": self.target_name,
            "tested": self.tested,
            "status": self.status,
            "active": self.active,
            "files": [file.to_dict() for file in self.files],
            "bazel_file_location": self.bazel_file_location
        }

    def set_files(self, files: List[str]):
        self.files = [BazelFile(file) for file in files]

class BazelFile:
    def __init__(self, file_name: str):
        self.file_name = file_name
        self.file_refs = []

    def to_dict(self) -> Dict:
        return {
            "file_name": self.file_name,
            "file_refs": self.file_refs
        }

class CodeSnippet:
    def __init__(self, snippet_type: str, ref_to_file: str, testing_info: List[BazelTarget]=None):
        self.snippet_type = snippet_type
        self.ref_to_file = ref_to_file
        self.testing_info = []

    def to_dict(self) -> Dict:
        return {
            "snippet_type": self.snippet_type,
            "ref_to_file": self.ref_to_file,
            "testing_info": [target.to_dict() for target in self.testing_info]
        }

class DocFile:
    def __init__(self, file_path: str, snippets: List[CodeSnippet]):
        self.file_path = file_path
        self.code_snippets = snippets
        self.coverage_percentage = 0.00

    def to_dict(self) -> Dict:
        return {
            "file_path": self.file_path,
            "code_snippets": [snippet.to_dict() for snippet in self.code_snippets],
        }