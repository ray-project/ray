from typing import List

from pip_requirements_parser import Requirement, RequirementsFile


def parse_lock_file(lock_file_path: str):
    rf = RequirementsFile.from_file(lock_file_path)
    return rf.requirements


def write_lock_file(requirements: List[Requirement], lock_file_path: str):
    with open(lock_file_path, "w") as f:
        for req in requirements:
            f.write(req.requirement_line.line + "\n")
