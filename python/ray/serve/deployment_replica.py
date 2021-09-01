"""
Contains classes, helper functions, data / wrapper classes to represent and
facilitate actions on the level of a single replica in serve deployment.
"""

from dataclasses import dataclass

@dataclass
class ReplicaTag:
    # Contextual input sources
    deployment_tag: str
    replica_suffix: str
    # Generated replica name with fixed naming rule
    replica_tag: str
    delimiter: str = "#"

    def __init__(self, deployment_tag: str, replica_suffix: str):
        self.replica_tag = f"{deployment_tag}{self.delimiter}{replica_suffix}"

    @classmethod
    def from_str(self, replica_tag):
        parsed = replica_tag.split(self.delimiter)
        assert len(parsed) == 2, (
            f"Given replica name {replica_tag} didn't match pattern, please "
            f"ensure it has exactly two fields with delimiter {self.delimiter}"
        )
        self.deployment_tag = parsed[0]
        self.replica_suffix = parsed[1]
        self.replica_tag = replica_tag

    def __str__(self):
        return self.replica_tag

    @property
    def deployment_tag(self):
        return self.deployment_tag

    @property
    def replica_suffix(self):
        return self.replica_suffix
