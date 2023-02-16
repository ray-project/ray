import os
import sys
from typing import Dict

import click
import yaml


class FormatDumper(yaml.SafeDumper):
    last_indent = 0

    def write_line_break(self, data=None):
        if (self.indent or 0) < self.last_indent:
            super().write_line_break()

        super().write_line_break(data)
        self.last_indent = self.indent or 0


def replace_prepare(dt: Dict):
    if "prepare" in dt and "wait_cluster" in dt["prepare"]:
        _, _, nodes, timeout = dt.pop("prepare").split(" ")
        dt["wait_for_nodes"] = {"num_nodes": int(nodes), "timeout": int(timeout)}


@click.command()
@click.argument("legacy_config", type=str)
@click.argument("prefix", type=str)
@click.argument("group", type=str)
@click.argument("alert", type=str)
def main(legacy_config: str, prefix: str, group: str, alert: str):
    with open(legacy_config, "rt") as fp:
        config = yaml.safe_load(fp)

    tests = []
    for old in config:
        test = {}
        test["name"] = f"{prefix}_{old['name']}"

        test["group"] = group
        test["working_dir"] = os.path.basename(os.path.dirname(legacy_config))

        test["legacy"] = {
            "test_name": old["name"],
            "test_suite": os.path.basename(legacy_config)[:-5],
        }

        test["frequency"] = "FILLOUT"
        test["team"] = old.get("team", "FILLOUT")

        test["cluster"] = {
            "cluster_env": old["cluster"]["app_config"],
            "cluster_compute": old["cluster"]["compute_template"],
        }

        if "cloud_id" in old["cluster"]:
            test["cluster"]["cloud_id"] = old["cluster"]["cloud_id"]
        if "cloud_name" in old["cluster"]:
            test["cluster"]["cloud_name"] = old["cluster"]["cloud_name"]

        if "driver_setup" in old:
            test["driver_setup"] = old["driver_setup"]

        use_connect = old["run"].pop("use_connect", False)
        autosuspend = old["run"].pop("autosuspend_mins", None)
        if autosuspend:
            test["cluster"]["autosuspend_mins"] = int(autosuspend)

        test["run"] = old["run"]
        replace_prepare(test["run"])
        if "smoke_test" in old:
            test["smoke_test"] = old["smoke_test"]
            if "run" in test["smoke_test"]:
                replace_prepare(test["smoke_test"]["run"])

        test["run"]["type"] = "sdk_command" if not use_connect else "client"
        if not use_connect:
            test["run"]["file_manager"] = "anyscale_job"

        test["alert"] = alert

        tests.append(test)

    yaml.dump(tests, sys.stdout, Dumper=FormatDumper, sort_keys=False)
    sys.stdout.flush()


if __name__ == "__main__":
    main()
