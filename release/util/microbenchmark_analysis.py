"""
Compare the last two versions and output the change rate.
This can also be used to draw graph, which is not
implemented yet.

Usage: python microbenchmark_analysis.py
"""
import glob

from collections import defaultdict

FRIST_VERSION = 0
LAST_VERSION = 5
FILES = sorted(glob.glob("./release_logs/[0-9].[0-9].[0-9]/microbenchmark.txt"))

task_info = defaultdict(list)
task_std_info = defaultdict(list)
version_list = []


def get_task_type(line):
    return line.split("per")[0]


def get_task_performance(line):
    return float(line.split(" ")[-3])


def get_task_std(line):
    return float(line.split(" ")[-1])


def main():
    for file_name in FILES:
        version = file_name.split("/")[1]
        version_list.append(version)

        with open(file_name) as file:
            for line in file.readlines():
                if line.startswith("#") or line.startswith("\n"):
                    continue
                line = line.strip()
                task_type = get_task_type(line)
                task_performance = get_task_performance(line)
                task_standard_deviation = get_task_std(line)
                task_info[task_type].append(task_performance)
                task_std_info[task_type].append(task_standard_deviation)

    for task_type, task_performance_list in task_info.items():
        # Newly introduced fields are not going to be compared.
        if len(task_performance_list) < 2:
            continue

        latest_perf = task_performance_list[-1]
        second_latest_perf = task_performance_list[-2]
        change_rate = (latest_perf - second_latest_perf) / second_latest_perf * 100
        print(
            "{} performance change rate: {}%".format(task_type, round(change_rate, 2))
        )


if __name__ == "__main__":
    main()
