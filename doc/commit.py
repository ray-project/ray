import subprocess

logs = subprocess.check_output(["git", "log", "--pretty=format:%H", "--first-parent", "origin/master", "-n", "1"]).decode("utf-8")
print(logs)