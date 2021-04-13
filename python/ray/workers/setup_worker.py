import argparse
import sys
import os
import subprocess

from ray._private.conda import get_conda_bin_executable, get_conda_activate_commands, get_or_create_conda_env

parser = argparse.ArgumentParser(
    description=("Set up the environment for a Ray worker and launch the worker."))

parser.add_argument(
    "--conda-env-name",
    type=str,
    help="the name of an existing conda env to activate")

parser.add_argument(
    "--conda-yaml-path",
    type=str,
    help="the path to a conda environment yaml to install")

args, remaining_args = parser.parse_known_args()

# conda_env_path = "/fake/path/environment.yml"
commands = []

if args.conda_env_name:
    commands += get_conda_activate_commands(args.conda_env_name)
elif args.conda_yaml_path:
    conda_env_name = get_or_create_conda_env(args.conda_yaml_path)
    commands += get_conda_activate_commands(conda_env_name)

# commands += ["python fake_default_worker.py"]
# remaining_args is like default_worker.py --node-ip...

commands += [' '.join(["exec python"] + remaining_args)]
command_separator = " && "
command_str = command_separator.join(commands)
# Like <conda cmds> && python default_worker.py --arg1=a --arg2=b ...
print("Command_str: ", command_str)
# subprocess.run([command_str], shell=True)
argv = sys.argv
print("This is the name of the program:", sys.argv[0])
  
print("Argument List:", str([sys.executable] + remaining_args))
# Step 0: Just test this script and see if it can activate a conda env CHECK!
# Step 0+: Create a conda env yaml CHECK! 
# Step 0++: Check if step 0+ is idempotent  CHECK!
os.execvp("bash", ["bash", "-c", command_str])
# os.execvp("ipython", ["ipython"] + [command])
# argv.pop(0) # pop setup_worker.py
# os.execvp(sys.executable, [sys.executable] + remaining_args)
