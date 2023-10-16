import argparse
import random
import subprocess


def run():
    parser = argparse.ArgumentParser(description="Your Script Description")
    parser.add_argument("command", help="The command to profile")
    parser.add_argument("-t", default="", help="Cuda APIs to be profiled")
    parser.add_argument("--stop-on-exit", default="true", help="profile finish on exit")
    parser.add_argument("--cudabacktrace", default="none", help="profile cuda apis")
    parser.add_argument("-o", default="report%p", help="output filename")
    parser.add_argument("-d", default="0", help="profiling duration")
    parser.add_argument("script", help="Python script to profile")
    parser.add_argument(
        "script_args", nargs=argparse.REMAINDER, help="Arguments for the Python script"
    )

    args = parser.parse_args()
    random_id = random.randint(1, 9999)

    if args.command != "profile":
        print("nothing")
    else:
        output_filepath = args.o.replace("%p", str(random_id)) + ".nsys-rep"
        with open(output_filepath, "w") as file:
            file.write("Mock file.\n")

    command = [args.script] + args.script_args
    subprocess.run(command)
