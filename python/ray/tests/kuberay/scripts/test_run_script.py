import subprocess

TEST_FILE = "test_script.py"

script_string = open(TEST_FILE).read()
out = subprocess.check_output(["python", "-c", script_string]).decode().strip()
print(out)
assert "Hello world." in out
print("ok")
