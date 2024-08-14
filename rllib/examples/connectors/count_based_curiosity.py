"""Placeholder for training with count-based curiosity.

The actual script can be found at a different location (see code below).
"""

if __name__ == "__main__":
    import subprocess
    import sys

    # Forward to "python ../curiosity/[same script name].py [same options]"
    command = [sys.executable, "../curiosity/", sys.argv[0]] + sys.argv[1:]

    # Run the script.
    subprocess.run(command, capture_output=True)
