# Replaced with the current commit when building the wheels.
commit = "{{RAY_COMMIT_SHA}}"
version = "3.0.0.dev0"

if __name__ == "__main__":
    print("%s %s" % (version, commit))
