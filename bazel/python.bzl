# py_test_module_list creates a py_test target for each
# Python file in `files`

test_modules = {}

def py_test_module_list(files, size, deps, extra_srcs, **kwargs):
    for file in files:
        # remove .py
        name = file[:-3]
        if name not in test_modules:
            test_modules[name] = {
                "size": size,
                "srcs": srcs + [file],
                "kwargs": **kwargs
            }

def py_test_run_all_subdirectory(include, exclude, extra_srcs, **kwargs):
    for file in native.glob(include = include, exclude = exclude):
        print(file)
        basename = file.rpartition("/")[-1]
        native.py_test(
            name = basename[:-3],
            srcs = extra_srcs + [file],
            **kwargs
        )

def add_tags(files, tags):
    for file in files:
        # remove .py
        name = file[:-3]
        if name not in test_modules:
            raise ValueError(f"name {name} doesn't exist. Cannot add tags {tags}")
            if "tags" not in test_modules[name]:
                test_modules[name]["tags"] = []
        test_modules[name]["tags"] += tags

def build():
    for name, body in test_modules.items():
        native.py_test(
            name = name,
            size = body["size"],
            srcs = body["srcs"],
            body["kwargs"]
        )
