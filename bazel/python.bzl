# py_test_module_list creates a py_test target for each
# Python file in `files`
def py_test_module_list(files, size, deps, extra_srcs, name_suffix="", **kwargs):
    for file in files:
        # remove .py
        name = file[:-3] + name_suffix
        main = file
        native.py_test(
            name = name,
            size = size,
            main = file,
            srcs = extra_srcs + [file],
            **kwargs
        )

def py_test_run_all_subdirectory(include, exclude, extra_srcs, **kwargs):
    for file in native.glob(include = include, exclude = exclude):
        print(file)
        basename = file.rpartition("/")[-1]
        native.py_test(
            name = basename[:-3],
            srcs = extra_srcs + [file],
            **kwargs
        )
