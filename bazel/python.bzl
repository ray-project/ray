# py_test_module_list creates a py_test target for each 
# Python file in `files`
def py_test_module_list(files, size, deps, extra_srcs, **kwargs):
  for file in files:
    # remove .py
    name = file[:-3]
    native.py_test(
      name=name,
      size=size,
      srcs=extra_srcs+[file],
      **kwargs
    )
