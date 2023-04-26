load("@bazel_skylib//lib:paths.bzl", "paths")

# py_test_module_list creates a py_test target for each
# Python file in `files`


def _dict_union(x, y):
   z = {}
   z.update(x)
   z.update(y)
   return z

def _is_ray_core_test(tags):
    if "team:core" not in tags:
        return False
    for tag in tags:
        if tag.startswith("small_size_python_tests"):
            return True
        if tag.startswith("medium_size_python_tests"):
            return True
        if tag.startswith("large_size_python_tests"):
            return True
    return False

def _add_ray_core_staging_tags(tags):
    if _is_ray_core_test(tags):
        tags.append("feature_test:use_ray_syncer=1")
    return tags

def py_test_module_list(files, size, deps, extra_srcs=[], name_suffix="", tags = [], env = {}, **kwargs):
    tags = _add_ray_core_staging_tags(tags)
    tags_without_feature = []

    feature_tags = ["main"]
    feature_envs = [env]

    test_prefix = "feature_test:"
    
    for tag in tags:
        if tag.startswith(test_prefix):
            tag = tag[len(test_prefix):]
            key, val = tag.split("=")
            feature_tags.append(key)
            feature_envs.append(_dict_union(env, {"RAY_" + key: val}))
        else:
            tags_without_feature.append(tag)
    for file in files:
        # remove .py
        for i in range(len(feature_tags)):
            basename = paths.split_extension(file)[0] + name_suffix
            if basename == file:
                basename = basename + "_test"
            if i != 0:
                basename = basename + "@" + feature_tags[i]
            test_tags = tags_without_feature + [feature_tags[i]]
            test_env = feature_envs[i]
            # print(basename, test_tags, test_env)
            native.py_test(
                name = basename,
                size = size,
                main = file,
                srcs = extra_srcs + [file],
                deps = deps,
                tags = test_tags,
                env = test_env,
                **kwargs
            )

def py_test_run_all_subdirectory(include, exclude, extra_srcs, **kwargs):
    for file in native.glob(include = include, exclude = exclude, allow_empty=False):
        print(file)
        basename = paths.split_extension(file)[0]
        if basename == file:
            basename = basename + "_test"
        native.py_test(
            name = basename,
            srcs = extra_srcs + [file],
            **kwargs
        )

# Runs all included notebooks as py_test targets, by first converting them to .py files with "test_myst_doc.py".
def py_test_run_all_notebooks(include, exclude, allow_empty=False, **kwargs):
    for file in native.glob(include = include, exclude = exclude, allow_empty=allow_empty):
        print(file)
        basename = paths.split_extension(file)[0]
        if basename == file:
            basename = basename + "_test"
        native.py_test(
            name = basename,
            main = "test_myst_doc.py",
            srcs = ["//doc:test_myst_doc.py"],
            # --find-recursively will look for file in all
            # directories inside cwd recursively if it cannot
            # find it right away. This allows to deal with
            # mismatches between `name` and `data` args.
            args = ["--find-recursively", "--path", file],
            **kwargs
        )
