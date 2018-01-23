from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import inspect


def code_gen(pd_obj, ray_obj, path):
    """Generate code skeleton for methods not in Ray

    Args:
        pd_obj: The pandas object to generate code from.
        ray_obj: The ray object to diff against.
        path: Path to output the file to.
    """

    with open(path, "w") as outfile:
        funcs = pandas_ray_diff(pd_obj, ray_obj)

        for func in funcs:
            if func[0] == "_" and func[1] != "_":
                continue
            if "attr" in func:
                # let's not mess with these
                continue
            try:
                outfile.write("\ndef " + func +
                              str(inspect.signature(getattr(pd_obj, func))) +
                              ":\n")
            except TypeError:
                outfile.write("\n@property")
                outfile.write("\ndef " + func + "(self):\n")
            except ValueError:
                continue
            outfile.write(
                "    raise NotImplementedError(\"Not Yet implemented.\")\n")


def code_gen_test(ray_obj, path, name):
    """Generate tests for methods in Ray."""

    with open(path, "a") as outfile:
        funcs = dir(ray_obj)

        for func in funcs:
            if func[0] == "_" and func[1] != "_":
                continue

            outfile.write("\n\ndef test_" + func + "():\n")
            outfile.write(
                "    ray_" + name + " = create_test_" + name + "()\n\n" +
                "    with pytest.raises(NotImplementedError):\n" +
                "        ray_" + name + "." + func)
            try:
                first = True
                param_num = \
                    len(inspect.signature(getattr(ray_obj, func)).parameters)
                if param_num > 1:
                    param_num -= 1

                for _ in range(param_num):
                    if first:
                        outfile.write("(None")
                        first = False
                    else:
                        outfile.write(", None")
            except (TypeError, ValueError, NotImplementedError):
                outfile.write("\n")
                continue

            if first:
                outfile.write("(")
            outfile.write(")\n")


def pandas_ray_diff(pd_obj, ray_obj):
    """Gets the diff of the methods in the Pandas and Ray objects.

    Args:
        pd_obj: The Pandas object to diff.
        ray_obj: The Ray object to diff.

    Returns:
        A list of method names that are different between the two.
    """
    pd_funcs = dir(pd_obj)
    ray_funcs = dir(ray_obj)

    pd_funcs = set(filter(lambda f: f[0] != "_" or f[1] == "_",
                          pd_funcs))

    diff = [x for x in pd_funcs if x not in set(ray_funcs)]
    return diff
