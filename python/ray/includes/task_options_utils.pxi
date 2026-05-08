# Convert a Python node label dictionary into a C++ unordered_map.
#
# Args:
#     label_dict: A Python dictionary mapping label keys to label values.
#         If None, no labels are added.
#     label_map: Output C++ map populated with UTF-8 encoded key/value pairs.
#
# Raises:
#     ValueError: If any label key or value is not a string.
#
# Returns:
#     0 on success.
cdef int prepare_labels(
        dict label_dict,
        unordered_map[c_string, c_string] *label_map) except -1:

    if label_dict is None:
        return 0

    label_map[0].reserve(len(label_dict))
    for key, value in label_dict.items():
        if not isinstance(key, str):
            raise ValueError(f"Label key must be string, but got {type(key)}")
        if not isinstance(value, str):
            raise ValueError(f"Label value must be string, but got {type(value)}")
        label_map[0][key.encode("utf-8")] = value.encode("utf-8")

    return 0

# Convert a Python label selector dictionary into a C++ LabelSelector.
#
# Args:
#     label_selector_dict: A Python dictionary describing label selector
#         constraints. If None, an empty selector is created.
#     c_label_selector: Output C++ LabelSelector object.
#
# Raises:
#     ValueError: If selector keys or values are not strings, if a key is empty,
#         or if an "in(...)" / "!in(...)" selector has no values.
#
# Returns:
#     0 on success.
cdef int prepare_label_selector(
        dict label_selector_dict,
        CLabelSelector *c_label_selector) except -1:

    c_label_selector[0] = CLabelSelector()

    if label_selector_dict is None:
        return 0

    for key, value in label_selector_dict.items():
        if not isinstance(key, str):
            raise ValueError(f"Label selector key type must be string, but got {type(key)}")
        if not isinstance(value, str):
            raise ValueError(f"Label selector value must be string, but got {type(value)}")
        if key == "":
            raise ValueError("Label selector key must be a non-empty string.")
        if (value.startswith("in(") and value.endswith(")")) or \
           (value.startswith("!in(") and value.endswith(")")):
            inner = value[value.index("(")+1:-1].strip()
            if not inner:
                raise ValueError(f"No values provided for Label Selector '{value[:value.index('(')]}' operator on key '{key}'.")
        # Add key-value constraint to the LabelSelector object.
        c_label_selector[0].AddConstraint(key.encode("utf-8"), value.encode("utf-8"))

    return 0

def node_labels_match_selector(node_labels: Dict[str, str], selector: Dict[str, str]) -> bool:
    """
    Checks if the given node labels satisfy the label selector. This helper function exposes
    the C++ logic for determining if a node satisfies a label selector to the Python layer.
    """
    cdef:
        CNodeResources c_node_resources
        CLabelSelector c_label_selector
        unordered_map[c_string, c_string] c_labels_map

    prepare_labels(node_labels, &c_labels_map)
    SetNodeResourcesLabels(c_node_resources, c_labels_map)
    prepare_label_selector(selector, &c_label_selector)

    # Return whether the node resources satisfy the label constraint.
    return c_node_resources.HasRequiredLabels(c_label_selector)

# Convert Python function descriptors into C++ function descriptors.
#
# Args:
#     pyfd_list: A Python iterable of function descriptors. Each descriptor must
#         expose module_name, class_name, and function_name.
#
# Returns:
#     A C++ vector of function descriptors.
cdef c_vector[CFunctionDescriptor] prepare_function_descriptors(pyfd_list):
    cdef:
        c_vector[CFunctionDescriptor] fd_list

    fd_list.reserve(len(pyfd_list))
    for pyfd in pyfd_list:
        fd_list.push_back(CFunctionDescriptorBuilder.BuildPython(
            pyfd.module_name, pyfd.class_name, pyfd.function_name, b""))
    return fd_list

# Convert actor concurrency group options into C++ concurrency groups.
#
# Args:
#     concurrency_groups_dict: A Python dictionary mapping concurrency group
#         names to their function descriptors and max concurrency.
#     concurrency_groups: Output C++ vector populated with concurrency groups.
#
# Raises:
#     ValueError: If concurrency_groups_dict is None.
#
# Returns:
#     1 on success, matching the previous helper behavior.
cdef int prepare_actor_concurrency_groups(
        dict concurrency_groups_dict,
        c_vector[CConcurrencyGroup] *concurrency_groups) except -1:

    cdef:
        c_vector[CFunctionDescriptor] c_fd_list

    if concurrency_groups_dict is None:
        raise ValueError("Must provide actor concurrency groups dictionary.")

    concurrency_groups.reserve(len(concurrency_groups_dict))
    for key, value in concurrency_groups_dict.items():
        c_fd_list = prepare_function_descriptors(value["function_descriptors"])
        concurrency_groups.push_back(CConcurrencyGroup(
            key.encode("ascii"), value["max_concurrency"], move(c_fd_list)))
    return 1

# Convert fallback scheduling strategy options into C++ fallback options.
#
# Args:
#     fallback_strategy: A Python list of dictionaries. Each dictionary may
#         contain a "label_selector" entry.
#     fallback_strategy_vector: Output C++ vector populated with fallback
#         options.
#
# Raises:
#     ValueError: If fallback_strategy contains non-dict elements or invalid
#         label selector values.
#
# Returns:
#     0 on success.
cdef int prepare_fallback_strategy(
        list fallback_strategy,
        c_vector[CFallbackOption] *fallback_strategy_vector) except -1:

    cdef dict label_selector_dict
    cdef CLabelSelector c_label_selector

    if fallback_strategy is None:
        return 0

    for strategy_dict in fallback_strategy:
        if not isinstance(strategy_dict, dict):
            raise ValueError(
                "Fallback strategy must be a list of dicts, "
                f"but got list containing {type(strategy_dict)}")

        label_selector_dict = strategy_dict.get("label_selector")

        if label_selector_dict is not None and not isinstance(label_selector_dict, dict):
            raise ValueError("Invalid fallback strategy element: invalid 'label_selector'.")

        prepare_label_selector(label_selector_dict, &c_label_selector)

        fallback_strategy_vector.push_back(
             CFallbackOption(c_label_selector)
        )

    return 0

# Convert a Python resource dictionary into a C++ resource map.
#
# Args:
#     resource_dict: A Python dictionary mapping resource names to quantities.
#     resource_map: Output C++ map populated with ASCII encoded resource names
#         and floating-point quantities.
#
# Raises:
#     ValueError: If resource_dict is None, if a resource quantity is not numeric,
#         if a quantity is negative, or if a unit-instance resource has an
#         invalid fractional quantity greater than 1.
#
# Returns:
#     0 on success.
cdef int prepare_resources(
        dict resource_dict,
        unordered_map[c_string, double] *resource_map) except -1:
    cdef:
        list unit_resources

    if resource_dict is None:
        raise ValueError("Must provide resource map.")

    unit_resources = (
        f"{RayConfig.instance().predefined_unit_instance_resources().decode('utf-8')},"
        f"{RayConfig.instance().custom_unit_instance_resources().decode('utf-8')}"
    ).split(",")

    resource_map[0].reserve(len(resource_dict))
    for key, value in resource_dict.items():
        if not (isinstance(value, int) or isinstance(value, float)):
            raise ValueError("Resource quantities may only be ints or floats.")
        if value < 0:
            raise ValueError("Resource quantities may not be negative.")
        if value > 0:
            if (value >= 1 and isinstance(value, float)
                    and not value.is_integer() and str(key) in unit_resources):
                raise ValueError(
                    f"{key} resource quantities >1 must ",
                    f"be whole numbers. The specified quantity {value} is invalid.")
            resource_map[0][key.encode("ascii")] = float(value)
    return 0
