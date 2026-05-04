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

cdef c_vector[CFunctionDescriptor] prepare_function_descriptors(pyfd_list):
    cdef:
        c_vector[CFunctionDescriptor] fd_list

    fd_list.reserve(len(pyfd_list))
    for pyfd in pyfd_list:
        fd_list.push_back(CFunctionDescriptorBuilder.BuildPython(
            pyfd.module_name, pyfd.class_name, pyfd.function_name, b""))
    return fd_list

cdef int prepare_actor_concurrency_groups(
        dict concurrency_groups_dict,
        c_vector[CConcurrencyGroup] *concurrency_groups):

    cdef:
        c_vector[CFunctionDescriptor] c_fd_list

    if concurrency_groups_dict is None:
        raise ValueError("Must provide it...")

    concurrency_groups.reserve(len(concurrency_groups_dict))
    for key, value in concurrency_groups_dict.items():
        c_fd_list = prepare_function_descriptors(value["function_descriptors"])
        concurrency_groups.push_back(CConcurrencyGroup(
            key.encode("ascii"), value["max_concurrency"], move(c_fd_list)))
    return 1


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

cdef int prepare_resources(
        dict resource_dict,
        unordered_map[c_string, double] *resource_map) except -1:
    cdef:
        list unit_resources

    if resource_dict is None:
        raise ValueError("Must provide resource map.")

    resource_map[0].reserve(len(resource_dict))
    for key, value in resource_dict.items():
        if not (isinstance(value, int) or isinstance(value, float)):
            raise ValueError("Resource quantities may only be ints or floats.")
        if value < 0:
            raise ValueError("Resource quantities may not be negative.")
        if value > 0:
            unit_resources = (
                f"{RayConfig.instance().predefined_unit_instance_resources()\
                .decode('utf-8')},"
                f"{RayConfig.instance().custom_unit_instance_resources()\
                .decode('utf-8')}"
            ).split(",")

            if (value >= 1 and isinstance(value, float)
                    and not value.is_integer() and str(key) in unit_resources):
                raise ValueError(
                    f"{key} resource quantities >1 must",
                    f" be whole numbers. The specified quantity {value} is invalid.")
            resource_map[0][key.encode("ascii")] = float(value)
    return 0