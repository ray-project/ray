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
