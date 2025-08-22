from ray.util.annotations import PublicAPI

import ray.util.accelerators.accelerators as _accelerators


@PublicAPI(stability="alpha")
class AcceleratorTypes:
    """Unified access point for all accelerator types supported by Ray, categorized by vendor.

    .. note::
        Accelerator types are dynamically collected from the `ray.util.accelerators.accelerators`
        module.

    Example:
        .. code-block:: python

            ############# Access via constant (TYPES) ###############
            import ray
            # Print all accelerator types as a raw string
            print(ray.util.accelerators.TYPES)

            ############# Access via AcceleratorTypes instance ###############
            from ray.util.accelerators.types import accelerators_types
            # Print all accelerator types categorized by vendor
            accelerators_types.print_all()

            # Print accelerator types for a specific vendor
            print("NVIDIA accelerators:\n", accelerators_types.nvidia_types)
            print("AMD accelerators:\n", accelerators_types.amd_types)

    Args:
        No explicit arguments required. The class dynamically initializes accelerator types
        by scanning the `ray.util.accelerators.accelerators` module on instantiation.
    """
    def __init__(self):
        self._all_constants = {
            name: getattr(_accelerators, name)
            for name in dir(_accelerators)
            if not (name.startswith("__") and name.endswith("__"))
        }

        VENDORS = ["NVIDIA", "AMD", "INTEL", "GOOGLE", "HUAWEI", "AWS"]
        for vendor in VENDORS:
            vendor_lower = vendor.lower()
            type_list = sorted(
                [
                    name
                    for name in self._all_constants.keys()
                    if name.startswith(f"{vendor}_")
                ]
            )
            setattr(self, f"{vendor_lower}_types", "\n".join(type_list))

    def print_all(self):
        """Print all accelerator types categorized by vendor"""
        vendor_map = {
            "nvidia": "NVIDIA",
            "amd": "AMD",
            "intel": "Intel",
            "google": "Google",
            "huawei": "Huawei",
            "aws": "AWS",
        }
        for vendor_key, vendor_name in vendor_map.items():
            type_string = getattr(self, f"{vendor_key}_types")
            if type_string:
                print(f"\n{vendor_name} Accelerators:")
                print(type_string)

    @property
    def all_constants(self):
        """Return a string composed of all keys in the _all_constants dictionary"""
        return "\n".join(self._all_constants.keys())


types = AcceleratorTypes()