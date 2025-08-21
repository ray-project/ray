from ray.util.annotations import PublicAPI

import ray.util.accelerators.accelerators as _accelerators


class AcceleratorTypes:
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

    @PublicAPI(stability="alpha")
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
