import ray.util.accelerators.accelerators as _accelerators
from ray.util.annotations import PublicAPI, DeveloperAPI


@DeveloperAPI(stability="alpha")
class AcceleratorTypes:
    """Unified access point for all accelerator types supported by Ray, categorized by vendor.

        Accelerator types are dynamically collected from the `ray.util.accelerators.accelerators`
        module.

    Example:

            ############# Access via constant (TYPES) ###############
            import ray
            # Print all accelerator types as a raw string
            print(ray.util.accelerators.TYPES)

            ############# Access via AcceleratorTypes instance ###############
            from ray.util.accelerators.types import all_types, vendor_types
            # Print all accelerator types categorized by vendor
            print (all_types())

            # Print accelerator types for a specific vendor
            print ("NVIDIA accelerators:", vendor_types("NVIDIA"))
            print ("AMD accelerators:", vendor_types("AMD"))

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
        """Return all accelerator types categorized by vendor as a string"""
        vendor_map = {
            "nvidia": "NVIDIA",
            "amd": "AMD",
            "intel": "Intel",
            "google": "Google",
            "huawei": "Huawei",
            "aws": "AWS",
        }
        output = []
        for vendor_key, vendor_name in vendor_map.items():
            type_string = getattr(self, f"{vendor_key}_types")
            if type_string:
                output.append(f"\n{vendor_name} Accelerators:")
                output.append(type_string)
        return "\n".join(output)

    @property
    def all_constants(self):
        return self.print_all()


accelerator_types = AcceleratorTypes()


@PublicAPI(stability="alpha")
def all_types():
    """Return all accelerator types categorized by vendor (via print_all)

    Return: String of accelerator types categorized by vendor, separated by newlines
    """
    return accelerator_types.print_all()


@PublicAPI(stability="alpha")
def vendor_types(vendor: str):
    """Return accelerator types for a specific vendor (uppercase input)

    Args:
        vendor: Uppercase vendor name (e.g., "NVIDIA", "AMD")

    Returns:
        String of accelerator types for the specified vendor, separated by newlines
        Returns empty string if vendor is invalid or has no accelerators
    """
    valid_vendors = ["NVIDIA", "AMD", "INTEL", "GOOGLE", "HUAWEI", "AWS"]
    vendor = vendor.upper()
    if vendor not in valid_vendors:
        return ""
    vendor_lower = vendor.lower()
    return getattr(accelerator_types, f"{vendor_lower}_types", "")
