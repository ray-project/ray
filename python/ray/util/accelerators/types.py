import ray.util.accelerators.accelerators as _accelerators


class AcceleratorTypes:
    def __init__(self):
        self._all_constants = {
            name: getattr(_accelerators, name)
            for name in dir(_accelerators)
            if not (name.startswith('__') and name.endswith('__'))
        }

        self.nvidia_types = "\n".join(sorted([name for name in self._all_constants.keys()
                                  if name.startswith('NVIDIA_')]))
        self.amd_types = "\n".join(sorted([name for name in self._all_constants.keys()
                               if name.startswith('AMD_')]))
        self.intel_types = "\n".join(sorted([name for name in self._all_constants.keys()
                                 if name.startswith('INTEL_')]))
        self.google_types = "\n".join(sorted([name for name in self._all_constants.keys()
                                 if name.startswith('GOOGLE_')]))
        self.huawei_types = "\n".join(sorted([name for name in self._all_constants.keys()
                                 if name.startswith('HUAWEI_')]))
        self.aws_types = "\n".join(sorted([name for name in self._all_constants.keys()
                              if name.startswith('AWS_')]))

    def print_all(self):
        """Print all accelerator types categorized by vendor"""
        if self.nvidia_types:
            print("\nNVIDIA Accelerators:")
            print(self.nvidia_types)

        if self.amd_types:
            print("\nAMD Accelerators:")
            print(self.amd_types)

        if self.intel_types:
            print("\nIntel Accelerators:")
            print(self.intel_types)

        if self.google_types:
            print("\nGoogle Accelerators:")
            print(self.google_types)

        if self.huawei_types:
            print("\nHuawei Accelerators:")
            print(self.huawei_types)

        if self.aws_types:
            print("\nAWS Accelerators:")
            print(self.aws_types)

    @property
    def all_constants(self):
        """Return a string composed of all keys in the _all_constants dictionary"""
        return "\n".join(self._all_constants.keys())


types = AcceleratorTypes()
