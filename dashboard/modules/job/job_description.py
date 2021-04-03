import attr
from attr.validators import instance_of, in_, deep_mapping
from ray.core.generated import common_pb2


@attr.s(repr=False, slots=True, hash=True)
class _AnyValidator(object):
    def __call__(self, inst, name, value):
        pass

    def __repr__(self):
        return "<any validator for any type>"


def any_():
    return _AnyValidator()


@attr.s(kw_only=True, slots=True)
class JobDescription:
    # The job driver language, this field determines how to start the
    # driver. The value is one of the names of enum Language defined in
    # common.proto, e.g. PYTHON
    language = attr.ib(type=str, validator=in_(common_pb2.Language.keys()))
    # The url to download the job package archive. The archive format is
    # one of “zip”, “tar”, “gztar”, “bztar”, or “xztar”. Please refer to
    # https://docs.python.org/3/library/shutil.html#shutil.unpack_archive
    url = attr.ib(type=str, validator=instance_of(str))
    # The entry to start the driver.
    # PYTHON:
    #   - The basename of driver filename without extension in the job
    #   package archive.
    # JAVA:
    #   - The driver class full name in the job package archive.
    driverEntry = attr.ib(type=str, validator=instance_of(str))
    # The driver arguments in list.
    # PYTHON:
    #   -  The arguments to pass to the main() function in driver entry.
    #   e.g. [1, False, 3.14, "abc"]
    # JAVA:
    #   - The arguments to pass to the driver command line.
    #   e.g. ["-custom-arg", "abc"]
    driverArgs = attr.ib(type=list, validator=instance_of(list), default=[])
    # The environment vars to pass to job config, type of keys should be str.
    env = attr.ib(
        type=dict,
        validator=deep_mapping(
            key_validator=instance_of(str),
            value_validator=any_(),
            mapping_validator=instance_of(dict)),
        default={})
