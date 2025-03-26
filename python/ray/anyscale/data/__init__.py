# NOTE: This is a temporary workaround to make sure that `ray.data.__init__`
#       is invoked before any of the code from `ray.anyscale.data` could be imported
#       to prevent circular import problem
#
# TODO(DATA-813) remove temporary workaround
import ray.data  # noqa: F401
