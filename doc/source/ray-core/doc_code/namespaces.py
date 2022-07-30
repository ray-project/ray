# flake8: noqa

# fmt: off
# __implicit_start_begin__
import ray

ray.init(namespace="hello")
print(ray.state.__dict__)

# __implicit_start_end__
# fmt: on

# fmt: off
# __iter_rows_begin__

# __iter_rows_end__
# fmt: on