import pytest
import sys

from ray.experimental.raysort import main


def test_distributed_sort():
    main.args = main.get_args()
    main.args.ray_address = None
    main.args.total_data_size = 1_000_000_000
    main.args.skip_input = True
    main.args.skip_output = True
    main.main()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
