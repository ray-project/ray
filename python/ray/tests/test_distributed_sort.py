import pytest
import sys

from ray.experimental.raysort import main


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows")
def test_distributed_sort():
    args = main.get_args(
        [
            "--total_data_size=1_000_000_000",
            "--num_mappers=4",
            "--num_reducers=4",
            "--num_mappers_per_round=2",
            "--ray_address=",
            "--skip_input",
            "--skip_output",
        ]
    )
    main.main(args)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
