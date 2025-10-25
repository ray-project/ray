import pytest

from ray.rllib.utils.metrics.stats import (
    MeanStats,
    MaxStats,
    MinStats,
    SumStats,
    LifetimeSumStats,
    EmaStats,
    PercentilesStats,
)
from ray.rllib.utils.test_utils import check
from ray.rllib.utils.framework import try_import_torch

torch, _ = try_import_torch()

# Skip all tests if torch is not available
pytestmark = pytest.mark.skipif(torch is None, reason="torch not installed")


def get_device(use_gpu):
    """Helper to get device based on GPU availability and test parameter."""
    if use_gpu:
        if not torch.cuda.is_available():
            pytest.skip("GPU not available")
        return torch.device("cuda")
    return torch.device("cpu")


class TestTensorHandling:
    """Tests to ensure GPU tensors stay on GPU until reduce/peek, and CPU tensors work correctly."""

    @pytest.mark.parametrize("use_gpu", [True, False])
    def test_mean_stats_tensors(self, use_gpu):
        device = get_device(use_gpu)
        stats = MeanStats(window=3)

        stats.push(torch.tensor(2.0, device=device))
        stats.push(torch.tensor(4.0, device=device))
        stats.push(torch.tensor(6.0, device=device))

        assert len(stats.values) == 3
        for value in stats.values:
            assert isinstance(value, torch.Tensor)
            assert value.device.type == device.type

        result = stats.peek()
        assert isinstance(result, float)
        check(result, 4.0)

        result = stats.reduce(compile=True)
        assert isinstance(result, float)
        check(result, 4.0)
        assert len(stats.values) == 0

    @pytest.mark.parametrize("use_gpu", [True, False])
    def test_sum_stats_tensors(self, use_gpu):
        device = get_device(use_gpu)
        stats = SumStats(window=3)

        stats.push(torch.tensor(2.0, device=device))
        stats.push(torch.tensor(4.0, device=device))
        stats.push(torch.tensor(6.0, device=device))

        for value in stats.values:
            assert isinstance(value, torch.Tensor)
            assert value.device.type == device.type

        check(stats.peek(), 12.0)
        check(stats.reduce(compile=True), 12.0)

    @pytest.mark.parametrize("use_gpu", [True, False])
    def test_max_stats_tensors(self, use_gpu):
        device = get_device(use_gpu)
        stats = MaxStats(window=5)

        stats.push(torch.tensor(1.0, device=device))
        stats.push(torch.tensor(5.0, device=device))
        stats.push(torch.tensor(3.0, device=device))

        for value in stats.values:
            assert isinstance(value, torch.Tensor)
            assert value.device.type == device.type

        check(stats.peek(), 5.0)
        check(stats.reduce(compile=True), 5.0)

    @pytest.mark.parametrize("use_gpu", [True, False])
    def test_min_stats_tensors(self, use_gpu):
        device = get_device(use_gpu)
        stats = MinStats(window=5)

        stats.push(torch.tensor(5.0, device=device))
        stats.push(torch.tensor(1.0, device=device))
        stats.push(torch.tensor(3.0, device=device))

        for value in stats.values:
            assert isinstance(value, torch.Tensor)
            assert value.device.type == device.type

        check(stats.peek(), 1.0)
        check(stats.reduce(compile=True), 1.0)

    @pytest.mark.parametrize("use_gpu", [True, False])
    def test_ema_stats_tensors(self, use_gpu):
        device = get_device(use_gpu)
        ema_coeff = 0.1
        stats = EmaStats(ema_coeff=ema_coeff)

        stats.push(torch.tensor(10.0, device=device))

        assert isinstance(stats._value, torch.Tensor)
        assert stats._value.device.type == device.type

        stats.push(torch.tensor(20.0, device=device))

        assert isinstance(stats._value, torch.Tensor)
        assert stats._value.device.type == device.type

        result = stats.peek()
        assert isinstance(result, float)
        check(result, 11.0)

        stats.push(torch.tensor(30.0, device=device))
        check(stats.peek(), 12.9)

    @pytest.mark.parametrize("use_gpu", [True, False])
    def test_percentiles_stats_tensors(self, use_gpu):
        device = get_device(use_gpu)
        stats = PercentilesStats(percentiles=[0, 50, 100], window=10)

        for i in range(1, 6):
            stats.push(torch.tensor(float(i), device=device))

        for value in stats.values:
            assert isinstance(value, torch.Tensor)
            assert value.device.type == device.type

        result = stats.peek(compile=True)
        assert isinstance(result, dict)
        check(result[0], 1.0)
        check(result[50], 3.0)
        check(result[100], 5.0)

    @pytest.mark.parametrize("use_gpu", [True, False])
    def test_lifetime_sum_stats_tensors(self, use_gpu):
        device = get_device(use_gpu)
        stats = LifetimeSumStats()

        stats.push(torch.tensor(10.0, device=device))

        assert isinstance(stats._lifetime_sum, torch.Tensor)
        assert stats._lifetime_sum.device.type == device.type

        stats.push(torch.tensor(20.0, device=device))

        assert isinstance(stats._lifetime_sum, torch.Tensor)
        assert stats._lifetime_sum.device.type == device.type

        result = stats.peek()
        assert isinstance(result, float)
        check(result, 30.0)

        result = stats.reduce(compile=True)
        assert isinstance(result, float)
        check(result, 30.0)

    @pytest.mark.parametrize("use_gpu", [True, False])
    def test_series_stats_no_window_tensors(self, use_gpu):
        device = get_device(use_gpu)
        stats = MeanStats(window=None)

        stats.push(torch.tensor(10.0, device=device))
        check(stats.peek(), 10.0)

        stats.push(torch.tensor(20.0, device=device))
        check(stats.peek(), 15.0)

        stats.push(torch.tensor(30.0, device=device))
        check(stats.peek(), 20.0)

        assert isinstance(stats.values[0], torch.Tensor)
        assert stats.values[0].device.type == device.type

    def test_mixed_cpu_gpu_not_mixed(self):
        if not torch.cuda.is_available():
            pytest.skip("GPU not available")

        device = torch.device("cuda")
        stats = MeanStats(window=5)

        stats.push(torch.tensor(2.0, device=device))
        stats.push(torch.tensor(4.0, device=device))

        for value in stats.values:
            assert value.device.type == "cuda"

        stats_cpu = MeanStats(window=5)
        stats_cpu.push(torch.tensor(2.0))
        stats_cpu.push(torch.tensor(4.0))

        for value in stats_cpu.values:
            assert value.device.type == "cpu"

    @pytest.mark.parametrize("use_gpu", [True, False])
    def test_reduce_performance(self, use_gpu):
        device = get_device(use_gpu)
        stats = SumStats(window=100)

        for i in range(100):
            stats.push(torch.tensor(float(i), device=device))

        for value in stats.values:
            assert isinstance(value, torch.Tensor)
            assert value.device.type == device.type

        result = stats.reduce(compile=True)
        expected = sum(range(100))
        check(result, expected)

    @pytest.mark.parametrize("use_gpu", [True, False])
    def test_nan_handling(self, use_gpu):
        device = get_device(use_gpu)
        stats = MeanStats(window=5)

        stats.push(torch.tensor(1.0, device=device))
        stats.push(torch.tensor(float("nan"), device=device))
        stats.push(torch.tensor(3.0, device=device))

        result = stats.peek()
        check(result, 2.0)

    @pytest.mark.parametrize("use_gpu", [True, False])
    def test_window_overflow(self, use_gpu):
        device = get_device(use_gpu)
        stats = MeanStats(window=3)

        for i in range(1, 6):
            stats.push(torch.tensor(float(i), device=device))

        assert len(stats.values) == 3
        check(stats.peek(), 4.0)


class TestBatchValuesToCPU:
    """Tests for batch_values_to_cpu utility function."""

    @pytest.mark.parametrize("use_gpu", [True, False])
    def test_batch_tensors_to_cpu(self, use_gpu):
        device = get_device(use_gpu)
        from ray.rllib.utils.metrics.stats.utils import batch_values_to_cpu

        tensors = [
            torch.tensor(1.0, device=device),
            torch.tensor(2.0, device=device),
            torch.tensor(3.0, device=device),
        ]

        cpu_values = batch_values_to_cpu(tensors)

        assert isinstance(cpu_values, list)
        assert len(cpu_values) == 3
        check(cpu_values, [1.0, 2.0, 3.0])

    def test_batch_non_tensors(self):
        from ray.rllib.utils.metrics.stats.utils import batch_values_to_cpu

        values = [1.0, 2.0, 3.0]
        result = batch_values_to_cpu(values)

        assert isinstance(result, list)
        check(result, [1.0, 2.0, 3.0])

    def test_batch_empty_list(self):
        from ray.rllib.utils.metrics.stats.utils import batch_values_to_cpu

        result = batch_values_to_cpu([])
        assert result == []


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
