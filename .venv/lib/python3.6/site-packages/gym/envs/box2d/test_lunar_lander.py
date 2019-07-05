import pytest
try:
    import Box2D
    from .lunar_lander import LunarLander, LunarLanderContinuous, demo_heuristic_lander
except ImportError:
    Box2D = None


@pytest.mark.skipif(Box2D is None, reason='Box2D not installed')
def test_lunar_lander():
    _test_lander(LunarLander(), seed=0)

@pytest.mark.skipif(Box2D is None, reason='Box2D not installed')
def test_lunar_lander_continuous():
    _test_lander(LunarLanderContinuous(), seed=0)

@pytest.mark.skipif(Box2D is None, reason='Box2D not installed')
def _test_lander(env, seed=None, render=False):
    total_reward = demo_heuristic_lander(env, seed=seed, render=render)
    assert total_reward > 100


