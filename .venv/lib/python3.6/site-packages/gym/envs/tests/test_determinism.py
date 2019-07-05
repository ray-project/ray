import numpy as np
import pytest

from gym.envs.tests.spec_list import spec_list

@pytest.mark.parametrize("spec", spec_list)
def test_env(spec):
    # Note that this precludes running this test in multiple
    # threads. However, we probably already can't do multithreading
    # due to some environments.
    env1 = spec.make()
    env1.seed(0)
    initial_observation1 = env1.reset()
    env1.action_space.seed(0)
    action_samples1 = [env1.action_space.sample() for i in range(4)]
    step_responses1 = [env1.step(action) for action in action_samples1]
    env1.close()

    env2 = spec.make()
    env2.seed(0)
    initial_observation2 = env2.reset()
    env2.action_space.seed(0)
    action_samples2 = [env2.action_space.sample() for i in range(4)]
    step_responses2 = [env2.step(action) for action in action_samples2]
    env2.close()

    for i, (action_sample1, action_sample2) in enumerate(zip(action_samples1, action_samples2)):
        try:
            assert_equals(action_sample1, action_sample2)
        except AssertionError:
            print('env1.action_space=', env1.action_space)
            print('env2.action_space=', env2.action_space)
            print('action_samples1=', action_samples1)
            print('action_samples2=', action_samples2)
            print('[{}] action_sample1: {}, action_sample2: {}'.format(i, action_sample1, action_sample2))
            raise

    # Don't check rollout equality if it's a a nondeterministic
    # environment.
    if spec.nondeterministic:
        return

    assert_equals(initial_observation1, initial_observation2)

    for i, ((o1, r1, d1, i1), (o2, r2, d2, i2)) in enumerate(zip(step_responses1, step_responses2)):
        assert_equals(o1, o2, '[{}] '.format(i))
        assert r1 == r2, '[{}] r1: {}, r2: {}'.format(i, r1, r2)
        assert d1 == d2, '[{}] d1: {}, d2: {}'.format(i, d1, d2)

        # Go returns a Pachi game board in info, which doesn't
        # properly check equality. For now, we hack around this by
        # just skipping Go.
        if spec.id not in ['Go9x9-v0', 'Go19x19-v0']:
            assert_equals(i1, i2, '[{}] '.format(i))

def assert_equals(a, b, prefix=None):
    assert type(a) == type(b), "{}Differing types: {} and {}".format(prefix, a, b)
    if isinstance(a, dict):
        assert list(a.keys()) == list(b.keys()), "{}Key sets differ: {} and {}".format(prefix, a, b)

        for k in a.keys():
            v_a = a[k]
            v_b = b[k]
            assert_equals(v_a, v_b)
    elif isinstance(a, np.ndarray):
        np.testing.assert_array_equal(a, b)
    elif isinstance(a, tuple):
        for elem_from_a, elem_from_b in zip(a, b):
            assert_equals(elem_from_a, elem_from_b)
    else:
        assert a == b
