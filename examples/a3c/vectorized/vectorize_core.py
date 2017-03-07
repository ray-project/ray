from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gym
from gym import spaces

class Env(gym.Env):
    """Base class capable of handling vectorized environments.
    """
    metadata = {
        # This key indicates whether an env is vectorized (or, in the case of
        # Wrappers where autovectorize=True, whether they should automatically
        # be wrapped by a Vectorize wrapper.)
        'runtime.vectorized': True,
    }

    # Number of remotes. User should set this.
    n = None

    @property
    def monitor(self):
        if not self.metadata['runtime.vectorized']:
            # Just delegate if we're not actually vectorized (like
            # Unvectorize)
            return super(Env, self).monitor

        if not hasattr(self, '_monitor'):
            # Not much we can do if we don't know how wide we'll
            # be. This can happen when closing.
            if self.n is None:
                raise Exception('You must call "configure()" before accesssing the monitor for {}'.format(self))

            # Circular dependencies :(
            import wrappers
            import monitoring
            # We need to maintain pointers to these to avoid them being
            # GC'd. They have a weak reference to us to avoid cycles.
            self._unvectorized = [wrappers.WeakUnvectorize(self) for _ in range(self.n)]
            # Store reference to avoid GC
            # self._render_cached = monitoring.RenderCache(self)
            self._monitor = monitoring.Monitor(self._unvectorized)
        return self._monitor

class Wrapper(Env, gym.Wrapper):
    """Use this instead of gym.Wrapper iff you're wrapping a vectorized env,
    (or a vanilla env you wish to be vectorized).
    """
    # If True and this is instantiated with a non-vectorized environment,
    # automatically wrap it with the Vectorize wrapper.
    autovectorize = True

    def __init__(self, env):
        super(Wrapper, self).__init__(env)
        if not env.metadata.get('runtime.vectorized'):
            if self.autovectorize:
                # Circular dependency :(
                import vectorize_wrappers as wrappers
                env = wrappers.Vectorize(env)
            else:
                raise Exception('This wrapper can only wrap vectorized envs (i.e. where env.metadata["runtime.vectorized"] = True), not {}. Set "self.autovectorize = True" to automatically add a Vectorize wrapper.'.format(env))

        self.env = env

    def _configure(self, **kwargs):
        super(Wrapper, self)._configure(**kwargs)
        assert self.env.n is not None, "Did not set self.env.n: self.n={} self.env={} self={}".format(self.env.n, self.env, self)
        self.n = self.env.n

class ObservationWrapper(Wrapper, gym.ObservationWrapper):
    pass

class RewardWrapper(Wrapper, gym.RewardWrapper):
    pass

class ActionWrapper(Wrapper, gym.ActionWrapper):
    pass
