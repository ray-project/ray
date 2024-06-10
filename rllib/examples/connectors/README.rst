
.. note::
    RLlib's Connector API has been re-written from scratch for the new API stack (|newstack|).
    We are now referring to connector-pieces and -pipelines as :py:class:`~ray.rllib.connectors.connector_v2.ConnectorV2`
    (as opposed to ``Connector``, which continue to work on the old API stack |oldstack|).


- |newstack| `Atari image frame stacking <https://github.com/ray-project/ray/blob/master/rllib/examples/connectors/frame_stacking.py>`__:
   An example using Atari framestacking in a very efficient manner, NOT in the environment itself (as a `gym.Wrapper`),
   but by stacking the observations on-the-fly using `EnvToModule` and `LearnerConnector` pipelines.
   This method of framestacking is more efficient as it avoids having to send large observation
   tensors through the network (ray).

- |newstack| `Mean/STD filtering of observations <https://github.com/ray-project/ray/blob/master/rllib/examples/connectors/mean_std_filtering.py>`__:
   An example of a :py:class:`~ray.rllib.connectors.connector_v2.ConnectorV2` that filters all observations from the environment using a
   plain mean/STD filter (i.e. shift by mean and divide by std-dev). This example demonstrates
   how a stateful :py:class:`~ray.rllib.connectors.connector_v2.ConnectorV2` class has its states
   (here the means and std's of the individual observation items) coming from the different
   :py:class:`~ray.rllib.env.env_runner.EnvRunner` instances a) merged into one common state and
   then b) broadcast again back to the remote :py:class:`~ray.rllib.env.env_runner.EnvRunner` workers.

- |newstack| `Include previous-action(s) and/or previous reward(s) in RLModule inputs <https://github.com/ray-project/ray/blob/master/rllib/examples/connectors/prev_actions_prev_rewards.py>`__:
   An example of a :py:class:`~ray.rllib.connectors.connector_v2.ConnectorV2` that adds the n previous action(s)
   and/or the m previous reward(s) to the RLModule's input dict (to perform its forward passes, both
   for inference and training).

- |newstack| `Nested action spaces <https://github.com/ray-project/ray/blob/master/rllib/examples/connectors/nested_action_spaces.py>`__:
   Learning in arbitrarily nested action spaces, using an env in which the action space equals the
   observation space (both are complex, nested Dicts) and the policy has to pick actions
   that closely match (or are identical) to the previously seen observations.

- |newstack| `Nested observation spaces <https://github.com/ray-project/ray/blob/master/rllib/examples/connectors/nested_observation_spaces.py>`__:
   Learning in arbitrarily nested observation spaces
   (using a CartPole-v1 variant with a nested Dict observation space).
