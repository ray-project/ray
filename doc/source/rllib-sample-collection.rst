RLlib Sample Collection
=======================

The SampleCollector Class is Used to Store and Retrieve Temporary Data
----------------------------------------------------------------------

RLlib's RolloutWorkers, when running against a live environment,
use the `SamplerInput` class to interact with that environment to
produce batches of experiences.
The two implemented sub-classes of `SamplerInput` are `SyncSampler` and `AsyncSampler`
(residing under the `RolloutWorker.sampler` property).
In case the "_use_trajectory_view_api" top-level config key is set to True, which is the case
by default since version >=1.2.0, every such sampler object will itself use the
`SampleCollector` API to store and retrieve temporary environment-, model-, and other data
during episode rollouts.
The exact number of environment transitions and behavior for a single such rollout is determined
by several important config keys:
**batch_mode []**: The two possible values are "truncate_episodes" (default) and
  "complete_episodes".
  In the `batch_mode=truncated_episodes` setting, rollouts are performed over exactly
  `rollout_fragment_length` (see below) number of steps. Thereby, steps are counted
  as environment steps by default (see below) or as individual agent steps. It
  does not matter, whether one or more episodes end within this rollout or whether
  the rollout starts within an already ongoing episode.
  In the `batch_mode=complete_episodes` setting, each rollout is exactly one
  episode long, no matter how long this episode lasts. The `rollout_fragment_length`
  setting will be ignored. Note that you have to be careful when chosing this batch_mode
  if your environment does not terminate easily (could lead to enormous batch sizes).
**rollout_fragment_length [int]**: The exact number of environment- or agent steps to
  be performed per rollout, if the `batch_mode` setting is "truncate_episodes".
  `rollout_fragment_length` is ignored, if `batch_mode=complete_episodes`.
  The unit to count fragments in (environment- or agent steps) is set via `count_steps_by`
  within the `multiagent` config dict.
**horizon [int]**:
**soft_horizon**:
**no_done_at_end**:


Determines, how to build per-RolloutWorker batches, which are then
  usually concat'd to form the train batch. Note that "steps" below can mean different things (either env- or agent-steps) and depends on the
  `count_steps_by` (multiagent) setting below.
    # truncate_episodes: Each produced batch (when calling
    #   RolloutWorker.sample()) will contain exactly `rollout_fragment_length`
    #   steps. This mode guarantees evenly sized batches, but increases
    #   variance as the future return must now be estimated at truncation
    #   boundaries.
    # complete_episodes: Each unroll happens exactly over one episode, from
    #   beginning to end. Data collection will not stop unless the episode
    #   terminates or a configured horizon (hard or soft) is hit.

**rollout_fragment_length**:

During these rollouts, a Sampler uses its SampleCollector to store trajectory (episode)
data and to retrieve this data for a) model compute-action calls, b) calling a Policy's
postprocessing function (postproces_trajectory), and c) building a final batch to be used for training.
This latter train-batch may be concatenated with other RolloutWorkers' batches to form the actual train batch
(of size "train_batch_size").


Trajectory View API
-------------------
**#TODO maybe we shouldn't even call this an "API", since 99% of all this happens automatically under the hood.**

The trajectory view API is a common format by which Models and Policies communicate
to each other as well as to the SampleCollector, which data to store, how to store
and retrieve it, and how to present this data as inputs to the Policy's different methods.

In particular, the methods that will receive inputs based on trajectory view rules are
a) Policy.compute_actions_from_input_dict, b) Policy.postprocess_trajectory, and c)
Policy.learn_on_batch (and consecutively: the Policy's loss function).

The input data to these methods can stem from either the environment (observations, rewards, and infos),
the model itself (previously computed actions, internal state outputs, action-probs, etc..)
or the Sampler (e.g. agent index, env ID, episode ID, timestep, etc..).
All data has an associated time axis, which is 0-based (the first action taken or the first reward
received in an episode has t=0). #TODO: observations are different, the initial obs has t=-1, the first observation seen after action0 is obs0: We should probably change this as it breaks RL conventions.

The idea is to allow more flexibility and standardization in how a model can define required
"views" on the ongoing trajectory (during action computations/inference), past episodes (training
on a batch), or even trajectories of other agents in the same episode, some of which
may even use a different policy.  #TODO <- this is not done yet.

Such a "view requirements" formalism is helpful when having to support more complex model
setups like RNNs, attention nets, observation image framestacking (e.g. for Atari),
and building multi-agent communication channels.

The way to define a set of rules used for making Policies and Models see certain
data is through a view requirements dict. Both Policy and Model hold such a dict
and the Policy's view requirements dict usually contains the one of the Model.
These dicts simply map strings (column names), such as "obs" or "actions" to
a ViewRequirement object, which defines the exact conditions by which this column
should be populated with data.

The ViewRequirements class
~~~~~~~~~~~~~~~~~~~~~~~~~~

View requirements for Models (`Model.forward`) and Policies
(`Policy.compute_actions_from_input_dict` and `Policy.learn_on_batch`) are stored
within the `view_requirements` properties of the `ModelV2` and `Policy` base classes:

#TODO: rename model.inference_view_requirements into simply `view_requirements`.

You can acccess these properties like this:

.. code-block:: python

    my_simple_model = ModelV2(...)
    print(my_simple_model.view_requirements)
    >>>{"obs": ViewRequirement(shift=0, space=[observation space])}

    my_lstm_model = LSTMModel(...)
    print(my_lstm_model.view_requirements)
    >>>{
    >>>    "obs": ViewRequirement(shift=0, space=[observation space]),
    >>>    "prev_actions": ViewRequirement(shift=-1, data_col="actions", space=[action space]),
    >>>    "prev_rewards": ViewRequirement(shift=-1, data_col="rewards"),
    >>>}

The `view_requirements` properties hold a dictionary mapping
string keys (e.g. "actions", "rewards", "next_obs", etc..)
to a ViewRequirement object. This ViewRequirement object determines what exact data to
provide under that key in case a SampleBatch or an input_dict needs to be build and fed into
one of the above ModelV2- or Policy methods.

Here is a description of the constructor-settable properties of a ViewRequirement object and
what each of these controls.

.. code-block:: bash
    data_col: An optional string key referencing the underlying data to use to
        create the view. If not provided, assumes that there is data under the
        dict-key under which this ViewRequirement resides.
        Examples:
        Policy.view_requirements = {"rewards": ViewRequirements(shift=0)}
            implies that the underlying data to use are the collected rewards from the environment.
        Policy.view_requirements = {"prev_rewards": ViewRequirements(data_col="rewards", shift=-1)}
            means that the actual data used to create the "prev_rewards" column is the "rewards" data
            from the environment (shifted by 1 timestep).

    space: An optional gym.Space used as a hint for the SampleCollector to know,
        how to fill timesteps before the episode actually started (e.g. if
        shift=-2, we need dummy data at timesteps -2 and -1).

    shift: An int, a list of ints, or a range string (e.g. "-50:-1") to indicate
        which time offsets or ranges of the underlying data to use for the view.
        Examples:
        shift=0 -> Use the data under `data_col` as is.
        shift=1 -> Use the data under `data_col`, but shifted by +1 timestep (used by e.g.
            next_obs views).
        shift=-1 -> Use the data under `data_col`, but shifted by -1 timestep (used by e.g.
            prev_actions views).
        shift=[-2, -1] -> Use the data under `data_col`, but always provide 2 values at each timestep:
            the previous one and the current one. Could be used e.g. to feed the last two actions or
            rewards into an LSTM.
        shift="-50:-1" -> Use the data under `data_col`, but always provide a range of the last 50 timesteps
