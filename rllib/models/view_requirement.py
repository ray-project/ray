from collections import namedtuple


ViewRequirement = namedtuple("ViewRequirement", [
    # The data column name from the SampleBatch (str key).
    "col",
    # List of relative (or absolute timesteps) to be present in the input_dict.
    "timesteps",
    # The name under which the specified view of `col` will be accessible in
    # the input_dict. Default: None (use `col`).
    "name",
    # Switch on absolute timestep mode. Default: False.
    "absolute_timesteps",
    # The fill mode in case t<0 or t>H: One of "zeros", "tile".
    "fill_mode",
    # The repeat-mode (one of "all" or "only_first"). E.g. for training,
    # we only want the first internal state timestep (the NN will
    # calculate all others again anyways).
    "repeat_mode",
    # Provide all data as time major (default: False).
    "time_major",
    ], defaults=[
        0,  # ts
        None,  # name
        False,  # absolute?
        "zeros",  # fill mode
        "all",  # repeat mode
        False,  # time-major
    ])
