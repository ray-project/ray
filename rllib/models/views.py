from collections import namedtuple
import numpy as np
"""
Single column view requirement specified by a Model
(`ModelV2.get_view_requirements()`) to tell the caller, which columns and
timesteps it needs to perform a forward pass.
"""
ViewRequirement = namedtuple(
    "ViewRequirement",
    [
        # The data column name from the SampleBatch (str key).
        "col",

        # List of relative (or absolute timesteps) to be present in the
        # input_dict.
        "timesteps",

        # The name under which the specified view of `col` will be accessible
        # in the input_dict. Default: None (use `col`).
        # TODO: (sven)
        # "name",

        # Switch on absolute timestep mode. Default: False.
        # TODO: (sven)
        # "absolute_timesteps",
        # The fill mode in case t<0 or t>H: One of "zeros", "tile".
        "fill_mode",
        # The repeat-mode (one of "all" or "only_first"). E.g. for training,
        # we only want the first internal state timestep (the NN will
        # calculate all others again anyways).
        "repeat_mode",

        # Provide all data as time major (default: False).
        # TODO: (sven)
        # "time_major",
    ],
    defaults=[
        0,  # ts
        # None,  # name
        # False,  # absolute?
        "zeros",  # fill mode
        "all",  # repeat mode
        # False,  # time-major
    ])


def get_view(model, trajectories, is_training=False):
    """Generates a View for calling the given Model based on its requirements.

    Args:
        model (ModelV2): The ModelV2 for which the View should be generated
            from `data`.
        trajectories (List[dict]): The data dict (keys=column names (str);
            values=raw data (Tensors or np.ndarrays)).
        is_training (bool): Whether the view should be generated for training
            purposes or inference (default).

    Returns:
        Dict[str,any]: The ready-to-be-passed data dict for the Model call.
    """
    view_requirements = model.get_view_requirements(is_training=is_training)
    view = {}
    for vr in view_requirements:
        # Create the batch of data from the different buffers in `data`.
        # TODO: (sven): Here, we actually do create a copy of the data (from a
        #   list). The only way to avoid this entirely would be to keep a
        #   single(!) np buffer per column across all currently ongoing
        #   agents + episodes (which seems very hard to realize).
        view[vr.col] = np.array(
            [t.buffers[vr.col][t.cursor + vr.timesteps] for t in trajectories])
    return view
