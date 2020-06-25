import numpy as np


def get_trajectory_view(model, trajectories, is_training=False):
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
    for col, config in view_requirements:
        # Create the batch of data from the different buffers in `data`.
        # TODO: (sven): Here, we actually do create a copy of the data (from a
        #   list). The only way to avoid this entirely would be to keep a
        #   single(!) np buffer per column across all currently ongoing
        #   agents + episodes (which seems very hard to realize).
        view[col] = np.array([
            t.buffers[col][t.cursor + config["timesteps"]]
            for t in trajectories
        ])
    return view
