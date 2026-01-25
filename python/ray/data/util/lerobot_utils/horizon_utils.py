import numpy as np


# Helper function to get the horizon indices
def get_horizon_indices(
    timestamps: np.ndarray, deltas: list[float], ep_start: int, ep_end: int
) -> tuple[np.ndarray, np.ndarray]:
    """Find indices of closest timestamps at each delta offset (vectorized).

    Args:
        timestamps: (N,) sorted array of timestamps
        deltas: list of D time offsets (e.g., [0, 0.1, 0.2, 0.3])
        ep_start: episode start index
        ep_end: episode end index (exclusive)

    Returns:
        indices: (N, D) array where indices[i, j] is the index of the closest
                timestamp to timestamps[i] + deltas[j], clamped to episode bounds
        is_pad: (N, D) boolean array indicating which positions were clamped
    """
    N = len(timestamps)
    deltas_arr = np.array(deltas)

    # Target timestamps: (N, D) via broadcasting
    target_times = timestamps[:, None] + deltas_arr[None, :]

    # Flatten for searchsorted: (N*D,)
    target_flat = target_times.ravel()

    # Find insertion points (right side)
    insert_idx = np.searchsorted(timestamps, target_flat)

    # Clip to valid range [0, N-1]
    insert_idx = np.clip(insert_idx, 0, N - 1)
    prev_idx = np.clip(insert_idx - 1, 0, N - 1)

    # Compute distances to both neighbors
    dist_right = np.abs(timestamps[insert_idx] - target_flat)
    dist_left = np.abs(timestamps[prev_idx] - target_flat)

    # Choose the closer one (unclamped)
    unclamped_idx = np.where(dist_left <= dist_right, prev_idx, insert_idx)

    # Apply episode boundary clamp
    clamped_idx = np.maximum(np.minimum(unclamped_idx, ep_end - 1), ep_start)

    # Compute padding mask: True where clamping occurred
    is_pad = (unclamped_idx != clamped_idx).reshape(N, len(deltas))

    # Reshape back to (N, D)
    return clamped_idx.reshape(N, len(deltas)), is_pad


# Helper function to process the horizon batch
def process_horizon_batch(
    batch, delta_timestamps: dict[str, list[float]], episode_dict: dict
) -> dict:
    """Sort batch by timestamp and build temporal horizons for specified keys."""
    if delta_timestamps is None:
        return batch

    sorted_batch = batch.sort_values(by="timestamp").reset_index(drop=True)
    ep_start = 0
    # Convert numpy.int64 to Python int for HuggingFace dataset indexing
    episode_idx = int(sorted_batch["episode_index"].values[0])
    ep_end = (
        episode_dict[episode_idx]["dataset_to_index"]
        - episode_dict[episode_idx]["dataset_from_index"]
    )
    # Assert timestamps are monotonically increasing after sort
    timestamps = sorted_batch["timestamp"].values
    assert (timestamps != np.nan).all(), "Timestamps are nan"
    assert (
        timestamps[1:] >= timestamps[:-1]
    ).all(), "Timestamps are not sorted in increasing order"

    # Build horizon for each key in delta_timestamps
    for key, deltas in delta_timestamps.items():
        # Get indices of closest timestamps at each delta offset: (N, D)
        # Also get padding mask indicating which positions were clamped
        horizon_indices, is_pad_mask = get_horizon_indices(
            timestamps, deltas, ep_start, ep_end
        )

        # Get the values for this key
        values = sorted_batch[key].values  # (N,) or (N, feature_dim) depending on dtype
        if isinstance(values[0], (list, np.ndarray)):
            # Stack into proper array first: (N, feature_dim)
            values_arr = np.stack(values)
            # Gather: (N, D, feature_dim)
            horizon_values = values_arr[horizon_indices]
        else:
            # Scalar values: (N,) -> (N, D)
            horizon_values = values[horizon_indices]
        # print(f"horizon_values: {horizon_values.shape}")
        # Replace the original column with horizoned values (each row is now a horizon)
        sorted_batch[key] = list(horizon_values)
        # Add padding mask column
        sorted_batch[key + "_is_pad"] = list(is_pad_mask)

    return sorted_batch


if __name__ == "__main__":
    indices, is_pad = get_horizon_indices(
        np.array([1, 2, 3, 4, 5]), [0.1, 0.2], ep_start=0, ep_end=5
    )
    print("Indices:\n", indices)
    print("Is Pad:\n", is_pad)
