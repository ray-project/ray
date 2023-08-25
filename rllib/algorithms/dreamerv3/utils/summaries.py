"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf

[2] Mastering Atari with Discrete World Models - 2021
D. Hafner, T. Lillicrap, M. Norouzi, J. Ba
https://arxiv.org/pdf/2010.02193.pdf
"""
import numpy as np

from ray.rllib.algorithms.dreamerv3.utils.debugging import (
    create_cartpole_dream_image,
    create_frozenlake_dream_image,
)
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.tf_utils import inverse_symlog


def _summarize(*, results, data_to_summarize, keys_to_log, include_histograms=False):
    for k in keys_to_log:
        if data_to_summarize[k].shape == ():
            results.update({k: data_to_summarize[k]})
        elif include_histograms:
            results.update({k: data_to_summarize[k]})


def reconstruct_obs_from_h_and_z(
    h_t0_to_H,
    z_t0_to_H,
    dreamer_model,
    obs_dims_shape,
):
    """Returns"""
    shape = h_t0_to_H.shape
    T = shape[0]  # inputs are time-major
    B = shape[1]
    # Compute actual observations using h and z and the decoder net.
    # Note that the last h-state (T+1) is NOT used here as it's already part of
    # a new trajectory.
    # Use mean() of the Gaussian, no sample! -> No need to construct dist object here.
    reconstructed_obs_distr_means_TxB = dreamer_model.world_model.decoder(
        # Fold time rank.
        h=np.reshape(h_t0_to_H, (T * B, -1)),
        z=np.reshape(z_t0_to_H, (T * B,) + z_t0_to_H.shape[2:]),
    )
    # Unfold time rank again.
    reconstructed_obs_T_B = np.reshape(
        reconstructed_obs_distr_means_TxB, (T, B) + obs_dims_shape
    )
    # Return inverse symlog'd (real env obs space) reconstructed observations.
    return reconstructed_obs_T_B


def report_dreamed_trajectory(
    *,
    results,
    env,
    dreamer_model,
    obs_dims_shape,
    batch_indices=(0,),
    desc=None,
    include_images=True,
):
    if not include_images:
        return

    dream_data = results["dream_data"]
    dreamed_obs_H_B = reconstruct_obs_from_h_and_z(
        h_t0_to_H=dream_data["h_states_t0_to_H_BxT"],
        z_t0_to_H=dream_data["z_states_prior_t0_to_H_BxT"],
        dreamer_model=dreamer_model,
        obs_dims_shape=obs_dims_shape,
    )
    func = (
        create_cartpole_dream_image
        if env.startswith("CartPole")
        else create_frozenlake_dream_image
    )
    # Take 0th dreamed trajectory and produce series of images.
    for b in batch_indices:
        images = []
        for t in range(len(dreamed_obs_H_B) - 1):
            images.append(
                func(
                    dreamed_obs=dreamed_obs_H_B[t][b],
                    dreamed_V=dream_data["values_dreamed_t0_to_H_BxT"][t][b],
                    dreamed_a=(dream_data["actions_ints_dreamed_t0_to_H_BxT"][t][b]),
                    dreamed_r_tp1=(dream_data["rewards_dreamed_t0_to_H_BxT"][t + 1][b]),
                    # `DISAGREE_intrinsic_rewards_H_B` are shifted by 1 already
                    # (from t1 to H, not t0 to H like all other data here).
                    dreamed_ri_tp1=(
                        results["DISAGREE_intrinsic_rewards_H_BxT"][t][b]
                        if "DISAGREE_intrinsic_rewards_H_BxT" in results
                        else None
                    ),
                    dreamed_c_tp1=(
                        dream_data["continues_dreamed_t0_to_H_BxT"][t + 1][b]
                    ),
                    value_target=results["VALUE_TARGETS_H_BxT"][t][b],
                    initial_h=dream_data["h_states_t0_to_H_BxT"][t][b],
                    as_tensor=True,
                ).numpy()
            )
        # Concat images along width-axis (so they show as a "film sequence" next to each
        # other).
        results.update(
            {
                f"dreamed_trajectories{('_'+desc) if desc else ''}_B{b}": (
                    np.concatenate(images, axis=1)
                ),
            }
        )


def report_predicted_vs_sampled_obs(
    *,
    results,
    sample,
    batch_size_B,
    batch_length_T,
    symlog_obs: bool = True,
):
    """Summarizes sampled data (from the replay buffer) vs world-model predictions.

    World model predictions are based on the posterior states (z computed from actual
    observation encoder input + the current h-states).

    Observations: Computes MSE (sampled vs predicted/recreated) over all features.
    For image observations, also creates direct image comparisons (sampled images
    vs predicted (posterior) ones).
    Rewards: Compute MSE (sampled vs predicted).
    Continues: Compute MSE (sampled vs predicted).

    Args:
        results: The results dict that was returned by `LearnerGroup.update()`.
        sample: The sampled data (dict) from the replay buffer. Already tf-tensor
            converted.
        batch_size_B: The batch size (B). This is the number of trajectories sampled
            from the buffer.
        batch_length_T: The batch length (T). This is the length of an individual
            trajectory sampled from the buffer.
    """
    predicted_observation_means_BxT = results[
        "WORLD_MODEL_fwd_out_obs_distribution_means_BxT"
    ]
    _report_obs(
        results=results,
        computed_float_obs_B_T_dims=np.reshape(
            predicted_observation_means_BxT,
            (batch_size_B, batch_length_T) + sample[SampleBatch.OBS].shape[2:],
        ),
        sampled_obs_B_T_dims=sample[SampleBatch.OBS],
        descr_prefix="WORLD_MODEL",
        descr_obs=f"predicted_posterior_T{batch_length_T}",
        symlog_obs=symlog_obs,
    )


def report_dreamed_eval_trajectory_vs_samples(
    *,
    results,
    dream_data,
    sample,
    burn_in_T,
    dreamed_T,
    dreamer_model,
    symlog_obs: bool = True,
):
    # Obs MSE.
    dreamed_obs_T_B = reconstruct_obs_from_h_and_z(
        h_t0_to_H=dream_data["h_states_t0_to_H_BxT"],
        z_t0_to_H=dream_data["z_states_prior_t0_to_H_BxT"],
        dreamer_model=dreamer_model,
        obs_dims_shape=sample[SampleBatch.OBS].shape[2:],
    )
    t0 = burn_in_T - 1
    tH = t0 + dreamed_T
    # Observation MSE and - if applicable - images comparisons.
    mse_sampled_vs_dreamed_obs = _report_obs(
        results=results,
        # Have to transpose b/c dreamed data is time-major.
        computed_float_obs_B_T_dims=np.transpose(
            dreamed_obs_T_B,
            axes=[1, 0] + list(range(2, len(dreamed_obs_T_B.shape))),
        ),
        sampled_obs_B_T_dims=sample[SampleBatch.OBS][:, t0 : tH + 1],
        descr_prefix="EVALUATION",
        descr_obs=f"dreamed_prior_H{dreamed_T}",
        symlog_obs=symlog_obs,
    )

    # Reward MSE.
    _report_rewards(
        results=results,
        computed_rewards=dream_data["rewards_dreamed_t0_to_H_BxT"],
        sampled_rewards=sample[SampleBatch.REWARDS][:, t0 : tH + 1],
        descr_prefix="EVALUATION",
        descr_reward=f"dreamed_prior_H{dreamed_T}",
    )

    # Continues MSE.
    _report_continues(
        results=results,
        computed_continues=dream_data["continues_dreamed_t0_to_H_BxT"],
        sampled_continues=(1.0 - sample["is_terminated"])[:, t0 : tH + 1],
        descr_prefix="EVALUATION",
        descr_cont=f"dreamed_prior_H{dreamed_T}",
    )
    return mse_sampled_vs_dreamed_obs


def report_sampling_and_replay_buffer(*, replay_buffer):
    episodes_in_buffer = replay_buffer.get_num_episodes()
    ts_in_buffer = replay_buffer.get_num_timesteps()
    replayed_steps = replay_buffer.get_sampled_timesteps()
    added_steps = replay_buffer.get_added_timesteps()

    # Summarize buffer, sampling, and train ratio stats.
    return {
        "BUFFER_capacity": replay_buffer.capacity,
        "BUFFER_size_num_episodes": episodes_in_buffer,
        "BUFFER_size_timesteps": ts_in_buffer,
        "BUFFER_replayed_steps": replayed_steps,
        "BUFFER_added_steps": added_steps,
    }


def _report_obs(
    *,
    results,
    computed_float_obs_B_T_dims,
    sampled_obs_B_T_dims,
    descr_prefix=None,
    descr_obs,
    symlog_obs,
):
    """Summarizes computed- vs sampled observations: MSE and (if applicable) images.

    Args:
        computed_float_obs_B_T_dims: Computed float observations
            (not clipped, not cast'd). Shape=(B, T, [dims ...]).
        sampled_obs_B_T_dims: Sampled observations (as-is from the environment, meaning
            this could be uint8, 0-255 clipped images). Shape=(B, T, [dims ...]).
        B: The batch size B (see shapes of `computed_float_obs_B_T_dims` and
            `sampled_obs_B_T_dims` above).
        T: The batch length T (see shapes of `computed_float_obs_B_T_dims` and
            `sampled_obs_B_T_dims` above).
        descr: A string used to describe the computed data to be used in the TB
            summaries.
    """
    # Videos: Create summary, comparing computed images with actual sampled ones.
    # 4=[B, T, w, h] grayscale image; 5=[B, T, w, h, C] RGB image.
    if len(sampled_obs_B_T_dims.shape) in [4, 5]:
        descr_prefix = (descr_prefix + "_") if descr_prefix else ""

        if symlog_obs:
            computed_float_obs_B_T_dims = inverse_symlog(computed_float_obs_B_T_dims)

        # Restore image pixels from normalized (non-symlog'd) data.
        if not symlog_obs:
            computed_float_obs_B_T_dims = (computed_float_obs_B_T_dims + 1.0) * 128
            sampled_obs_B_T_dims = (sampled_obs_B_T_dims + 1.0) * 128
            sampled_obs_B_T_dims = np.clip(sampled_obs_B_T_dims, 0.0, 255.0).astype(
                np.uint8
            )
        computed_images = np.clip(computed_float_obs_B_T_dims, 0.0, 255.0).astype(
            np.uint8
        )
        # Concat sampled and computed images along the height axis (3) such that
        # real images show below respective predicted ones.
        # (B, T, C, h, w)
        sampled_vs_computed_images = np.concatenate(
            [computed_images, sampled_obs_B_T_dims],
            axis=3,
        )
        # Add grayscale dim, if necessary.
        if len(sampled_obs_B_T_dims.shape) == 2 + 2:
            sampled_vs_computed_images = np.expand_dims(sampled_vs_computed_images, -1)

        results.update(
            {f"{descr_prefix}sampled_vs_{descr_obs}_videos": sampled_vs_computed_images}
        )

    # return mse_sampled_vs_computed_obs


def _report_rewards(
    *,
    results,
    computed_rewards,
    sampled_rewards,
    descr_prefix=None,
    descr_reward,
):
    descr_prefix = (descr_prefix + "_") if descr_prefix else ""
    mse_sampled_vs_computed_rewards = np.mean(
        np.square(computed_rewards - sampled_rewards)
    )
    mse_sampled_vs_computed_rewards = np.mean(mse_sampled_vs_computed_rewards)
    results.update(
        {
            f"{descr_prefix}sampled_vs_{descr_reward}_rewards_mse": (
                mse_sampled_vs_computed_rewards
            ),
        }
    )


def _report_continues(
    *,
    results,
    computed_continues,
    sampled_continues,
    descr_prefix=None,
    descr_cont,
):
    descr_prefix = (descr_prefix + "_") if descr_prefix else ""
    # Continue MSE.
    mse_sampled_vs_computed_continues = np.mean(
        np.square(
            computed_continues - sampled_continues.astype(computed_continues.dtype)
        )
    )
    results.update(
        {
            f"{descr_prefix}sampled_vs_{descr_cont}_continues_mse": (
                mse_sampled_vs_computed_continues
            ),
        }
    )
