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
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.core.columns import Columns
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.metrics import (
    LEARNER_RESULTS,
    REPLAY_BUFFER_RESULTS,
)
from ray.rllib.utils.torch_utils import inverse_symlog

torch, _ = try_import_torch()


def reconstruct_obs_from_h_and_z(
    h_t0_to_H,
    z_t0_to_H,
    dreamer_model,
    obs_dims_shape,
    framework="torch",
):
    """Returns"""
    shape = h_t0_to_H.shape
    T = shape[0]  # inputs are time-major
    B = shape[1]
    # Compute actual observations using h and z and the decoder net.
    # Note that the last h-state (T+1) is NOT used here as it's already part of
    # a new trajectory.
    # Use mean() of the Gaussian, no sample! -> No need to construct dist object here.
    if framework == "torch":
        device = next(iter(dreamer_model.world_model.decoder.parameters())).device
        reconstructed_obs_distr_means_TxB = (
            dreamer_model.world_model.decoder(
                # Fold time rank.
                h=torch.from_numpy(h_t0_to_H).reshape((T * B, -1)).to(device),
                z=torch.from_numpy(z_t0_to_H)
                .reshape((T * B,) + z_t0_to_H.shape[2:])
                .to(device),
            )
            .detach()
            .cpu()
            .numpy()
        )
    else:
        reconstructed_obs_distr_means_TxB = dreamer_model.world_model.decoder(
            # Fold time rank.
            h=h_t0_to_H.reshape((T * B, -1)),
            z=z_t0_to_H.reshape((T * B,) + z_t0_to_H.shape[2:]),
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
    framework="torch",
):
    if not include_images:
        return

    dream_data = results["dream_data"]
    dreamed_obs_H_B = reconstruct_obs_from_h_and_z(
        h_t0_to_H=dream_data["h_states_t0_to_H_BxT"],
        z_t0_to_H=dream_data["z_states_prior_t0_to_H_BxT"],
        dreamer_model=dreamer_model,
        obs_dims_shape=obs_dims_shape,
        framework=framework,
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
    metrics,
    sample,
    batch_size_B,
    batch_length_T,
    symlog_obs: bool = True,
    do_report: bool = True,
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
        metrics: The MetricsLogger object of the DreamerV3 algo.
        sample: The sampled data (dict) from the replay buffer. Already torch-tensor
            converted.
        batch_size_B: The batch size (B). This is the number of trajectories sampled
            from the buffer.
        batch_length_T: The batch length (T). This is the length of an individual
            trajectory sampled from the buffer.
        do_report: Whether to actually log the report (default). If this is set to
            False, this function serves as a clean-up on the given metrics, making sure
            they do NOT contain anymore any (spacious) data relevant for producing
            the report/videos.
    """
    fwd_output_key = (
        LEARNER_RESULTS,
        DEFAULT_MODULE_ID,
        "WORLD_MODEL_fwd_out_obs_distribution_means_b0xT",
    )
    # logged as a non-reduced item (still a list)
    predicted_observation_means_single_example = metrics.peek(
        fwd_output_key, default=[None]
    )[-1]
    metrics.delete(fwd_output_key, key_error=False)

    final_result_key = (
        f"WORLD_MODEL_sampled_vs_predicted_posterior_b0x{batch_length_T}_videos"
    )
    if not do_report:
        metrics.delete(final_result_key, key_error=False)
        return

    _report_obs(
        metrics=metrics,
        computed_float_obs_B_T_dims=np.reshape(
            predicted_observation_means_single_example,
            # WandB videos need to be channels first.
            (1, batch_length_T) + sample[Columns.OBS].shape[2:],
        ),
        sampled_obs_B_T_dims=sample[Columns.OBS][0:1],
        metrics_key=final_result_key,
        symlog_obs=symlog_obs,
    )


def report_dreamed_eval_trajectory_vs_samples(
    *,
    metrics,
    sample,
    burn_in_T,
    dreamed_T,
    dreamer_model,
    symlog_obs: bool = True,
    do_report: bool = True,
    framework="torch",
) -> None:
    """Logs dreamed observations, rewards, continues and compares them vs sampled data.

    For obs, we'll try to create videos (side-by-side comparison) of the dreamed,
    recreated-from-prior obs vs the sampled ones (over dreamed_T timesteps).

    Args:
        metrics: The MetricsLogger object of the DreamerV3 algo.
        sample: The sampled data (dict) from the replay buffer. Already torch-tensor
            converted.
        burn_in_T: The number of burn-in timesteps (these will be skipped over in the
            reported video comparisons and MSEs).
        dreamed_T: The number of timesteps to produce dreamed data for.
        dreamer_model: The DreamerModel to use to create observation vectors/images
            from dreamed h- and (prior) z-states.
        symlog_obs: Whether to inverse-symlog the computed observations or not. Set this
            to True for environments, in which we should symlog the observations.
        do_report: Whether to actually log the report (default). If this is set to
            False, this function serves as a clean-up on the given metrics, making sure
            they do NOT contain anymore any (spacious) data relevant for producing
            the report/videos.
    """
    dream_data = metrics.peek(
        (LEARNER_RESULTS, DEFAULT_MODULE_ID, "dream_data"),
        default={},
    )
    metrics.delete(LEARNER_RESULTS, DEFAULT_MODULE_ID, "dream_data", key_error=False)

    final_result_key_obs = f"EVALUATION_sampled_vs_dreamed_prior_H{dreamed_T}_obs"
    final_result_key_rew = (
        f"EVALUATION_sampled_vs_dreamed_prior_H{dreamed_T}_rewards_MSE"
    )
    final_result_key_cont = (
        f"EVALUATION_sampled_vs_dreamed_prior_H{dreamed_T}_continues_MSE"
    )
    if not do_report:
        metrics.delete(final_result_key_obs, key_error=False)
        metrics.delete(final_result_key_rew, key_error=False)
        metrics.delete(final_result_key_cont, key_error=False)
        return

    # Obs MSE.
    dreamed_obs_H_B = reconstruct_obs_from_h_and_z(
        h_t0_to_H=dream_data["h_states_t0_to_H_Bx1"][0],  # [0] b/c reduce=None (list)
        z_t0_to_H=dream_data["z_states_prior_t0_to_H_Bx1"][0],
        dreamer_model=dreamer_model,
        obs_dims_shape=sample[Columns.OBS].shape[2:],
        framework=framework,
    )
    t0 = burn_in_T
    tH = t0 + dreamed_T
    # Observation MSE and - if applicable - images comparisons.
    _report_obs(
        metrics=metrics,
        # WandB videos need to be 5D (B, L, c, h, w) -> transpose/swap H and B axes.
        computed_float_obs_B_T_dims=np.swapaxes(dreamed_obs_H_B, 0, 1)[
            0:1
        ],  # for now: only B=1
        sampled_obs_B_T_dims=sample[Columns.OBS][0:1, t0:tH],
        metrics_key=final_result_key_obs,
        symlog_obs=symlog_obs,
    )

    # Reward MSE.
    _report_rewards(
        metrics=metrics,
        computed_rewards=dream_data["rewards_dreamed_t0_to_H_Bx1"][0],
        sampled_rewards=sample[Columns.REWARDS][:, t0:tH],
        metrics_key=final_result_key_rew,
    )

    # Continues MSE.
    _report_continues(
        metrics=metrics,
        computed_continues=dream_data["continues_dreamed_t0_to_H_Bx1"][0],
        sampled_continues=(1.0 - sample["is_terminated"])[:, t0:tH],
        metrics_key=final_result_key_cont,
    )


def report_sampling_and_replay_buffer(*, metrics, replay_buffer):
    episodes_in_buffer = replay_buffer.get_num_episodes()
    ts_in_buffer = replay_buffer.get_num_timesteps()
    replayed_steps = replay_buffer.get_sampled_timesteps()
    added_steps = replay_buffer.get_added_timesteps()

    # Summarize buffer, sampling, and train ratio stats.
    metrics.log_dict(
        {
            "capacity": replay_buffer.capacity,
            "size_num_episodes": episodes_in_buffer,
            "size_timesteps": ts_in_buffer,
            "replayed_steps": replayed_steps,
            "added_steps": added_steps,
        },
        key=REPLAY_BUFFER_RESULTS,
        window=1,
    )  # window=1 b/c these are current (total count/state) values.


def _report_obs(
    *,
    metrics,
    computed_float_obs_B_T_dims,
    sampled_obs_B_T_dims,
    metrics_key,
    symlog_obs,
):
    """Summarizes computed- vs sampled observations: MSE and (if applicable) images.

    Args:
        metrics: The MetricsLogger object of the DreamerV3 algo.
        computed_float_obs_B_T_dims: Computed float observations
            (not clipped, not cast'd). Shape=(B, T, [dims ...]).
        sampled_obs_B_T_dims: Sampled observations (as-is from the environment, meaning
            this could be uint8, 0-255 clipped images). Shape=(B, T, [dims ...]).
        metrics_key: The metrics key (or key sequence) under which to log ths resulting
            video sequence.
        symlog_obs: Whether to inverse-symlog the computed observations or not. Set this
            to True for environments, in which we should symlog the observations.
    """
    # Videos: Create summary, comparing computed images with actual sampled ones.
    # 4=[B, T, w, h] grayscale image; 5=[B, T, w, h, C] RGB image.
    if len(sampled_obs_B_T_dims.shape) in [4, 5]:
        # WandB videos need to be channels first.
        transpose_axes = (
            (0, 1, 4, 2, 3) if len(sampled_obs_B_T_dims.shape) == 5 else (0, 3, 1, 2)
        )

        if symlog_obs:
            computed_float_obs_B_T_dims = inverse_symlog(computed_float_obs_B_T_dims)

        # Restore image pixels from normalized (non-symlog'd) data.
        if not symlog_obs:
            computed_float_obs_B_T_dims = (computed_float_obs_B_T_dims + 1.0) * 128
            sampled_obs_B_T_dims = (sampled_obs_B_T_dims + 1.0) * 128
            sampled_obs_B_T_dims = np.clip(sampled_obs_B_T_dims, 0.0, 255.0).astype(
                np.uint8
            )
            sampled_obs_B_T_dims = np.transpose(sampled_obs_B_T_dims, transpose_axes)
        computed_images = np.clip(computed_float_obs_B_T_dims, 0.0, 255.0).astype(
            np.uint8
        )
        computed_images = np.transpose(computed_images, transpose_axes)
        # Concat sampled and computed images along the height axis (3) such that
        # real images show below respective predicted ones.
        # (B, T, C, h, w)
        sampled_vs_computed_images = np.concatenate(
            [computed_images, sampled_obs_B_T_dims],
            axis=-1,  # concat on width axis (looks nicer)
        )
        # Add grayscale dim, if necessary.
        if len(sampled_obs_B_T_dims.shape) == 2 + 2:
            sampled_vs_computed_images = np.expand_dims(sampled_vs_computed_images, -1)

        metrics.log_value(
            metrics_key,
            sampled_vs_computed_images,
            reduce=None,  # No reduction, we want the obs tensor to stay in-tact.
            window=1,
        )


def _report_rewards(
    *,
    metrics,
    computed_rewards,
    sampled_rewards,
    metrics_key,
):
    mse_sampled_vs_computed_rewards = np.mean(
        np.square(computed_rewards - sampled_rewards)
    )
    mse_sampled_vs_computed_rewards = np.mean(mse_sampled_vs_computed_rewards)
    metrics.log_value(
        metrics_key,
        mse_sampled_vs_computed_rewards,
        window=1,
    )


def _report_continues(
    *,
    metrics,
    computed_continues,
    sampled_continues,
    metrics_key,
):
    # Continue MSE.
    mse_sampled_vs_computed_continues = np.mean(
        np.square(
            computed_continues - sampled_continues.astype(computed_continues.dtype)
        )
    )
    metrics.log_value(
        metrics_key,
        mse_sampled_vs_computed_continues,
        window=1,
    )
