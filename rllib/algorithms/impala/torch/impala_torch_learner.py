import contextlib
from typing import Dict

from ray.rllib.algorithms.impala.impala import IMPALAConfig
from ray.rllib.algorithms.impala.impala_learner import IMPALALearner
from ray.rllib.algorithms.impala.torch.vtrace_torch_v2 import (
    make_time_major,
    vtrace_torch,
)
from ray.rllib.core.columns import Columns
from ray.rllib.core.learner.learner import ENTROPY_KEY
from ray.rllib.core.learner.torch.torch_learner import TorchLearner
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import ModuleID, ParamDict, TensorType

torch, nn = try_import_torch()


class IMPALATorchLearner(IMPALALearner, TorchLearner):
    """Implements the IMPALA loss function in torch."""

    @override(TorchLearner)
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        config: IMPALAConfig,
        batch: Dict,
        fwd_out: Dict[str, TensorType],
    ) -> TensorType:
        module = self.module[module_id].unwrapped()

        # TODO (sven): Now that we do the +1ts trick to be less vulnerable about
        #  bootstrap values at the end of rollouts in the new stack, we might make
        #  this a more flexible, configurable parameter for users, e.g.
        #  `v_trace_seq_len` (independent of `rollout_fragment_length`). Separation
        #  of concerns (sampling vs learning).
        rollout_frag_or_episode_len = config.get_rollout_fragment_length()
        recurrent_seq_len = batch.get("seq_lens")

        loss_mask = batch[Columns.LOSS_MASK].float()
        loss_mask_time_major = make_time_major(
            loss_mask,
            trajectory_len=rollout_frag_or_episode_len,
            recurrent_seq_len=recurrent_seq_len,
        )
        size_loss_mask = torch.sum(loss_mask)

        # Behavior actions logp and target actions logp.
        behaviour_actions_logp = batch[Columns.ACTION_LOGP]
        target_policy_dist = module.get_train_action_dist_cls().from_logits(
            fwd_out[Columns.ACTION_DIST_INPUTS]
        )
        target_actions_logp = target_policy_dist.logp(batch[Columns.ACTIONS])

        # Values and bootstrap values.
        values = module.compute_values(
            batch, embeddings=fwd_out.get(Columns.EMBEDDINGS)
        )
        values_time_major = make_time_major(
            values,
            trajectory_len=rollout_frag_or_episode_len,
            recurrent_seq_len=recurrent_seq_len,
        )
        assert Columns.VALUES_BOOTSTRAPPED not in batch
        # Use as bootstrap values the vf-preds in the next "batch row", except
        # for the very last row (which doesn't have a next row), for which the
        # bootstrap value does not matter b/c it has a +1ts value at its end
        # anyways. So we chose an arbitrary item (for simplicity of not having to
        # move new data to the device).
        bootstrap_values = torch.cat(
            [
                values_time_major[0][1:],  # 0th ts values from "next row"
                values_time_major[0][0:1],  # <- can use any arbitrary value here
            ],
            dim=0,
        )

        # TODO(Artur): In the old impala code, actions were unsqueezed if they were
        #  multi_discrete. Find out why and if we need to do the same here.
        #  actions = actions if is_multidiscrete else torch.unsqueeze(actions, dim=1)
        target_actions_logp_time_major = make_time_major(
            target_actions_logp,
            trajectory_len=rollout_frag_or_episode_len,
            recurrent_seq_len=recurrent_seq_len,
        )
        behaviour_actions_logp_time_major = make_time_major(
            behaviour_actions_logp,
            trajectory_len=rollout_frag_or_episode_len,
            recurrent_seq_len=recurrent_seq_len,
        )
        rewards_time_major = make_time_major(
            batch[Columns.REWARDS],
            trajectory_len=rollout_frag_or_episode_len,
            recurrent_seq_len=recurrent_seq_len,
        )

        # the discount factor that is used should be gamma except for timesteps where
        # the episode is terminated. In that case, the discount factor should be 0.
        discounts_time_major = (
            1.0
            - make_time_major(
                batch[Columns.TERMINATEDS],
                trajectory_len=rollout_frag_or_episode_len,
                recurrent_seq_len=recurrent_seq_len,
            ).type(dtype=torch.float32)
        ) * config.gamma

        # Note that vtrace will compute the main loop on the CPU for better performance.
        vtrace_adjusted_target_values, pg_advantages = vtrace_torch(
            target_action_log_probs=target_actions_logp_time_major,
            behaviour_action_log_probs=behaviour_actions_logp_time_major,
            discounts=discounts_time_major,
            rewards=rewards_time_major,
            values=values_time_major,
            bootstrap_values=bootstrap_values,
            clip_rho_threshold=config.vtrace_clip_rho_threshold,
            clip_pg_rho_threshold=config.vtrace_clip_pg_rho_threshold,
        )

        # The policy gradients loss.
        pi_loss = -torch.sum(
            target_actions_logp_time_major * pg_advantages * loss_mask_time_major
        )
        mean_pi_loss = pi_loss / size_loss_mask

        # The baseline loss.
        delta = values_time_major - vtrace_adjusted_target_values
        vf_loss = 0.5 * torch.sum(torch.pow(delta, 2.0) * loss_mask_time_major)
        mean_vf_loss = vf_loss / size_loss_mask

        # The entropy loss.
        entropy_loss = -torch.sum(target_policy_dist.entropy() * loss_mask)
        mean_entropy_loss = entropy_loss / size_loss_mask

        # The summed weighted loss.
        total_loss = (
            mean_pi_loss
            + mean_vf_loss * config.vf_loss_coeff
            + (
                mean_entropy_loss
                * self.entropy_coeff_schedulers_per_module[
                    module_id
                ].get_current_value()
            )
        )

        # Log important loss stats.
        self.metrics.log_dict(
            {
                "pi_loss": pi_loss,
                "mean_pi_loss": mean_pi_loss,
                "vf_loss": vf_loss,
                "mean_vf_loss": mean_vf_loss,
                ENTROPY_KEY: -mean_entropy_loss,
            },
            key=module_id,
            window=1,  # <- single items (should not be mean/ema-reduced over time).
        )
        # Return the total loss.
        return total_loss

    @override(TorchLearner)
    def _uncompiled_update(
        self,
        batch: Dict,
        **kwargs,
    ):
        """Performs a single update given a batch of data.

        This override is critical to prevent DDP (DistributedDataParallel)
        deadlocks caused by the unique properties of APPO's asynchronous,
        multi-agent data pipeline.

        **The Problem: Asymmetric Graph Deadlock**
        1.  APPO's asynchronous `EnvRunners` send data to each Learner (DDP rank)
            independently.
        2.  This means that ranks will receive sometimes **asymmetric batches**
            (e.g., Rank 0 gets `{'p0', 'p1'}`, while Rank 1 gets just `{'p0'}`).
        3.  The default DDP update path (using automatic, hook-based
            synchronization) fails in this scenario. When `backward()` is
            called, Rank 1 has no computation graph for `p1`'s parameters,
            so its DDP hooks for `p1` never fire.
        4.  Rank 0, which *does* have a graph for `p1`, waits forever for
            `p1` gradients from Rank 1, causing a **permanent deadlock**.

        The solution is manual synchronization. This function replaces DDP's
        fragile, hook-based communication with a robust, manual, three-stage process:

        1.  **Disable Hooks (The `no_sync` context):**
            The *entire* `forward_train`, `compute_losses`, and
            `compute_gradients` (which calls `backward()`) chain is wrapped
            in a `mod.no_sync()` context. This is the most critical step, as
            DDP hooks are attached during the *forward pass*. This correctly
            prevents DDP's automatic communication from firing.

        2.  **Synchronize `backward()` (avoid GPU race condition):**
            A call to `torch.cuda.synchronize()` is added *after*
            `total_loss.backward()` (inside `compute_gradients`). On GPU,
            `backward()` is asynchronous. This `synchronize()` call forces
            the CPU to wait for the GPU to *actually finish* computing the
            gradients before we proceed to the next step. This prevents a
            race condition where we try to `all_reduce` a `.grad` attribute
            that is still `None` because the GPU is lagging.

        3.  **Manual `all_reduce` (zero-padding):**
            After the `no_sync` block, we manually `all_reduce` all
            gradients. To prevent a deadlock here, we iterate over all
            parameters. If `param.grad is None` (which happens on the
            "incomplete" rank for policy `p1`), we zero-pad. This "zero-padding"
            ensures that *all* ranks participate in the `all_reduce` call for
            *all* parameters, making the manual synchronization robust.
        """
        # For single-learner setups, call the super's update.
        if self.config.num_learners < 2 or not self.config.is_multi_agent:
            return super()._uncompiled_update(batch=batch, **kwargs)

        # Compute the off-policyness of the batch.
        self._compute_off_policyness(batch)

        # These must be defined outside the scope of the `with` block
        fwd_out = {}
        loss_per_module = {}
        gradients = {}

        # 1. Enter no_sync() for the forward and backward pass.
        with contextlib.ExitStack() as stack:
            for mod in self.module.values():
                if isinstance(mod, torch.nn.Module):
                    stack.enter_context(mod.no_sync())

            # All Torch DDP-affected computation must be inside to avoid firing
            # the hooks on policy gradients that are only trained on some ranks.
            # 2. Forward pass (now inside no_sync)
            fwd_out = self.module.forward_train(batch)

            # 3. Loss computation (now inside no_sync)
            loss_per_module = self.compute_losses(fwd_out=fwd_out, batch=batch)

            # 4. Compute gradients LOCALLY (backward() is called inside)
            gradients = self.compute_gradients(loss_per_module)

        # 5. Manually All-Reduce gradients (outside no_sync).
        # We iterate over all known parameters (`self._params`). This is important
        # to ensure the `all_reduce`` calls are made on all ranks for all params.
        for param in self._params.values():
            # Is the parameter present on this rank?
            present = 1
            if param.grad is None:
                # Parameter is not present on this rank. Keep track of that
                # for averaging later.
                present = 0
                # This parameter was not used (e.g., p1 on Rank 0).
                # Create a zero-gradient to participate in the all_reduce.
                param.grad = torch.zeros_like(param)
            # Now, all ranks have a valid `param.grad` tensor, i.e. all ranks will call
            # all_reduce. No deadlock.
            torch.distributed.all_reduce(param.grad, op=torch.distributed.ReduceOp.SUM)
            # Scale the gradients accordingly.
            denom = torch.tensor(present, device=param.device, dtype=param.dtype)
            # Receive the number of participating ranks for this param.
            torch.distributed.all_reduce(denom, op=torch.distributed.ReduceOp.SUM)
            # Average the summed gradients.
            param.grad.data.div_(denom.clamp(min=1.0))

        # 6. Collect the gradients for all modules to update the modules synchronously.
        gradients = {pid: p.grad for pid, p in self._params.items()}

        # 7. Postprocess gradients, e.g. clipping.
        postprocessed_gradients = self.postprocess_gradients(gradients)

        # 8. Apply the post-processed gradients to the weigths.
        self.apply_gradients(postprocessed_gradients)

        return fwd_out, loss_per_module, {}

    @override(TorchLearner)
    def compute_gradients(
        self, loss_per_module: Dict[ModuleID, TensorType], **kwargs
    ) -> ParamDict:
        """Computes the gradients by running `backward`.

        This method is a core part of the manual synchronization logic
        in `_uncompiled_update`. It performs the `backward()` pass locally
        without DDP's automatic communication.

        Key implementation details:
        1.**GPU Synchronization:** It includes a `torch.cuda.synchronize()`
            call after `total_loss.backward()`. This is critical for GPU
            training, as `backward()` is asynchronous. This sync
            prevents a race condition where the calling function
            (`_uncompiled_update`) would try to access `param.grad`
            before the GPU has finished computing it (mistakenly reading `None`).
        2.  **Asymmetric Gradients:** On an incomplete batch, parameters for
            missing modules will correctly have a `None` gradient. This is
            expected and handled by the zero-padding logic in
            `_uncompiled_update`'s `all_reduce` loop.
        3. If the loss is zero (i.e., no modules were trained on this rank),
            this method returns an empty dict.
        """
        # If a single learner is used, fall back to the super's method.
        if self.config.num_learners < 2 or not self.config.is_multi_agent:
            return super().compute_gradients(loss_per_module=loss_per_module, **kwargs)

        for optim in self._optimizer_parameters:
            # `set_to_none=True` is a faster way to zero out the gradients.
            optim.zero_grad(set_to_none=True)

        if self._grad_scalers is not None:
            total_loss = sum(
                self._grad_scalers[mid].scale(loss)
                for mid, loss in loss_per_module.items()
            )
        else:
            total_loss = sum(loss_per_module.values())

        # If we don't have any loss computations, `sum` returns 0.
        if isinstance(total_loss, int):
            assert total_loss == 0
            return {}

        # This backward() call is inside no_sync(). It will be a clean,
        # local-only operation.
        total_loss.backward()

        # We must force the CPU to wait for the async `backward()` call to
        # finish on the GPU. Otherwise, the `param.grad is None` check in
        # `_uncompiled_update` will race against the GPU and fail.
        if torch.cuda.is_available():
            torch.cuda.synchronize()

        # This line is now safe. `grads` will have `None` for unused params
        # (e.g., p1 on Rank 0). This is expected and handled by `_uncompiled_update`'s
        # all_reduce loop.
        grads = {pid: p.grad for pid, p in self._params.items()}

        return grads


ImpalaTorchLearner = IMPALATorchLearner
