class InverseDynamicsModelBasedCuriosity:
    """Implementation of:
    [1] Curiosity-driven Exploration by Self-supervised Prediction
    Pathak, Agrawal, Efros, and Darrell - UC Berkeley - ICML 2017.
    https://arxiv.org/pdf/1705.05363.pdf

    Learns a simplified model of the environment based on three networks:
    1) Embedding observations into latent space ("feature" network).
    2) Predicting the action, given two consecutive embedded observations
    ("inverse" network).
    3) Predicting the next embedded obs, given an obs and action
    ("forward" network).

    The less the agent is able to predict the actually observed next feature
    vector, given obs and action (through the forwards network), the larger the
    "intrinsic reward", which will be added to the extrinsic reward.
    Therefore, if a state transition was unexpected, the agent becomes
    "curious" and will further explore this transition leading to better
    exploration in sparse rewards environments.
    """

    def __init__(self):


    def forward(self, batch: Dict[str, Any]) -> Dict[str, Any]:
        # Push both observations through feature net to get both phis.
        phis, _ = self.model._curiosity_feature_net(
            {
                SampleBatch.OBS: torch.cat(
                    [
                        torch.from_numpy(sample_batch[SampleBatch.OBS]).to(
                            policy.device
                        ),
                        torch.from_numpy(sample_batch[SampleBatch.NEXT_OBS]).to(
                            policy.device
                        ),
                    ]
                )
            }
        )
        phi, next_phi = torch.chunk(phis, 2)
        actions_tensor = (
            torch.from_numpy(sample_batch[SampleBatch.ACTIONS]).long().to(policy.device)
        )

        # Predict next phi with forward model.
        predicted_next_phi = self.model._curiosity_forward_fcnet(
            torch.cat([phi, one_hot(actions_tensor, self.action_space).float()], dim=-1)
        )

        # Forward loss term (predicted phi', given phi and action vs actually
        # observed phi').
        forward_l2_norm_sqared = 0.5 * torch.sum(
            torch.pow(predicted_next_phi - next_phi, 2.0), dim=-1
        )
        forward_loss = torch.mean(forward_l2_norm_sqared)

        # Scale intrinsic reward by eta hyper-parameter.
        sample_batch[SampleBatch.REWARDS] = (
            sample_batch[SampleBatch.REWARDS]
            + self.eta * forward_l2_norm_sqared.detach().cpu().numpy()
        )

        # Inverse loss term (prediced action that led from phi to phi' vs
        # actual action taken).
        phi_cat_next_phi = torch.cat([phi, next_phi], dim=-1)
        dist_inputs = self.model._curiosity_inverse_fcnet(phi_cat_next_phi)
        action_dist = (
            TorchCategorical(dist_inputs, self.model)
            if isinstance(self.action_space, Discrete)
            else TorchMultiCategorical(dist_inputs, self.model, self.action_space.nvec)
        )
        # Neg log(p); p=probability of observed action given the inverse-NN
        # predicted action distribution.
        inverse_loss = -action_dist.logp(actions_tensor)
        inverse_loss = torch.mean(inverse_loss)

        # Calculate the ICM loss.
        loss = (1.0 - self.beta) * inverse_loss + self.beta * forward_loss
        # Perform an optimizer step.
        self._optimizer.zero_grad()
        loss.backward()
        self._optimizer.step()

        # Return the postprocessed sample batch (with the corrected rewards).
        return sample_batch
