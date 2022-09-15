import flax
import flax.linen as nn
import jax
import jax.numpy as jnp
from PIL import Image
from transformers import CLIPTokenizer, FlaxCLIPTextModel

@flax.struct.dataclass
class InferenceState:
    text_encoder_params: flax.core.FrozenDict
    unet_params: flax.core.FrozenDict
    vae_params: flax.core.FrozenDict


class StableDiffusionPipeline:
    def __init__(self, vae, text_encoder, tokenizer, unet, scheduler):
        scheduler = scheduler.set_format("np")
        self.vae = vae
        self.text_encoder = text_encoder
        self.tokenizer = tokenizer
        self.unet = unet
        self.scheduler = scheduler

    def numpy_to_pil(images):
        """
        Convert a numpy image or a batch of images to a PIL image.
        """
        if images.ndim == 3:
            images = images[None, ...]
        images = (images * 255).round().astype("uint8")
        pil_images = [Image.fromarray(image) for image in images]

        return pil_images

    def sample(
        self,
        input_ids: jnp.ndarray,
        uncond_input_ids: jnp.ndarray,
        prng_seed: jax.random.PRNGKey,
        inference_state: InferenceState,
        num_inference_steps: int = 50,
        guidance_scale: float = 1.0,
        debug: bool = False,
    ):

        self.scheduler.set_timesteps(num_inference_steps, offset=1)

        text_embeddings = self.text_encoder(input_ids, params=inference_state.text_encoder_params)[0]
        uncond_embeddings = self.text_encoder(uncond_input_ids, params=inference_state.text_encoder_params)[0]
        context = jnp.concatenate([uncond_embeddings, text_embeddings])

        latents_shape = (
            input_ids.shape[0],
            self.unet.config.sample_size,
            self.unet.config.sample_size,
            self.unet.config.in_channels,
        )
        latents = jax.random.normal(prng_seed, shape=latents_shape, dtype=jnp.float32)

        def loop_body(step, latents):
            # For classifier free guidance, we need to do two forward passes.
            # Here we concatenate the unconditional and text embeddings into a single batch
            # to avoid doing two forward passes
            latents_input = jnp.concatenate([latents] * 2)

            t = jnp.array(self.scheduler.timesteps)[step]
            timestep = jnp.broadcast_to(t, latents_input.shape[0])

            # predict the noise residual
            noise_pred = self.unet(
                latents_input, timestep, encoder_hidden_states=context, params=inference_state.unet_params
            )
            # perform guidance
            noise_pred_uncond, noise_prediction_text = jnp.split(noise_pred, 2, axis=0)
            noise_pred = noise_pred_uncond + guidance_scale * (noise_prediction_text - noise_pred_uncond)

            # compute the previous noisy sample x_t -> x_t-1
            latents = self.scheduler.step(noise_pred, t, latents)["prev_sample"]
            return latents

        if debug:
            # run with python for loop
            for i in range(num_inference_steps):
                latents = loop_body(i, latents)
        else:
            latents = jax.lax.fori_loop(0, num_inference_steps, loop_body, latents)

        # scale and decode the image latents with vae
        latents = 1 / 0.18215 * latents
        image = self.vae.decode(latents, params=inference_state.vae_params)

        return image
