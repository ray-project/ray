"""TODO(Runhui): JAX Operator"""
import cupy as cp
from jax import grad, value_and_grad
import jax.numpy as jnp
from jax.lib import xla_client
from jax.tree_util import tree_flatten

from ray.util.distml.base_operator import TrainingOperator
from ray.util.sgd.utils import TimerCollection, AverageMeterCollection

class JAXTrainingOperator(TrainingOperator):

    def __init__(self, operator_config):
        self.train_step_num = 0 # use to record the training step that has passed.
        self.timers = TimerCollection()
        super(JAXTrainingOperator, self).__init__(operator_config)

    def setup(self, *args, **kwargs):
        """Function that needs to be override by users.
        
        Example:
            # some code is the same for all users, maybe we can put it in register.
            rng_key = random.PRNGKey(0)
            input_shape = (28, 28, 1, batch_size)
            lr=0.01
            init_fun, predict_fun = ResNet18(num_classes)
            _, init_params = init_fun(rng_key, input_shape)
            
            opt_init, opt_update, get_params = optimizers.adam(lr)
            opt_state = opt_init(init_params)
            
            self.register(model=(opt_state, get_params, predict_fun), optimizer=opt_update, criterion=lambda logits, targets:-jnp.sum(logits * targets)
        
        """
        
        pass

    def register(self, *, model, optimizer, criterion, jit_mode=False):
        """Register a few critical information about the model to operator."""
        self.criterion = criterion
        
        self._register_model(model)
        self._register_optimizer(optimizer)
            
    def _register_model(self, model):
        self.opt_state = model[0]
        self.get_params = model[1]
        self.predict_fun = model[2]

    def _register_optimizer(self, optimizer):
        self.opt_update = optimizer

    def register_data(self, *, train_loader=None, validation_loader=None):
        self.train_loader = train_loader
        self.validation_loader = validation_loader

    def loss_fn(self, params, batch):
        inputs, targets = batch
        logits = self.predict_fun(params, inputs)
        return self.criterion(logits, targets)

    def yield_train_loader(self):
        for batch in self.train_loader:
            yield batch

    def yield_validation_loader(self):
        for batch in self.validation_loader:
            yield batch

    # def train_epoch(self, iterator, info):
    #     if not self.train_dataloader:
    #         raise RuntimeError("Train dataloader hasn't been set.")

    #     for idx, batch in enumerate(self.train_dataloader):
    #         self.train_step(batch)

    #     params = self.get_params(self.opt_state)
        
    #     return {"train_loss": loss.item(), NUM_SAMPLES: features[0].size(0)}

    # def train_step(self, batch):
    #     gradient = self.derive_updates(batch, opt_state, loss_fn)     
        
    #     flatten_gradient, _ = tree_flatten(gradient)
    #     # <=allreduce strategy=>
    #     #for g in ftree:
    #     #    if not len(g):
    #     #        continue
    #     #    cp_g = cp.fromDlpack(self.get_jax_dlpack(g))
    #     #    col.allreduce(cp_g, group_name="default")
    #     #    cp_g/=self.world_size
     
    #     self.apply_updates(gradient)

    def derive_updates(self, batch):
        return self._calculate_gradient(self.opt_state, batch)
        
    def apply_updates(self, gradient):
        self.opt_state = self.opt_update(self.train_step_num, gradient, self.opt_state)
        self.train_step_num += 1

    def updates_transform(self, updates):
        flatten_grads = tree_flatten(updates)[0]

        for g in flatten_grads:
            if not len(g):
               continue
            yield cp.fromDlpack(self.get_jax_dlpack(g))
            # col.allreduce(cp_g, group_name="default")
            # cp_g/=self.world_size

    def _calculate_gradient(self, opt_state, batch):
        params = self.get_params(opt_state)
        loss_val, gradient = value_and_grad(self.loss_fn)(params, batch)
        return loss_val, gradient

    def get_jax_dlpack(self, tensor):
        return xla_client._xla.buffer_to_dlpack_managed_tensor(tensor.device_buffer,
                                                               take_ownership=False)

    def validate(self, info={}):
        if not hasattr(self, "opt_state"):
            raise RuntimeError("model has not registered.")
        params = self.get_params(self.opt_state)
        validation_loader = self.validation_loader
        metric_meters = AverageMeterCollection()

        for batch_idx, batch in enumerate(validation_loader):
            batch_info = {"batch_idx": batch_idx}
            batch_info.update(info)
            metrics = self.validate_batch(params, batch, batch_info)
            metric_meters.update(metrics, n=metrics.pop("samples_num", 1))

        return metric_meters.summary()

    def validate_batch(self, params, batch, batch_info):
        if not hasattr(self, "opt_state"):
            raise RuntimeError("model unset. Please register model in setup.")
        if not hasattr(self, "criterion"):
            raise RuntimeError("criterion unset. Please register criterion in setup.")
        criterion = self.criterion
        predict_fun = self.predict_fun
        # unpack features into list to support multiple inputs model
        *inputs, targets = batch

        with self.timers.record("eval_fwd"):
            outputs = predict_fun(params, *inputs)
            loss = criterion(outputs, targets)
            prediction_class = jnp.argmax(outputs, axis=1)
            targets_class = jnp.argmax(targets, axis=1)

        acc = jnp.mean(prediction_class == targets_class)
        samples_num = targets.shape[0]

        return {
            "val_loss": loss,
            "val_accuracy": acc,
            "samples_num": samples_num
        }


    def set_parameters(self, params):
        # need in place parameters in opt_state
        
        pass
        
    def get_parameters(self):
        return self.get_params(self.opt_state)
