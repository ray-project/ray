"""TODO(Runhui): JAX Operator"""
import numpy as np
import cupy as cp
from jax import grad, value_and_grad
import jax.numpy as jnp
from jax.lib import xla_client
from jax.tree_util import tree_flatten, tree_unflatten, tree_structure
from jax._src.util import unzip2
from jax.experimental.optimizers import Optimizer

from ray.util.distml.base_operator import TrainingOperator
from ray.util.sgd.utils import TimerCollection, AverageMeterCollection

jax2tf_available = True
try:
    import tensorflow as tf
    from ray.util.distml.jax_utils import convert_and_save_model, load_params_from_tf
except:
    print("Warning: Can not import jax2tf, please install tensorflow to support jax2tf. otherwise you can't save models.")
    jax2tf_available = False


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
        self.init_fun = model[1]
        self.predict_fun = model[2]

    def _register_optimizer(self, optimizer):
        self.opt_init = optimizer[0]
        self.opt_update = optimizer[1]
        self.get_params = optimizer[2]

    def register_data(self, *, train_loader=None, validation_loader=None):
        self.train_loader = train_loader
        self.validation_loader = validation_loader

    def register_input_signatures(self, *, input_shape):
        if not isinstance(input_shape, list):
            input_shape = [input_shape]

        input_signatures = [
            # The first one will be the serving signature
            tf.TensorSpec(s, tf.float32)
            for s in input_shape
        ]
        self.input_signatures = input_signatures

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
            yield jax2cupy(g)
            cp_g/=self.world_size

    def jax2cupy(self, x):
        return cp.fromDlpack(self.get_jax_dlpack(x))

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

    def save_parameters(self, checkpoint):
        # save model
        if jax2tf_available:
            if not getattr(self, "input_signatures"):
                print("Warning: input_signatures has not set. "
                    "Please using register_input_signatures to register it.")
                self.input_signatures = [tf.TensorSpec([], tf.float32)]
            convert_and_save_model(
                self.predict_fun,
                self.get_params(self.opt_state),
                checkpoint,
                input_signatures=self.input_signatures,
                compile_model=False)
        else:
            raise RuntimeError("jax2tf is not available.")

    def load_parameters(self, checkpoint):
        print("Warning: Jax not support loading checkpoint. ")
        if jax2tf_available:
            restored_model = load_params_from_tf(checkpoint)
        else:
            raise RuntimeError("jax2tf is not available.")

    def get_parameters(self, cpu):
        params = self.get_params(self.opt_state)
        flatten_params, tree = tree_flatten(params)
        if not hasattr(self, "tree"):
            self.tree = tree
        
        if cpu:
            flatten_params = list(map(np.asarray, flatten_params))
        else:
            flatten_params = list(map(jax2cupy, flatten_params))
        return flatten_params

    def get_named_parameters(self, cpu):
        params = self.get_parameters(cpu)
        dict_params = {f"{idx}":p for idx, p in enumerate(params)}
        return dict_params

    def set_parameters(self, params):
        # need in place parameters in opt_state
        if isinstance(params, dict):
            print("Warning: using named parameter to set params")
            params = list(params.values())

        if not hasattr(self, "tree"):
            self.tree = tree_structure(self.get_params(self.opt_state)) 

        states_flat, tree, subtrees = self.opt_state
        states = list(map(tree_unflatten, subtrees, states_flat))

        new_states = params, *states[1:]
        new_states_flat, subtrees2 = unzip2(map(tree_flatten, states))

        for idx, (subtree, subtree2) in enumerate(zip(subtrees, subtrees2)):
            if subtree2 != subtree:
                msg = ("optimizer update function produced an output structure that "
                        "did not match its input structure: input {} and output {}.")
                raise TypeError(msg.format(subtree, subtree2))

        self.opt_state = Optimizer(new_states_flat, tree, subtrees)

    def reset_optimizer_for_params(self, params):
        opt_init, opt_update, get_params
        self.opt_state = self.opt_init(params)

    # some operation for this ml system.
    def ones(self, shape, cpu=True):
        if cpu:
            return np.ones(shape)
        else:
            return jnp.ones(shape)

    # some operation for this ml system.
    def zeros(self, shape, cpu=True):
        if cpu:
            return np.zeros(shape)
        else:
            return jnp.zeros(shape)

    def numel(self, v):
        return np.size(v)