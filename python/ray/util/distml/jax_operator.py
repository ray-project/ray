import numpy as np
import cupy as cp
from jax import grad, value_and_grad
import jax.numpy as jnp
from jax.lib import xla_client
from jax.dlpack import from_dlpack, to_dlpack
from jax.tree_util import tree_flatten, tree_unflatten, tree_structure, tree_map, build_tree
from jax._src.util import unzip2
from jax.experimental.optimizers import OptimizerState

from ray.util.distml.base_operator import TrainingOperator
from ray.util.sgd.utils import TimerCollection, AverageMeterCollection

jax2tf_available = True
try:
    import tensorflow as tf
    from ray.util.distml.jax_utils import convert_and_save_model, load_params_from_tf
except:
    print("Warning: Can not import jax2tf, please install tensorflow to support jax2tf. otherwise you can't save models.")
    jax2tf_available = False

tqdm = None
try:
    from tqdm import tqdm
except ImportError:
    pass


class JAXTrainingOperator(TrainingOperator):

    def __init__(self, operator_config):
        self.train_step_num = 0 # use to record the training step that has passed.
        self.timers = TimerCollection()
        super(JAXTrainingOperator, self).__init__(operator_config)

        if hasattr(self._config, "jit_mode"):
            assert not self._config["jit_mode"], "Not support jit in jax operator."

        self.setup(**self._config)

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

    def register(self, 
                 *,
                 model, 
                 optimizer, 
                 criterion, 
                 lr_schedulers=None, 
                 jit_mode=False):
        """Register a few critical information about the model to operator."""
        self.criterion = criterion
        if lr_schedulers:
            self.lr_schedulers = lr_schedulers
            print("WARNING: jax not support learning rate scheduler." 
                  "This will not work.")
        
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

    def loss_func(self, params, batch):
        inputs, targets = batch
        logits = self.predict_fun(params, inputs)
        return self.criterion(logits, targets)

    def derive_updates(self, batch):
        loss_val, gradient = self._calculate_gradient(self.opt_state, batch)
        return loss_val.item(), tree_flatten(gradient)[0]
        
    def apply_updates(self, gradient, num_workers):
        if isinstance(gradient, dict):
            gradient = list(gradient.values())
        gradient = tree_unflatten(self.opt_state[1], gradient)
        gradient = tree_map(lambda x: x/num_workers, gradient)
        self.opt_state = self.opt_update(self.train_step_num, gradient, self.opt_state)
        self.train_step_num += 1

    def to_cupy(self, tensor):
        if isinstance(tensor, list):
            return list(map(self.to_cupy, tensor))
        ctensor = cp.fromDlpack(self.get_jax_dlpack(tensor))
        assert ctensor.data.ptr == tensor.unsafe_buffer_pointer()
        return ctensor

    def to_operator_tensor(self, tensor):
        if isinstance(tensor, list):
            return list(map(self.to_operator_tensor, tensor))
        return from_dlpack(tensor.toDlpack())

    def _calculate_gradient(self, opt_state, batch):
        params = self.get_params(opt_state)
        loss_val, gradient = value_and_grad(self.loss_func)(params, batch)
        return loss_val, gradient

    def get_jax_dlpack(self, tensor):
        return xla_client._xla.buffer_to_dlpack_managed_tensor(tensor.device_buffer,
                                                               take_ownership=False)

    def validate(self, info={}):
        if not hasattr(self, "opt_state"):
            raise RuntimeError("model unset. Please register model in setup.")
        if not hasattr(self, "criterion"):
            raise RuntimeError("criterion unset. Please register criterion in setup.")
        
        params = self.get_params(self.opt_state)
        validation_loader = self.validation_loader
        metric_meters = AverageMeterCollection()

        for batch_idx, batch in enumerate(validation_loader):
            batch_info = {"batch_idx": batch_idx}
            batch_info.update(info)
            metrics = self.validate_step(params, batch, batch_info)
            metric_meters.update(metrics, n=metrics.pop("samples_num", 1))
        return metric_meters.summary()

    def validate_step(self, params, batch, batch_info):
        criterion = self.criterion
        predict_fun = self.predict_fun
        # unpack features into list to support multiple inputs model
        inputs, targets = batch

        with self.timers.record("eval_fwd"):
            outputs = predict_fun(params, inputs)
            loss = criterion(outputs, targets)
            prediction_class = jnp.argmax(outputs, axis=1)
            targets_class = jnp.argmax(targets, axis=1)

        acc = jnp.mean(prediction_class == targets_class)
        samples_num = targets.shape[0]

        return {
            "val_loss": loss.item(),
            "val_accuracy": acc.item(),
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
            flatten_params = flatten_params
            # flatten_params = list(map(self.to_cupy, flatten_params))
        return flatten_params

    def get_named_parameters(self, cpu):
        params = self.get_parameters(cpu)
        if hasattr(self, "preset_keys"):
            dict_params = {name:p for name, p in zip(self.preset_keys, params)}
        else:
            dict_params = {f"{idx}":p for idx, p in enumerate(params)}
        return dict_params

    def set_parameters(self, new_params):
        if isinstance(new_params, dict):
            keys, new_params = unzip2(sorted(new_params.items(), key=lambda d: int(d[0])))
            self.preset_keys = keys

        if not hasattr(self, "tree"):
            self.tree = tree_structure(self.get_params(self.opt_state)) 

        states_flat, tree, subtrees = self.opt_state

        states = map(tree_unflatten, subtrees, states_flat)

        def update(param, state):
            new_state = param, *state[1:]
            return new_state

        new_states = map(update, new_params, states)
        new_state_flat, new_subtrees = unzip2(map(tree_flatten, new_states))

        for idx, (subtree, new_subtree) in enumerate(zip(subtrees, new_subtrees)):
            if new_subtree != subtree:
                msg = ("input structur did not match the save params struture. "
                       "input {} and output {}.")
                raise TypeError(msg.format(subtree, new_subtree))

        self.opt_state = OptimizerState(new_state_flat, tree, subtrees)

    def reset_optimizer_for_params(self, params):
        self.tree = tree_structure(params)
        self.opt_state = self.opt_init(params)
        params2 = self.get_params(self.opt_state)
        assert params == params2

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

    # some operation for this ml system.
    def ones_like(self, x, cpu=True):
        if cpu:
            return np.ones_like(x)
        else:
            return jnp.ones_like(x)

    # some operation for this ml system.
    def zeros_like(self, x, cpu=True):
        if cpu:
            return np.zeros_like(x)
        else:
            return jnp.zeros_like(x)

    def numel(self, v):
        return np.size(v)

    def asarray(self, v):
        return jnp.asarray(v)

    def clean_redundancy(self):
        del self.train_loader
        del self.validation_loader