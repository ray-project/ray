"""A basic MNIST example using Numpy and JAX.

The primary aim here is simplicity and minimal dependencies.
"""
import time

import numpy as np
import numpy.random as npr

import jax
from jax import jit, grad, random, dlpack
from jax.tree_util import tree_flatten
from jax.experimental import optimizers
import jax.numpy as jnp
import jax.experimental.stax as stax
# import datasets
# from resnet import ResNet18

import ray
import ray.util.collective as col
import cupy as cp
import os

import functools


def create_root_context():
    return VariableContext({}, '')

class VariableContext(object):
    def __init__(self, name2val, prefix, allow_new=True):
        self.name2val = name2val
        self.prefix = prefix
        self.allow_new = allow_new
    def scope(self, name):
        return VariableContext(self.name2val,
            self._join(self.prefix, name), self.allow_new)
    def get_variable(self, name, initializer):
        return self.get_variable_absolute(
            name=self._join(self.prefix, name),
            initializer=initializer)
    def get_variable_absolute(self, name, initializer):
        if name not in self.name2val:
            assert self.allow_new
            val = initializer()
            assert type(val) == np.ndarray and val.dtype == np.float32
            self.name2val[name] = jnp.asarray(val)

        return self.name2val[name]
    def _join(self, *xs):
        return '/'.join(xs)
    def variables_list(self):
        return list(self.name2val.values())
    def replace_with_list(self, newlist):
        assert len(newlist) == len(self.name2val)
        name2val = {k : v for (k, v) in zip(self.name2val.keys(), newlist)}
        return VariableContext(name2val, self.prefix, self.allow_new)

def print_variables(cx):
    for (name, val) in sorted(cx.name2val.items()):
        print(f'{name:20s} {str(val.shape):20s} {str(val.dtype):20s}')

# End framework
# ----------------------------------------

def normax(shape, axis):
    out = npr.randn(*shape).astype(jnp.float32)
    out /= np.sqrt(np.square(out).sum(axis=axis, keepdims=True))
    return out

def normc(*shape):
    return normax(shape, axis=0)

def randn(shape, stddev):
    return npr.randn(*shape).astype(jnp.float32) * stddev

def gelu(x):
    return 0.5*x*(1+jnp.tanh(0.79788*(x+0.044715*x**3)))

def _norm(x, *, axis, g=None, b=None, e=1e-5):
    u = jnp.mean(x, axis=axis, keepdims=True)
    s = jnp.mean(jnp.square(x-u), axis=axis, keepdims=True)
    x = (x - u) / jnp.sqrt(s + e)
    if g is not None and b is not None:
        x = x * g + b
    return x

def norm(cx, x, axis=-1):
    n_state = x.shape[axis]
    g = cx.get_variable("g", initializer=lambda : np.ones(n_state, 'f'))
    b = cx.get_variable("b", initializer=lambda : np.zeros(n_state, 'f'))
    return _norm(x, g=g, b=b, axis=axis)

def mask_attn_weights(w):
    n = w.shape[-1]
    b = jnp.tril(jnp.ones((n,n)))
    b = jnp.reshape(b, (1, 1, n, n))
    w = w * b - 1e9 * (1 - b)
    return w

def _attn(Q_bhtr, K_bhrt, V_bhtr):
    R = Q_bhtr.shape[-1]
    W_bhtt = jnp.matmul(Q_bhtr, K_bhrt) / jnp.sqrt(R)
    W_bhtt = mask_attn_weights(W_bhtt)
    W_bhtt = stax.softmax(W_bhtt, axis=-1)
    A_bhtr = jnp.matmul(W_bhtt, V_bhtr)
    return A_bhtr

def dense(cx, X_btk, F):
    B, T, K = X_btk.shape
    X_bt_k = jnp.reshape(X_btk, (-1, K))
    W_kf = cx.get_variable("w", initializer=lambda : normc(K, F))
    b_f = cx.get_variable("b", initializer=lambda : np.zeros(F,'f'))
    Y_bt_f = jnp.matmul(X_bt_k, W_kf) + b_f
    return jnp.reshape(Y_bt_f, (B, T, F))

def attn(cx, X_btk, n_state, n_head):
    B, T, _K = X_btk.shape
    assert n_state % n_head==0
    QKV_b_t_3s = dense(cx.scope('c_attn'), X_btk, n_state * 3)
    QKV_b_t_3h_r = jnp.reshape(QKV_b_t_3s, (B, T, 3 * n_head, n_state // n_head))
    Q_bthr, K_bthr, V_bthr = jnp.split(QKV_b_t_3h_r, 3, axis=2)
    Q_bhtr = jnp.transpose(Q_bthr, (0, 2, 1, 3))
    V_bhtr = jnp.transpose(V_bthr, (0, 2, 1, 3))
    K_bhrt = jnp.transpose(K_bthr, (0, 2, 3, 1))
    A_bhtr = _attn(Q_bhtr, K_bhrt, V_bhtr)
    A_bthr = jnp.transpose(A_bhtr, (0, 2, 1, 3))
    A_bts = jnp.reshape(A_bthr, (B, T, n_state))
    P_bts = dense(cx.scope('c_proj'), A_bts, n_state)
    return P_bts

def mlp(cx, X_bts, *, n_hid):
    S = X_bts.shape[-1]
    H_bth = stax.relu(dense(cx.scope('c_fc'), X_bts, n_hid))
    Y_bts = dense(cx.scope('c_proj'), H_bth, S)
    return Y_bts

def block(cx, X_bts, *, n_head):
    _B, _T, S = X_bts.shape
    A_bts = attn(cx.scope('attn'), X_bts, S, n_head)
    N_bts = norm(cx.scope('ln_1'), X_bts + A_bts, axis=-1)
    M_bts = mlp(cx.scope('mlp'), N_bts, n_hid=S * 4)
    Y_bts = norm(cx.scope('ln_2'), N_bts + M_bts, axis=-1)
    return Y_bts

def fc(cx, X_bts, n_hid):
    B, K = X_bts.shape
    W_kf = cx.get_variable("w", initializer=lambda : normc(K, n_hid))
    b_f = cx.get_variable("b", initializer=lambda : np.zeros(n_hid,'f'))
    Y_bt_f = jnp.matmul(X_bts, W_kf) + b_f
    return Y_bt_f

def transformer(cx, tok_bt, *, n_vocab, n_head, n_layer, n_ctx, n_embd):
    B, T = tok_bt.shape
    pos_bt = jax.lax.broadcasted_iota(jnp.int32, (B, T), 1)
    tokenembs_qe = cx.get_variable('tokenembs',
        initializer=lambda : normc(n_vocab, n_embd) * 0.1)
    posembs_pe = cx.get_variable('posembs',
        initializer=lambda : normc(n_ctx, n_embd) * 0.1)
    tokenemb_bte = tokenembs_qe[tok_bt]
    assert isinstance(tok_bt, jnp.ndarray)
    posemb_bte = posembs_pe[pos_bt]
    last_bts = tokenemb_bte + posemb_bte
    for layer in range(n_layer):
        last_bts = block(cx.scope(f'h{layer:02d}'), last_bts, n_head=n_head)
    # logits_btq = jnp.matmul(last_bts, tokenembs_qe.T) # This output (batchsize, length, vocab_size)
    logits_btq = fc(cx.scope('pred_head'), last_bts[:, 0, :], n_hid=5)
    logprobs_btq = stax.logsoftmax(logits_btq)
    return logprobs_btq


if __name__ == '__main__':
    os.environ["CUDA_VISIBLE_DEVICES"] = "7"

    from sst import make_sst5_dataloader
    train_dataloader, val_dataloader, test_dataloader = make_sst5_dataloader()

    npr.seed(0)
    n_ctx = 256  # length
    batch_size = 128
    n_head = 4
    n_layer = 6
    n_embd = 256
    model = functools.partial(transformer, n_vocab=30522,
        n_head=n_head, n_layer=n_layer, n_ctx=n_ctx, n_embd=n_embd)

    # Xtr_bt, Xte_bt = train_test_split(codebook, text, n_ctx)
    root_cx = create_root_context()

    def loss(cx, batch):
        input, target = batch
        logprobs_btq = model(cx, input[:, :-1])
        return -jnp.sum(logprobs_btq * target)

        ## this for seq2seq
        # B, T = X_bt.shape
        # Y_bt = target
        # loglosses_bt = - logprobs_btq.reshape((B*T, -1))[
            # jnp.arange(B*T), Y_bt.reshape((-1,))]
        # return loglosses_bt.mean()

    def loss2(params, batch):
        cx = root_cx.replace_with_list(params)
        return loss(cx, batch)

    batch = next(iter(train_dataloader))
    loss(root_cx, batch) # Just create variables
    root_cx.allow_new = False
    print_variables(root_cx)
    init_params = root_cx.variables_list()

    opt_init, opt_update, get_params = optimizers.adam(step_size=1e-1)

    opt_state = opt_init(init_params)

    @jax.jit
    def update(i, opt_state, batch):
        params = get_params(opt_state)
        v, g = jax.value_and_grad(loss2)(params, batch)
        return v, opt_update(i, g, opt_state)

    def accuracy(params, dataloader):
        result = []
        cx = root_cx.replace_with_list(params)
        for _, (inputs, targets) in enumerate(dataloader):
            logits = model(cx, inputs[:, :-1])
            predicted_class = jnp.argmax(logits, axis=1)
            target_class = jnp.argmax(targets, axis=1)
            result.append(jnp.mean(predicted_class == target_class))
        return np.array(result).mean()

    steps = 0
    for epoch in range(1000):
        print('Epoch', epoch)
        for batch in train_dataloader:
            tstart = time.time()
            lossval, opt_state = update(steps, opt_state, batch)
            steps += 1
            print(f'loss={lossval:8.3f} dur={time.time()-tstart:.2f}')
        params = get_params(opt_state)
        val_acc = accuracy(params, val_dataloader)
        print("Val set accuracy {}".format(val_acc))

    test_acc = accuracy(params, test_dataloader)
    print("Test set accuracy {}".format(test_acc))




