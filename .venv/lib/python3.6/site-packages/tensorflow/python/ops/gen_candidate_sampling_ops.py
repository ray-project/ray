"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: candidate_sampling_ops.cc
"""

import collections as _collections
import six as _six

from tensorflow.python import pywrap_tensorflow as _pywrap_tensorflow
from tensorflow.python.eager import context as _context
from tensorflow.python.eager import core as _core
from tensorflow.python.eager import execute as _execute
from tensorflow.python.framework import dtypes as _dtypes
from tensorflow.python.framework import errors as _errors
from tensorflow.python.framework import tensor_shape as _tensor_shape

from tensorflow.core.framework import op_def_pb2 as _op_def_pb2
# Needed to trigger the call to _set_call_cpp_shape_fn.
from tensorflow.python.framework import common_shapes as _common_shapes
from tensorflow.python.framework import op_def_registry as _op_def_registry
from tensorflow.python.framework import ops as _ops
from tensorflow.python.framework import op_def_library as _op_def_library
from tensorflow.python.util.deprecation import deprecated_endpoints
from tensorflow.python.util import dispatch as _dispatch
from tensorflow.python.util.tf_export import tf_export


_all_candidate_sampler_outputs = ["sampled_candidates", "true_expected_count",
                                 "sampled_expected_count"]
_AllCandidateSamplerOutput = _collections.namedtuple(
    "AllCandidateSampler", _all_candidate_sampler_outputs)


def all_candidate_sampler(true_classes, num_true, num_sampled, unique, seed=0, seed2=0, name=None):
  r"""Generates labels for candidate sampling with a learned unigram distribution.

  See explanations of candidate sampling and the data formats at
  go/candidate-sampling.

  For each batch, this op picks a single set of sampled candidate labels.

  The advantages of sampling candidates per-batch are simplicity and the
  possibility of efficient dense matrix multiplication. The disadvantage is that
  the sampled candidates must be chosen independently of the context and of the
  true labels.

  Args:
    true_classes: A `Tensor` of type `int64`.
      A batch_size * num_true matrix, in which each row contains the
      IDs of the num_true target_classes in the corresponding original label.
    num_true: An `int` that is `>= 1`. Number of true labels per context.
    num_sampled: An `int` that is `>= 1`. Number of candidates to produce.
    unique: A `bool`.
      If unique is true, we sample with rejection, so that all sampled
      candidates in a batch are unique. This requires some approximation to
      estimate the post-rejection sampling probabilities.
    seed: An optional `int`. Defaults to `0`.
      If either seed or seed2 are set to be non-zero, the random number
      generator is seeded by the given seed.  Otherwise, it is seeded by a
      random seed.
    seed2: An optional `int`. Defaults to `0`.
      An second seed to avoid seed collision.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (sampled_candidates, true_expected_count, sampled_expected_count).

    sampled_candidates: A `Tensor` of type `int64`.
    true_expected_count: A `Tensor` of type `float32`.
    sampled_expected_count: A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "AllCandidateSampler", name, _ctx._post_execution_callbacks,
        true_classes, "num_true", num_true, "num_sampled", num_sampled,
        "unique", unique, "seed", seed, "seed2", seed2)
      _result = _AllCandidateSamplerOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return all_candidate_sampler_eager_fallback(
            true_classes, num_true=num_true, num_sampled=num_sampled,
            unique=unique, seed=seed, seed2=seed2, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  num_true = _execute.make_int(num_true, "num_true")
  num_sampled = _execute.make_int(num_sampled, "num_sampled")
  unique = _execute.make_bool(unique, "unique")
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  _, _, _op = _op_def_lib._apply_op_helper(
        "AllCandidateSampler", true_classes=true_classes, num_true=num_true,
                               num_sampled=num_sampled, unique=unique,
                               seed=seed, seed2=seed2, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("num_true", _op.get_attr("num_true"), "num_sampled",
            _op.get_attr("num_sampled"), "unique", _op.get_attr("unique"),
            "seed", _op.get_attr("seed"), "seed2", _op.get_attr("seed2"))
  _execute.record_gradient(
      "AllCandidateSampler", _inputs_flat, _attrs, _result, name)
  _result = _AllCandidateSamplerOutput._make(_result)
  return _result



def all_candidate_sampler_eager_fallback(true_classes, num_true, num_sampled, unique, seed=0, seed2=0, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function all_candidate_sampler
  """
  _ctx = ctx if ctx else _context.context()
  num_true = _execute.make_int(num_true, "num_true")
  num_sampled = _execute.make_int(num_sampled, "num_sampled")
  unique = _execute.make_bool(unique, "unique")
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  true_classes = _ops.convert_to_tensor(true_classes, _dtypes.int64)
  _inputs_flat = [true_classes]
  _attrs = ("num_true", num_true, "num_sampled", num_sampled, "unique",
  unique, "seed", seed, "seed2", seed2)
  _result = _execute.execute(b"AllCandidateSampler", 3, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "AllCandidateSampler", _inputs_flat, _attrs, _result, name)
  _result = _AllCandidateSamplerOutput._make(_result)
  return _result


_compute_accidental_hits_outputs = ["indices", "ids", "weights"]
_ComputeAccidentalHitsOutput = _collections.namedtuple(
    "ComputeAccidentalHits", _compute_accidental_hits_outputs)


def compute_accidental_hits(true_classes, sampled_candidates, num_true, seed=0, seed2=0, name=None):
  r"""Computes the ids of the positions in sampled_candidates that match true_labels.

  When doing log-odds NCE, the result of this op should be passed through a
  SparseToDense op, then added to the logits of the sampled candidates. This has
  the effect of 'removing' the sampled labels that match the true labels by
  making the classifier sure that they are sampled labels.

  Args:
    true_classes: A `Tensor` of type `int64`.
      The true_classes output of UnpackSparseLabels.
    sampled_candidates: A `Tensor` of type `int64`.
      The sampled_candidates output of CandidateSampler.
    num_true: An `int`. Number of true labels per context.
    seed: An optional `int`. Defaults to `0`.
      If either seed or seed2 are set to be non-zero, the random number
      generator is seeded by the given seed.  Otherwise, it is seeded by a
      random seed.
    seed2: An optional `int`. Defaults to `0`.
      An second seed to avoid seed collision.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (indices, ids, weights).

    indices: A `Tensor` of type `int32`.
    ids: A `Tensor` of type `int64`.
    weights: A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ComputeAccidentalHits", name, _ctx._post_execution_callbacks,
        true_classes, sampled_candidates, "num_true", num_true, "seed", seed,
        "seed2", seed2)
      _result = _ComputeAccidentalHitsOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return compute_accidental_hits_eager_fallback(
            true_classes, sampled_candidates, num_true=num_true, seed=seed,
            seed2=seed2, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  num_true = _execute.make_int(num_true, "num_true")
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ComputeAccidentalHits", true_classes=true_classes,
                                 sampled_candidates=sampled_candidates,
                                 num_true=num_true, seed=seed, seed2=seed2,
                                 name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("num_true", _op.get_attr("num_true"), "seed",
            _op.get_attr("seed"), "seed2", _op.get_attr("seed2"))
  _execute.record_gradient(
      "ComputeAccidentalHits", _inputs_flat, _attrs, _result, name)
  _result = _ComputeAccidentalHitsOutput._make(_result)
  return _result



def compute_accidental_hits_eager_fallback(true_classes, sampled_candidates, num_true, seed=0, seed2=0, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function compute_accidental_hits
  """
  _ctx = ctx if ctx else _context.context()
  num_true = _execute.make_int(num_true, "num_true")
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  true_classes = _ops.convert_to_tensor(true_classes, _dtypes.int64)
  sampled_candidates = _ops.convert_to_tensor(sampled_candidates, _dtypes.int64)
  _inputs_flat = [true_classes, sampled_candidates]
  _attrs = ("num_true", num_true, "seed", seed, "seed2", seed2)
  _result = _execute.execute(b"ComputeAccidentalHits", 3, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ComputeAccidentalHits", _inputs_flat, _attrs, _result, name)
  _result = _ComputeAccidentalHitsOutput._make(_result)
  return _result


_fixed_unigram_candidate_sampler_outputs = ["sampled_candidates",
                                           "true_expected_count",
                                           "sampled_expected_count"]
_FixedUnigramCandidateSamplerOutput = _collections.namedtuple(
    "FixedUnigramCandidateSampler", _fixed_unigram_candidate_sampler_outputs)


def fixed_unigram_candidate_sampler(true_classes, num_true, num_sampled, unique, range_max, vocab_file="", distortion=1, num_reserved_ids=0, num_shards=1, shard=0, unigrams=[], seed=0, seed2=0, name=None):
  r"""Generates labels for candidate sampling with a learned unigram distribution.

  A unigram sampler could use a fixed unigram distribution read from a
  file or passed in as an in-memory array instead of building up the distribution
  from data on the fly. There is also an option to skew the distribution by
  applying a distortion power to the weights.

  The vocabulary file should be in CSV-like format, with the last field
  being the weight associated with the word.

  For each batch, this op picks a single set of sampled candidate labels.

  The advantages of sampling candidates per-batch are simplicity and the
  possibility of efficient dense matrix multiplication. The disadvantage is that
  the sampled candidates must be chosen independently of the context and of the
  true labels.

  Args:
    true_classes: A `Tensor` of type `int64`.
      A batch_size * num_true matrix, in which each row contains the
      IDs of the num_true target_classes in the corresponding original label.
    num_true: An `int` that is `>= 1`. Number of true labels per context.
    num_sampled: An `int` that is `>= 1`.
      Number of candidates to randomly sample.
    unique: A `bool`.
      If unique is true, we sample with rejection, so that all sampled
      candidates in a batch are unique. This requires some approximation to
      estimate the post-rejection sampling probabilities.
    range_max: An `int` that is `>= 1`.
      The sampler will sample integers from the interval [0, range_max).
    vocab_file: An optional `string`. Defaults to `""`.
      Each valid line in this file (which should have a CSV-like format)
      corresponds to a valid word ID. IDs are in sequential order, starting from
      num_reserved_ids. The last entry in each line is expected to be a value
      corresponding to the count or relative probability. Exactly one of vocab_file
      and unigrams needs to be passed to this op.
    distortion: An optional `float`. Defaults to `1`.
      The distortion is used to skew the unigram probability distribution.
      Each weight is first raised to the distortion's power before adding to the
      internal unigram distribution. As a result, distortion = 1.0 gives regular
      unigram sampling (as defined by the vocab file), and distortion = 0.0 gives
      a uniform distribution.
    num_reserved_ids: An optional `int`. Defaults to `0`.
      Optionally some reserved IDs can be added in the range [0,
      ..., num_reserved_ids) by the users. One use case is that a special unknown
      word token is used as ID 0. These IDs will have a sampling probability of 0.
    num_shards: An optional `int` that is `>= 1`. Defaults to `1`.
      A sampler can be used to sample from a subset of the original range
      in order to speed up the whole computation through parallelism. This parameter
      (together with 'shard') indicates the number of partitions that are being
      used in the overall computation.
    shard: An optional `int` that is `>= 0`. Defaults to `0`.
      A sampler can be used to sample from a subset of the original range
      in order to speed up the whole computation through parallelism. This parameter
      (together with 'num_shards') indicates the particular partition number of a
      sampler op, when partitioning is being used.
    unigrams: An optional list of `floats`. Defaults to `[]`.
      A list of unigram counts or probabilities, one per ID in sequential
      order. Exactly one of vocab_file and unigrams should be passed to this op.
    seed: An optional `int`. Defaults to `0`.
      If either seed or seed2 are set to be non-zero, the random number
      generator is seeded by the given seed.  Otherwise, it is seeded by a
      random seed.
    seed2: An optional `int`. Defaults to `0`.
      An second seed to avoid seed collision.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (sampled_candidates, true_expected_count, sampled_expected_count).

    sampled_candidates: A `Tensor` of type `int64`.
    true_expected_count: A `Tensor` of type `float32`.
    sampled_expected_count: A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "FixedUnigramCandidateSampler", name, _ctx._post_execution_callbacks,
        true_classes, "num_true", num_true, "num_sampled", num_sampled,
        "unique", unique, "range_max", range_max, "vocab_file", vocab_file,
        "distortion", distortion, "num_reserved_ids", num_reserved_ids,
        "num_shards", num_shards, "shard", shard, "unigrams", unigrams,
        "seed", seed, "seed2", seed2)
      _result = _FixedUnigramCandidateSamplerOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return fixed_unigram_candidate_sampler_eager_fallback(
            true_classes, num_true=num_true, num_sampled=num_sampled,
            unique=unique, range_max=range_max, vocab_file=vocab_file,
            distortion=distortion, num_reserved_ids=num_reserved_ids,
            num_shards=num_shards, shard=shard, unigrams=unigrams, seed=seed,
            seed2=seed2, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  num_true = _execute.make_int(num_true, "num_true")
  num_sampled = _execute.make_int(num_sampled, "num_sampled")
  unique = _execute.make_bool(unique, "unique")
  range_max = _execute.make_int(range_max, "range_max")
  if vocab_file is None:
    vocab_file = ""
  vocab_file = _execute.make_str(vocab_file, "vocab_file")
  if distortion is None:
    distortion = 1
  distortion = _execute.make_float(distortion, "distortion")
  if num_reserved_ids is None:
    num_reserved_ids = 0
  num_reserved_ids = _execute.make_int(num_reserved_ids, "num_reserved_ids")
  if num_shards is None:
    num_shards = 1
  num_shards = _execute.make_int(num_shards, "num_shards")
  if shard is None:
    shard = 0
  shard = _execute.make_int(shard, "shard")
  if unigrams is None:
    unigrams = []
  if not isinstance(unigrams, (list, tuple)):
    raise TypeError(
        "Expected list for 'unigrams' argument to "
        "'fixed_unigram_candidate_sampler' Op, not %r." % unigrams)
  unigrams = [_execute.make_float(_f, "unigrams") for _f in unigrams]
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  _, _, _op = _op_def_lib._apply_op_helper(
        "FixedUnigramCandidateSampler", true_classes=true_classes,
                                        num_true=num_true,
                                        num_sampled=num_sampled,
                                        unique=unique, range_max=range_max,
                                        vocab_file=vocab_file,
                                        distortion=distortion,
                                        num_reserved_ids=num_reserved_ids,
                                        num_shards=num_shards, shard=shard,
                                        unigrams=unigrams, seed=seed,
                                        seed2=seed2, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("num_true", _op.get_attr("num_true"), "num_sampled",
            _op.get_attr("num_sampled"), "unique", _op.get_attr("unique"),
            "range_max", _op.get_attr("range_max"), "vocab_file",
            _op.get_attr("vocab_file"), "distortion",
            _op.get_attr("distortion"), "num_reserved_ids",
            _op.get_attr("num_reserved_ids"), "num_shards",
            _op.get_attr("num_shards"), "shard", _op.get_attr("shard"),
            "unigrams", _op.get_attr("unigrams"), "seed",
            _op.get_attr("seed"), "seed2", _op.get_attr("seed2"))
  _execute.record_gradient(
      "FixedUnigramCandidateSampler", _inputs_flat, _attrs, _result, name)
  _result = _FixedUnigramCandidateSamplerOutput._make(_result)
  return _result



def fixed_unigram_candidate_sampler_eager_fallback(true_classes, num_true, num_sampled, unique, range_max, vocab_file="", distortion=1, num_reserved_ids=0, num_shards=1, shard=0, unigrams=[], seed=0, seed2=0, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function fixed_unigram_candidate_sampler
  """
  _ctx = ctx if ctx else _context.context()
  num_true = _execute.make_int(num_true, "num_true")
  num_sampled = _execute.make_int(num_sampled, "num_sampled")
  unique = _execute.make_bool(unique, "unique")
  range_max = _execute.make_int(range_max, "range_max")
  if vocab_file is None:
    vocab_file = ""
  vocab_file = _execute.make_str(vocab_file, "vocab_file")
  if distortion is None:
    distortion = 1
  distortion = _execute.make_float(distortion, "distortion")
  if num_reserved_ids is None:
    num_reserved_ids = 0
  num_reserved_ids = _execute.make_int(num_reserved_ids, "num_reserved_ids")
  if num_shards is None:
    num_shards = 1
  num_shards = _execute.make_int(num_shards, "num_shards")
  if shard is None:
    shard = 0
  shard = _execute.make_int(shard, "shard")
  if unigrams is None:
    unigrams = []
  if not isinstance(unigrams, (list, tuple)):
    raise TypeError(
        "Expected list for 'unigrams' argument to "
        "'fixed_unigram_candidate_sampler' Op, not %r." % unigrams)
  unigrams = [_execute.make_float(_f, "unigrams") for _f in unigrams]
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  true_classes = _ops.convert_to_tensor(true_classes, _dtypes.int64)
  _inputs_flat = [true_classes]
  _attrs = ("num_true", num_true, "num_sampled", num_sampled, "unique",
  unique, "range_max", range_max, "vocab_file", vocab_file, "distortion",
  distortion, "num_reserved_ids", num_reserved_ids, "num_shards", num_shards,
  "shard", shard, "unigrams", unigrams, "seed", seed, "seed2", seed2)
  _result = _execute.execute(b"FixedUnigramCandidateSampler", 3,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "FixedUnigramCandidateSampler", _inputs_flat, _attrs, _result, name)
  _result = _FixedUnigramCandidateSamplerOutput._make(_result)
  return _result


_learned_unigram_candidate_sampler_outputs = ["sampled_candidates",
                                             "true_expected_count",
                                             "sampled_expected_count"]
_LearnedUnigramCandidateSamplerOutput = _collections.namedtuple(
    "LearnedUnigramCandidateSampler",
    _learned_unigram_candidate_sampler_outputs)


def learned_unigram_candidate_sampler(true_classes, num_true, num_sampled, unique, range_max, seed=0, seed2=0, name=None):
  r"""Generates labels for candidate sampling with a learned unigram distribution.

  See explanations of candidate sampling and the data formats at
  go/candidate-sampling.

  For each batch, this op picks a single set of sampled candidate labels.

  The advantages of sampling candidates per-batch are simplicity and the
  possibility of efficient dense matrix multiplication. The disadvantage is that
  the sampled candidates must be chosen independently of the context and of the
  true labels.

  Args:
    true_classes: A `Tensor` of type `int64`.
      A batch_size * num_true matrix, in which each row contains the
      IDs of the num_true target_classes in the corresponding original label.
    num_true: An `int` that is `>= 1`. Number of true labels per context.
    num_sampled: An `int` that is `>= 1`.
      Number of candidates to randomly sample.
    unique: A `bool`.
      If unique is true, we sample with rejection, so that all sampled
      candidates in a batch are unique. This requires some approximation to
      estimate the post-rejection sampling probabilities.
    range_max: An `int` that is `>= 1`.
      The sampler will sample integers from the interval [0, range_max).
    seed: An optional `int`. Defaults to `0`.
      If either seed or seed2 are set to be non-zero, the random number
      generator is seeded by the given seed.  Otherwise, it is seeded by a
      random seed.
    seed2: An optional `int`. Defaults to `0`.
      An second seed to avoid seed collision.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (sampled_candidates, true_expected_count, sampled_expected_count).

    sampled_candidates: A `Tensor` of type `int64`.
    true_expected_count: A `Tensor` of type `float32`.
    sampled_expected_count: A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "LearnedUnigramCandidateSampler", name,
        _ctx._post_execution_callbacks, true_classes, "num_true", num_true,
        "num_sampled", num_sampled, "unique", unique, "range_max", range_max,
        "seed", seed, "seed2", seed2)
      _result = _LearnedUnigramCandidateSamplerOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return learned_unigram_candidate_sampler_eager_fallback(
            true_classes, num_true=num_true, num_sampled=num_sampled,
            unique=unique, range_max=range_max, seed=seed, seed2=seed2,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  num_true = _execute.make_int(num_true, "num_true")
  num_sampled = _execute.make_int(num_sampled, "num_sampled")
  unique = _execute.make_bool(unique, "unique")
  range_max = _execute.make_int(range_max, "range_max")
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  _, _, _op = _op_def_lib._apply_op_helper(
        "LearnedUnigramCandidateSampler", true_classes=true_classes,
                                          num_true=num_true,
                                          num_sampled=num_sampled,
                                          unique=unique, range_max=range_max,
                                          seed=seed, seed2=seed2, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("num_true", _op.get_attr("num_true"), "num_sampled",
            _op.get_attr("num_sampled"), "unique", _op.get_attr("unique"),
            "range_max", _op.get_attr("range_max"), "seed",
            _op.get_attr("seed"), "seed2", _op.get_attr("seed2"))
  _execute.record_gradient(
      "LearnedUnigramCandidateSampler", _inputs_flat, _attrs, _result, name)
  _result = _LearnedUnigramCandidateSamplerOutput._make(_result)
  return _result



def learned_unigram_candidate_sampler_eager_fallback(true_classes, num_true, num_sampled, unique, range_max, seed=0, seed2=0, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function learned_unigram_candidate_sampler
  """
  _ctx = ctx if ctx else _context.context()
  num_true = _execute.make_int(num_true, "num_true")
  num_sampled = _execute.make_int(num_sampled, "num_sampled")
  unique = _execute.make_bool(unique, "unique")
  range_max = _execute.make_int(range_max, "range_max")
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  true_classes = _ops.convert_to_tensor(true_classes, _dtypes.int64)
  _inputs_flat = [true_classes]
  _attrs = ("num_true", num_true, "num_sampled", num_sampled, "unique",
  unique, "range_max", range_max, "seed", seed, "seed2", seed2)
  _result = _execute.execute(b"LearnedUnigramCandidateSampler", 3,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "LearnedUnigramCandidateSampler", _inputs_flat, _attrs, _result, name)
  _result = _LearnedUnigramCandidateSamplerOutput._make(_result)
  return _result


_log_uniform_candidate_sampler_outputs = ["sampled_candidates",
                                         "true_expected_count",
                                         "sampled_expected_count"]
_LogUniformCandidateSamplerOutput = _collections.namedtuple(
    "LogUniformCandidateSampler", _log_uniform_candidate_sampler_outputs)


def log_uniform_candidate_sampler(true_classes, num_true, num_sampled, unique, range_max, seed=0, seed2=0, name=None):
  r"""Generates labels for candidate sampling with a log-uniform distribution.

  See explanations of candidate sampling and the data formats at
  go/candidate-sampling.

  For each batch, this op picks a single set of sampled candidate labels.

  The advantages of sampling candidates per-batch are simplicity and the
  possibility of efficient dense matrix multiplication. The disadvantage is that
  the sampled candidates must be chosen independently of the context and of the
  true labels.

  Args:
    true_classes: A `Tensor` of type `int64`.
      A batch_size * num_true matrix, in which each row contains the
      IDs of the num_true target_classes in the corresponding original label.
    num_true: An `int` that is `>= 1`. Number of true labels per context.
    num_sampled: An `int` that is `>= 1`.
      Number of candidates to randomly sample.
    unique: A `bool`.
      If unique is true, we sample with rejection, so that all sampled
      candidates in a batch are unique. This requires some approximation to
      estimate the post-rejection sampling probabilities.
    range_max: An `int` that is `>= 1`.
      The sampler will sample integers from the interval [0, range_max).
    seed: An optional `int`. Defaults to `0`.
      If either seed or seed2 are set to be non-zero, the random number
      generator is seeded by the given seed.  Otherwise, it is seeded by a
      random seed.
    seed2: An optional `int`. Defaults to `0`.
      An second seed to avoid seed collision.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (sampled_candidates, true_expected_count, sampled_expected_count).

    sampled_candidates: A `Tensor` of type `int64`.
    true_expected_count: A `Tensor` of type `float32`.
    sampled_expected_count: A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "LogUniformCandidateSampler", name, _ctx._post_execution_callbacks,
        true_classes, "num_true", num_true, "num_sampled", num_sampled,
        "unique", unique, "range_max", range_max, "seed", seed, "seed2",
        seed2)
      _result = _LogUniformCandidateSamplerOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return log_uniform_candidate_sampler_eager_fallback(
            true_classes, num_true=num_true, num_sampled=num_sampled,
            unique=unique, range_max=range_max, seed=seed, seed2=seed2,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  num_true = _execute.make_int(num_true, "num_true")
  num_sampled = _execute.make_int(num_sampled, "num_sampled")
  unique = _execute.make_bool(unique, "unique")
  range_max = _execute.make_int(range_max, "range_max")
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  _, _, _op = _op_def_lib._apply_op_helper(
        "LogUniformCandidateSampler", true_classes=true_classes,
                                      num_true=num_true,
                                      num_sampled=num_sampled, unique=unique,
                                      range_max=range_max, seed=seed,
                                      seed2=seed2, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("num_true", _op.get_attr("num_true"), "num_sampled",
            _op.get_attr("num_sampled"), "unique", _op.get_attr("unique"),
            "range_max", _op.get_attr("range_max"), "seed",
            _op.get_attr("seed"), "seed2", _op.get_attr("seed2"))
  _execute.record_gradient(
      "LogUniformCandidateSampler", _inputs_flat, _attrs, _result, name)
  _result = _LogUniformCandidateSamplerOutput._make(_result)
  return _result



def log_uniform_candidate_sampler_eager_fallback(true_classes, num_true, num_sampled, unique, range_max, seed=0, seed2=0, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function log_uniform_candidate_sampler
  """
  _ctx = ctx if ctx else _context.context()
  num_true = _execute.make_int(num_true, "num_true")
  num_sampled = _execute.make_int(num_sampled, "num_sampled")
  unique = _execute.make_bool(unique, "unique")
  range_max = _execute.make_int(range_max, "range_max")
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  true_classes = _ops.convert_to_tensor(true_classes, _dtypes.int64)
  _inputs_flat = [true_classes]
  _attrs = ("num_true", num_true, "num_sampled", num_sampled, "unique",
  unique, "range_max", range_max, "seed", seed, "seed2", seed2)
  _result = _execute.execute(b"LogUniformCandidateSampler", 3,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "LogUniformCandidateSampler", _inputs_flat, _attrs, _result, name)
  _result = _LogUniformCandidateSamplerOutput._make(_result)
  return _result


_thread_unsafe_unigram_candidate_sampler_outputs = ["sampled_candidates",
                                                   "true_expected_count",
                                                   "sampled_expected_count"]
_ThreadUnsafeUnigramCandidateSamplerOutput = _collections.namedtuple(
    "ThreadUnsafeUnigramCandidateSampler",
    _thread_unsafe_unigram_candidate_sampler_outputs)


def thread_unsafe_unigram_candidate_sampler(true_classes, num_true, num_sampled, unique, range_max, seed=0, seed2=0, name=None):
  r"""Generates labels for candidate sampling with a learned unigram distribution.

  See explanations of candidate sampling and the data formats at
  go/candidate-sampling.

  For each batch, this op picks a single set of sampled candidate labels.

  The advantages of sampling candidates per-batch are simplicity and the
  possibility of efficient dense matrix multiplication. The disadvantage is that
  the sampled candidates must be chosen independently of the context and of the
  true labels.

  Args:
    true_classes: A `Tensor` of type `int64`.
      A batch_size * num_true matrix, in which each row contains the
      IDs of the num_true target_classes in the corresponding original label.
    num_true: An `int` that is `>= 1`. Number of true labels per context.
    num_sampled: An `int` that is `>= 1`.
      Number of candidates to randomly sample.
    unique: A `bool`.
      If unique is true, we sample with rejection, so that all sampled
      candidates in a batch are unique. This requires some approximation to
      estimate the post-rejection sampling probabilities.
    range_max: An `int` that is `>= 1`.
      The sampler will sample integers from the interval [0, range_max).
    seed: An optional `int`. Defaults to `0`.
      If either seed or seed2 are set to be non-zero, the random number
      generator is seeded by the given seed.  Otherwise, it is seeded by a
      random seed.
    seed2: An optional `int`. Defaults to `0`.
      An second seed to avoid seed collision.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (sampled_candidates, true_expected_count, sampled_expected_count).

    sampled_candidates: A `Tensor` of type `int64`.
    true_expected_count: A `Tensor` of type `float32`.
    sampled_expected_count: A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ThreadUnsafeUnigramCandidateSampler", name,
        _ctx._post_execution_callbacks, true_classes, "num_true", num_true,
        "num_sampled", num_sampled, "unique", unique, "range_max", range_max,
        "seed", seed, "seed2", seed2)
      _result = _ThreadUnsafeUnigramCandidateSamplerOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return thread_unsafe_unigram_candidate_sampler_eager_fallback(
            true_classes, num_true=num_true, num_sampled=num_sampled,
            unique=unique, range_max=range_max, seed=seed, seed2=seed2,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  num_true = _execute.make_int(num_true, "num_true")
  num_sampled = _execute.make_int(num_sampled, "num_sampled")
  unique = _execute.make_bool(unique, "unique")
  range_max = _execute.make_int(range_max, "range_max")
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ThreadUnsafeUnigramCandidateSampler", true_classes=true_classes,
                                               num_true=num_true,
                                               num_sampled=num_sampled,
                                               unique=unique,
                                               range_max=range_max, seed=seed,
                                               seed2=seed2, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("num_true", _op.get_attr("num_true"), "num_sampled",
            _op.get_attr("num_sampled"), "unique", _op.get_attr("unique"),
            "range_max", _op.get_attr("range_max"), "seed",
            _op.get_attr("seed"), "seed2", _op.get_attr("seed2"))
  _execute.record_gradient(
      "ThreadUnsafeUnigramCandidateSampler", _inputs_flat, _attrs, _result, name)
  _result = _ThreadUnsafeUnigramCandidateSamplerOutput._make(_result)
  return _result



def thread_unsafe_unigram_candidate_sampler_eager_fallback(true_classes, num_true, num_sampled, unique, range_max, seed=0, seed2=0, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function thread_unsafe_unigram_candidate_sampler
  """
  _ctx = ctx if ctx else _context.context()
  num_true = _execute.make_int(num_true, "num_true")
  num_sampled = _execute.make_int(num_sampled, "num_sampled")
  unique = _execute.make_bool(unique, "unique")
  range_max = _execute.make_int(range_max, "range_max")
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  true_classes = _ops.convert_to_tensor(true_classes, _dtypes.int64)
  _inputs_flat = [true_classes]
  _attrs = ("num_true", num_true, "num_sampled", num_sampled, "unique",
  unique, "range_max", range_max, "seed", seed, "seed2", seed2)
  _result = _execute.execute(b"ThreadUnsafeUnigramCandidateSampler", 3,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ThreadUnsafeUnigramCandidateSampler", _inputs_flat, _attrs, _result, name)
  _result = _ThreadUnsafeUnigramCandidateSamplerOutput._make(_result)
  return _result


_uniform_candidate_sampler_outputs = ["sampled_candidates",
                                     "true_expected_count",
                                     "sampled_expected_count"]
_UniformCandidateSamplerOutput = _collections.namedtuple(
    "UniformCandidateSampler", _uniform_candidate_sampler_outputs)


def uniform_candidate_sampler(true_classes, num_true, num_sampled, unique, range_max, seed=0, seed2=0, name=None):
  r"""Generates labels for candidate sampling with a uniform distribution.

  See explanations of candidate sampling and the data formats at
  go/candidate-sampling.

  For each batch, this op picks a single set of sampled candidate labels.

  The advantages of sampling candidates per-batch are simplicity and the
  possibility of efficient dense matrix multiplication. The disadvantage is that
  the sampled candidates must be chosen independently of the context and of the
  true labels.

  Args:
    true_classes: A `Tensor` of type `int64`.
      A batch_size * num_true matrix, in which each row contains the
      IDs of the num_true target_classes in the corresponding original label.
    num_true: An `int` that is `>= 1`. Number of true labels per context.
    num_sampled: An `int` that is `>= 1`.
      Number of candidates to randomly sample.
    unique: A `bool`.
      If unique is true, we sample with rejection, so that all sampled
      candidates in a batch are unique. This requires some approximation to
      estimate the post-rejection sampling probabilities.
    range_max: An `int` that is `>= 1`.
      The sampler will sample integers from the interval [0, range_max).
    seed: An optional `int`. Defaults to `0`.
      If either seed or seed2 are set to be non-zero, the random number
      generator is seeded by the given seed.  Otherwise, it is seeded by a
      random seed.
    seed2: An optional `int`. Defaults to `0`.
      An second seed to avoid seed collision.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (sampled_candidates, true_expected_count, sampled_expected_count).

    sampled_candidates: A `Tensor` of type `int64`.
    true_expected_count: A `Tensor` of type `float32`.
    sampled_expected_count: A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "UniformCandidateSampler", name, _ctx._post_execution_callbacks,
        true_classes, "num_true", num_true, "num_sampled", num_sampled,
        "unique", unique, "range_max", range_max, "seed", seed, "seed2",
        seed2)
      _result = _UniformCandidateSamplerOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return uniform_candidate_sampler_eager_fallback(
            true_classes, num_true=num_true, num_sampled=num_sampled,
            unique=unique, range_max=range_max, seed=seed, seed2=seed2,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  num_true = _execute.make_int(num_true, "num_true")
  num_sampled = _execute.make_int(num_sampled, "num_sampled")
  unique = _execute.make_bool(unique, "unique")
  range_max = _execute.make_int(range_max, "range_max")
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  _, _, _op = _op_def_lib._apply_op_helper(
        "UniformCandidateSampler", true_classes=true_classes,
                                   num_true=num_true, num_sampled=num_sampled,
                                   unique=unique, range_max=range_max,
                                   seed=seed, seed2=seed2, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("num_true", _op.get_attr("num_true"), "num_sampled",
            _op.get_attr("num_sampled"), "unique", _op.get_attr("unique"),
            "range_max", _op.get_attr("range_max"), "seed",
            _op.get_attr("seed"), "seed2", _op.get_attr("seed2"))
  _execute.record_gradient(
      "UniformCandidateSampler", _inputs_flat, _attrs, _result, name)
  _result = _UniformCandidateSamplerOutput._make(_result)
  return _result



def uniform_candidate_sampler_eager_fallback(true_classes, num_true, num_sampled, unique, range_max, seed=0, seed2=0, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function uniform_candidate_sampler
  """
  _ctx = ctx if ctx else _context.context()
  num_true = _execute.make_int(num_true, "num_true")
  num_sampled = _execute.make_int(num_sampled, "num_sampled")
  unique = _execute.make_bool(unique, "unique")
  range_max = _execute.make_int(range_max, "range_max")
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  true_classes = _ops.convert_to_tensor(true_classes, _dtypes.int64)
  _inputs_flat = [true_classes]
  _attrs = ("num_true", num_true, "num_sampled", num_sampled, "unique",
  unique, "range_max", range_max, "seed", seed, "seed2", seed2)
  _result = _execute.execute(b"UniformCandidateSampler", 3,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "UniformCandidateSampler", _inputs_flat, _attrs, _result, name)
  _result = _UniformCandidateSamplerOutput._make(_result)
  return _result

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "AllCandidateSampler"
#   input_arg {
#     name: "true_classes"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "sampled_candidates"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "true_expected_count"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "sampled_expected_count"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "num_true"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "num_sampled"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "unique"
#     type: "bool"
#   }
#   attr {
#     name: "seed"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   attr {
#     name: "seed2"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "ComputeAccidentalHits"
#   input_arg {
#     name: "true_classes"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "sampled_candidates"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "indices"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "ids"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "weights"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "num_true"
#     type: "int"
#   }
#   attr {
#     name: "seed"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   attr {
#     name: "seed2"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
# }
# op {
#   name: "FixedUnigramCandidateSampler"
#   input_arg {
#     name: "true_classes"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "sampled_candidates"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "true_expected_count"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "sampled_expected_count"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "num_true"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "num_sampled"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "unique"
#     type: "bool"
#   }
#   attr {
#     name: "range_max"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "vocab_file"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "distortion"
#     type: "float"
#     default_value {
#       f: 1
#     }
#   }
#   attr {
#     name: "num_reserved_ids"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   attr {
#     name: "num_shards"
#     type: "int"
#     default_value {
#       i: 1
#     }
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "shard"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "unigrams"
#     type: "list(float)"
#     default_value {
#       list {
#       }
#     }
#   }
#   attr {
#     name: "seed"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   attr {
#     name: "seed2"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "LearnedUnigramCandidateSampler"
#   input_arg {
#     name: "true_classes"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "sampled_candidates"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "true_expected_count"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "sampled_expected_count"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "num_true"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "num_sampled"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "unique"
#     type: "bool"
#   }
#   attr {
#     name: "range_max"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "seed"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   attr {
#     name: "seed2"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "LogUniformCandidateSampler"
#   input_arg {
#     name: "true_classes"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "sampled_candidates"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "true_expected_count"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "sampled_expected_count"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "num_true"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "num_sampled"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "unique"
#     type: "bool"
#   }
#   attr {
#     name: "range_max"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "seed"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   attr {
#     name: "seed2"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "ThreadUnsafeUnigramCandidateSampler"
#   input_arg {
#     name: "true_classes"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "sampled_candidates"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "true_expected_count"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "sampled_expected_count"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "num_true"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "num_sampled"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "unique"
#     type: "bool"
#   }
#   attr {
#     name: "range_max"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "seed"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   attr {
#     name: "seed2"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "UniformCandidateSampler"
#   input_arg {
#     name: "true_classes"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "sampled_candidates"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "true_expected_count"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "sampled_expected_count"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "num_true"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "num_sampled"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "unique"
#     type: "bool"
#   }
#   attr {
#     name: "range_max"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "seed"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   attr {
#     name: "seed2"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   is_stateful: true
# }
_op_def_lib = _InitOpDefLibrary(b"\n\327\001\n\023AllCandidateSampler\022\020\n\014true_classes\030\t\032\026\n\022sampled_candidates\030\t\032\027\n\023true_expected_count\030\001\032\032\n\026sampled_expected_count\030\001\"\023\n\010num_true\022\003int(\0010\001\"\026\n\013num_sampled\022\003int(\0010\001\"\016\n\006unique\022\004bool\"\017\n\004seed\022\003int\032\002\030\000\"\020\n\005seed2\022\003int\032\002\030\000\210\001\001\n\230\001\n\025ComputeAccidentalHits\022\020\n\014true_classes\030\t\022\026\n\022sampled_candidates\030\t\032\013\n\007indices\030\003\032\007\n\003ids\030\t\032\013\n\007weights\030\001\"\017\n\010num_true\022\003int\"\017\n\004seed\022\003int\032\002\030\000\"\020\n\005seed2\022\003int\032\002\030\000\n\225\003\n\034FixedUnigramCandidateSampler\022\020\n\014true_classes\030\t\032\026\n\022sampled_candidates\030\t\032\027\n\023true_expected_count\030\001\032\032\n\026sampled_expected_count\030\001\"\023\n\010num_true\022\003int(\0010\001\"\026\n\013num_sampled\022\003int(\0010\001\"\016\n\006unique\022\004bool\"\024\n\trange_max\022\003int(\0010\001\"\030\n\nvocab_file\022\006string\032\002\022\000\"\032\n\ndistortion\022\005float\032\005%\000\000\200?\"\033\n\020num_reserved_ids\022\003int\032\002\030\000\"\031\n\nnum_shards\022\003int\032\002\030\001(\0010\001\"\022\n\005shard\022\003int\032\002\030\000(\001\"\033\n\010unigrams\022\013list(float)\032\002\n\000\"\017\n\004seed\022\003int\032\002\030\000\"\020\n\005seed2\022\003int\032\002\030\000\210\001\001\n\370\001\n\036LearnedUnigramCandidateSampler\022\020\n\014true_classes\030\t\032\026\n\022sampled_candidates\030\t\032\027\n\023true_expected_count\030\001\032\032\n\026sampled_expected_count\030\001\"\023\n\010num_true\022\003int(\0010\001\"\026\n\013num_sampled\022\003int(\0010\001\"\016\n\006unique\022\004bool\"\024\n\trange_max\022\003int(\0010\001\"\017\n\004seed\022\003int\032\002\030\000\"\020\n\005seed2\022\003int\032\002\030\000\210\001\001\n\364\001\n\032LogUniformCandidateSampler\022\020\n\014true_classes\030\t\032\026\n\022sampled_candidates\030\t\032\027\n\023true_expected_count\030\001\032\032\n\026sampled_expected_count\030\001\"\023\n\010num_true\022\003int(\0010\001\"\026\n\013num_sampled\022\003int(\0010\001\"\016\n\006unique\022\004bool\"\024\n\trange_max\022\003int(\0010\001\"\017\n\004seed\022\003int\032\002\030\000\"\020\n\005seed2\022\003int\032\002\030\000\210\001\001\n\375\001\n#ThreadUnsafeUnigramCandidateSampler\022\020\n\014true_classes\030\t\032\026\n\022sampled_candidates\030\t\032\027\n\023true_expected_count\030\001\032\032\n\026sampled_expected_count\030\001\"\023\n\010num_true\022\003int(\0010\001\"\026\n\013num_sampled\022\003int(\0010\001\"\016\n\006unique\022\004bool\"\024\n\trange_max\022\003int(\0010\001\"\017\n\004seed\022\003int\032\002\030\000\"\020\n\005seed2\022\003int\032\002\030\000\210\001\001\n\361\001\n\027UniformCandidateSampler\022\020\n\014true_classes\030\t\032\026\n\022sampled_candidates\030\t\032\027\n\023true_expected_count\030\001\032\032\n\026sampled_expected_count\030\001\"\023\n\010num_true\022\003int(\0010\001\"\026\n\013num_sampled\022\003int(\0010\001\"\016\n\006unique\022\004bool\"\024\n\trange_max\022\003int(\0010\001\"\017\n\004seed\022\003int\032\002\030\000\"\020\n\005seed2\022\003int\032\002\030\000\210\001\001")
