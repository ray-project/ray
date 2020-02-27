import numpy as np
import ray.experimental.array.remote as ra
import ray

BLOCK_SIZE = 10


class DistArray:
    def __init__(self, shape, objectids=None):
        self.shape = shape
        self.ndim = len(shape)
        self.num_blocks = [
            int(np.ceil(1.0 * a / BLOCK_SIZE)) for a in self.shape
        ]
        if objectids is not None:
            self.objectids = objectids
        else:
            self.objectids = np.empty(self.num_blocks, dtype=object)
        if self.num_blocks != list(self.objectids.shape):
            raise Exception("The fields `num_blocks` and `objectids` are "
                            "inconsistent, `num_blocks` is {} and `objectids` "
                            "has shape {}".format(self.num_blocks,
                                                  list(self.objectids.shape)))

    @staticmethod
    def compute_block_lower(index, shape):
        if len(index) != len(shape):
            raise Exception("The fields `index` and `shape` must have the "
                            "same length, but `index` is {} and `shape` is "
                            "{}.".format(index, shape))
        return [elem * BLOCK_SIZE for elem in index]

    @staticmethod
    def compute_block_upper(index, shape):
        if len(index) != len(shape):
            raise Exception("The fields `index` and `shape` must have the "
                            "same length, but `index` is {} and `shape` is "
                            "{}.".format(index, shape))
        upper = []
        for i in range(len(shape)):
            upper.append(min((index[i] + 1) * BLOCK_SIZE, shape[i]))
        return upper

    @staticmethod
    def compute_block_shape(index, shape):
        lower = DistArray.compute_block_lower(index, shape)
        upper = DistArray.compute_block_upper(index, shape)
        return [u - l for (l, u) in zip(lower, upper)]

    @staticmethod
    def compute_num_blocks(shape):
        return [int(np.ceil(1.0 * a / BLOCK_SIZE)) for a in shape]

    def assemble(self):
        """Assemble an array from a distributed array of object IDs."""
        first_block = ray.get(self.objectids[(0, ) * self.ndim])
        dtype = first_block.dtype
        result = np.zeros(self.shape, dtype=dtype)
        for index in np.ndindex(*self.num_blocks):
            lower = DistArray.compute_block_lower(index, self.shape)
            upper = DistArray.compute_block_upper(index, self.shape)
            value = ray.get(self.objectids[index])
            result[tuple(slice(l, u) for (l, u) in zip(lower, upper))] = value
        return result

    def __getitem__(self, sliced):
        # TODO(rkn): Fix this, this is just a placeholder that should work but
        # is inefficient.
        a = self.assemble()
        return a[sliced]


@ray.remote
def assemble(a):
    return a.assemble()


# TODO(rkn): What should we call this method?
@ray.remote
def numpy_to_dist(a):
    result = DistArray(a.shape)
    for index in np.ndindex(*result.num_blocks):
        lower = DistArray.compute_block_lower(index, a.shape)
        upper = DistArray.compute_block_upper(index, a.shape)
        idx = tuple(slice(l, u) for (l, u) in zip(lower, upper))
        result.objectids[index] = ray.put(a[idx])
    return result


@ray.remote
def zeros(shape, dtype_name="float"):
    result = DistArray(shape)
    for index in np.ndindex(*result.num_blocks):
        result.objectids[index] = ra.zeros.remote(
            DistArray.compute_block_shape(index, shape), dtype_name=dtype_name)
    return result


@ray.remote
def ones(shape, dtype_name="float"):
    result = DistArray(shape)
    for index in np.ndindex(*result.num_blocks):
        result.objectids[index] = ra.ones.remote(
            DistArray.compute_block_shape(index, shape), dtype_name=dtype_name)
    return result


@ray.remote
def copy(a):
    result = DistArray(a.shape)
    for index in np.ndindex(*result.num_blocks):
        # We don't need to actually copy the objects because remote objects are
        # immutable.
        result.objectids[index] = a.objectids[index]
    return result


@ray.remote
def eye(dim1, dim2=-1, dtype_name="float"):
    dim2 = dim1 if dim2 == -1 else dim2
    shape = [dim1, dim2]
    result = DistArray(shape)
    for (i, j) in np.ndindex(*result.num_blocks):
        block_shape = DistArray.compute_block_shape([i, j], shape)
        if i == j:
            result.objectids[i, j] = ra.eye.remote(
                block_shape[0], block_shape[1], dtype_name=dtype_name)
        else:
            result.objectids[i, j] = ra.zeros.remote(
                block_shape, dtype_name=dtype_name)
    return result


@ray.remote
def triu(a):
    if a.ndim != 2:
        raise Exception("Input must have 2 dimensions, but a.ndim is "
                        "{}.".format(a.ndim))
    result = DistArray(a.shape)
    for (i, j) in np.ndindex(*result.num_blocks):
        if i < j:
            result.objectids[i, j] = ra.copy.remote(a.objectids[i, j])
        elif i == j:
            result.objectids[i, j] = ra.triu.remote(a.objectids[i, j])
        else:
            result.objectids[i, j] = ra.zeros_like.remote(a.objectids[i, j])
    return result


@ray.remote
def tril(a):
    if a.ndim != 2:
        raise Exception("Input must have 2 dimensions, but a.ndim is "
                        "{}.".format(a.ndim))
    result = DistArray(a.shape)
    for (i, j) in np.ndindex(*result.num_blocks):
        if i > j:
            result.objectids[i, j] = ra.copy.remote(a.objectids[i, j])
        elif i == j:
            result.objectids[i, j] = ra.tril.remote(a.objectids[i, j])
        else:
            result.objectids[i, j] = ra.zeros_like.remote(a.objectids[i, j])
    return result


@ray.remote
def blockwise_dot(*matrices):
    n = len(matrices)
    if n % 2 != 0:
        raise Exception("blockwise_dot expects an even number of arguments, "
                        "but len(matrices) is {}.".format(n))
    shape = (matrices[0].shape[0], matrices[n // 2].shape[1])
    result = np.zeros(shape)
    for i in range(n // 2):
        result += np.dot(matrices[i], matrices[n // 2 + i])
    return result


@ray.remote
def dot(a, b):
    if a.ndim != 2:
        raise Exception("dot expects its arguments to be 2-dimensional, but "
                        "a.ndim = {}.".format(a.ndim))
    if b.ndim != 2:
        raise Exception("dot expects its arguments to be 2-dimensional, but "
                        "b.ndim = {}.".format(b.ndim))
    if a.shape[1] != b.shape[0]:
        raise Exception("dot expects a.shape[1] to equal b.shape[0], but "
                        "a.shape = {} and b.shape = {}.".format(
                            a.shape, b.shape))
    shape = [a.shape[0], b.shape[1]]
    result = DistArray(shape)
    for (i, j) in np.ndindex(*result.num_blocks):
        args = list(a.objectids[i, :]) + list(b.objectids[:, j])
        result.objectids[i, j] = blockwise_dot.remote(*args)
    return result


@ray.remote
def subblocks(a, *ranges):
    """
    This function produces a distributed array from a subset of the blocks in
    the `a`. The result and `a` will have the same number of dimensions. For
    example,
        subblocks(a, [0, 1], [2, 4])
    will produce a DistArray whose objectids are
        [[a.objectids[0, 2], a.objectids[0, 4]],
         [a.objectids[1, 2], a.objectids[1, 4]]]
    We allow the user to pass in an empty list [] to indicate the full range.
    """
    ranges = list(ranges)
    if len(ranges) != a.ndim:
        raise Exception("sub_blocks expects to receive a number of ranges "
                        "equal to a.ndim, but it received {} ranges and "
                        "a.ndim = {}.".format(len(ranges), a.ndim))
    for i in range(len(ranges)):
        # We allow the user to pass in an empty list to indicate the full
        # range.
        if ranges[i] == []:
            ranges[i] = range(a.num_blocks[i])
        if not np.alltrue(ranges[i] == np.sort(ranges[i])):
            raise Exception("Ranges passed to sub_blocks must be sorted, but "
                            "the {}th range is {}.".format(i, ranges[i]))
        if ranges[i][0] < 0:
            raise Exception("Values in the ranges passed to sub_blocks must "
                            "be at least 0, but the {}th range is {}.".format(
                                i, ranges[i]))
        if ranges[i][-1] >= a.num_blocks[i]:
            raise Exception("Values in the ranges passed to sub_blocks must "
                            "be less than the relevant number of blocks, but "
                            "the {}th range is {}, and a.num_blocks = {}."
                            .format(i, ranges[i], a.num_blocks))
    last_index = [r[-1] for r in ranges]
    last_block_shape = DistArray.compute_block_shape(last_index, a.shape)
    shape = [(len(ranges[i]) - 1) * BLOCK_SIZE + last_block_shape[i]
             for i in range(a.ndim)]
    result = DistArray(shape)
    for index in np.ndindex(*result.num_blocks):
        result.objectids[index] = a.objectids[tuple(
            ranges[i][index[i]] for i in range(a.ndim))]
    return result


@ray.remote
def transpose(a):
    if a.ndim != 2:
        raise Exception("transpose expects its argument to be 2-dimensional, "
                        "but a.ndim = {}, a.shape = {}.".format(
                            a.ndim, a.shape))
    result = DistArray([a.shape[1], a.shape[0]])
    for i in range(result.num_blocks[0]):
        for j in range(result.num_blocks[1]):
            result.objectids[i, j] = ra.transpose.remote(a.objectids[j, i])
    return result


# TODO(rkn): support broadcasting?
@ray.remote
def add(x1, x2):
    if x1.shape != x2.shape:
        raise Exception("add expects arguments `x1` and `x2` to have the same "
                        "shape, but x1.shape = {}, and x2.shape = {}.".format(
                            x1.shape, x2.shape))
    result = DistArray(x1.shape)
    for index in np.ndindex(*result.num_blocks):
        result.objectids[index] = ra.add.remote(x1.objectids[index],
                                                x2.objectids[index])
    return result


# TODO(rkn): support broadcasting?
@ray.remote
def subtract(x1, x2):
    if x1.shape != x2.shape:
        raise Exception("subtract expects arguments `x1` and `x2` to have the "
                        "same shape, but x1.shape = {}, and x2.shape = {}."
                        .format(x1.shape, x2.shape))
    result = DistArray(x1.shape)
    for index in np.ndindex(*result.num_blocks):
        result.objectids[index] = ra.subtract.remote(x1.objectids[index],
                                                     x2.objectids[index])
    return result
