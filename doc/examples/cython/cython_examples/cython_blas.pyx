#!python
# cython: embedsignature=True, binding=True

#  Copyright 2016 Intel Corporation
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

# Authors: Yida Wang
# (Intel Labs), 2016

cimport scipy.linalg.cython_blas as blas


def compute_self_corr_for_voxel_sel(py_trans_a, py_trans_b, py_m, py_n, py_k,
                                    py_alpha, py_a, py_lda, int py_start_voxel,
                                    py_b, py_ldb, py_beta, py_c, py_ldc,
                                    int py_start_epoch):
    """ use blas API sgemm wrapped by scipy to compute correlation

    This method is limited to process self-correlation.
    The blas APIs process matrices in column-major,
    but our matrices are in row-major,
    so we play the transpose trick here, i.e. A*B=(B^T*A^T)^T.
    The resulting matrix in shape [num_assigned_voxels, num_voxels]
    is stored in an alternate way to make sure that
    the correlation vectors of the same voxel stored continuously

    Parameters
    ----------
    py_trans_a: str
    do transpose or not for the first matrix A

    py_trans_b: str
    do transpose or not for the first matrix B

    py_m: int
    the row of the resulting matrix C
    in our case, is num_voxels

    py_n: int
    the column of the resulting matrix C
    in our case, is num_assigned_voxels

    py_k: int
    the collapsed dimension of the multiplying matrices
    i.e. the column of the first matrix after transpose if necessary
    the row of the second matrix after transpose if necessary

    py_alpha: float
    the weight applied to the first matrix A

    py_a: 2D array in shape [epoch_length, num_voxels]
    It is the activity data of an epoch, part 1 of the data to be
    correlated with. Note that py_a can point to the same location of py_b.

    py_lda: int
    the stride of the first matrix A

    py_start_voxel: int
    the starting voxel of assigned voxels
    used to locate the second matrix B

    py_b: 2D array in shape [epoch_length, num_voxels]
    It is the activity data of an epoch, part 2 of the data to be
    correlated with. Note that py_a can point to the same location of py_b.

    py_ldb: int
    the stride of the second matrix B

    py_beta: float
    the weight applied to the resulting matrix C

    py_c: 3D array in shape [num_selected_voxels, num_epochs, num_voxels]
    place to store the resulting correlation values

    py_ldc: int
    the stride of the resulting matrix
    in our case, num_voxels*num_epochs

    py_start_epoch: int
    the epoch over which the correlation is computed

    Returns
    -------
    py_c: 3D array in shape [num_selected_voxels, num_epochs, num_voxels]
    write the resulting correlation values in an alternate way
    for the processing epoch
    """
    cdef bytes by_trans_a=py_trans_a.encode()
    cdef bytes by_trans_b=py_trans_b.encode()
    cdef char* trans_a = by_trans_a
    cdef char* trans_b = by_trans_b
    cdef int M, N, K, lda, ldb, ldc
    M = py_m
    N = py_n
    K = py_k
    lda = py_lda
    ldb = py_ldb
    ldc = py_ldc
    cdef float alpha, beta
    alpha = py_alpha
    beta = py_beta
    cdef float[:, ::1] A
    A = py_a
    cdef float[:, ::1] B
    B = py_b
    cdef float[:, :, ::1] C
    C = py_c
    blas.sgemm(trans_a, trans_b, &M, &N, &K, &alpha, &A[0, 0], &lda,
               &B[0, py_start_voxel], &ldb, &beta,
               &C[0, py_start_epoch, 0], &ldc)


def compute_kernel_matrix(py_uplo, py_trans, py_n, py_k, py_alpha, py_a,
                          int py_start_voxel, py_lda,
                          py_beta, py_c, py_ldc):
    """ use blas API syrk wrapped by scipy to compute kernel matrix of SVM

    The blas APIs process matrices in column-major, but our matrices are
    in row-major, so we play the transpose trick here, i.e. A*B=(B^T*A^T)^T

    In SVM with linear kernel, the distance of two samples
    is essentially the dot product of them.
    Therefore, the kernel matrix can be obtained by matrix multiplication.
    Since the kernel matrix is symmetric, ssyrk is used,
    the other half of the matrix is assigned later.
    In our case, the dimension of samples is much larger than
    the number samples, so we proportionally shrink the values of
    the kernel matrix for getting more robust alpha values in SVM iteration.

    Parameters
    ----------
    py_uplo: str
    getting the upper or lower triangle of the matrix

    py_trans: str
    do transpose or not for the input matrix A

    py_n: int
    the row and column of the resulting matrix C
    in our case, is num_epochs

    py_k: int
    the collapsed dimension of the multiplying matrices
    i.e. the column of the first matrix after transpose if necessary
    the row of the second matrix after transpose if necessary
    in our case, is num_voxels

    py_alpha: float
    the weight applied to the input matrix A

    py_a: 3D array in shape [num_assigned_voxels, num_epochs, num_voxels]
    in our case the normalized correlation values of a voxel

    py_start_voxel: int
    the processed voxel
    used to locate the input matrix A

    py_lda: int
    the stride of the input matrix A

    py_beta: float
    the weight applied to the resulting matrix C

    py_c: 2D array in shape [num_epochs, num_epochs]
    place to store the resulting kernel matrix

    py_ldc: int
    the stride of the resulting matrix

    Returns
    -------
    py_c: 2D array in shape [num_epochs, num_epochs]
    write the resulting kernel_matrix
    for the processing voxel
    """
    cdef bytes by_uplo=py_uplo.encode()
    cdef bytes by_trans=py_trans.encode()
    cdef char* uplo = by_uplo
    cdef char* trans = by_trans
    cdef int N, K, lda, ldc
    N = py_n
    K = py_k
    lda = py_lda
    ldc = py_ldc
    cdef float alpha, beta
    alpha = py_alpha
    beta = py_beta
    cdef float[:, :, ::1] A
    A = py_a
    cdef float[:, ::1] C
    C = py_c
    blas.ssyrk(uplo, trans, &N, &K, &alpha, &A[py_start_voxel, 0, 0], &lda,
               &beta, &C[0, 0], &ldc)
    # complete the other half of the kernel matrix
    if py_uplo == 'L':
        for j in range(py_c.shape[0]):
            for k in range(j):
                py_c[j, k] = py_c[k, j]
    else:
        for j in range(py_c.shape[0]):
            for k in range(j):
                py_c[k, j] = py_c[j, k]


def compute_single_self_corr_syrk(py_uplo, py_trans, py_n, py_k,
                                  py_alpha, py_a, py_lda,
                                  py_beta, py_c, py_ldc,
                                  int py_start_sample):
    """ use blas API syrk wrapped by scipy to compute correlation matrix

    This is to compute the correlation between selected voxels for
    final training and classification. Since the resulting correlation
    matrix is symmetric, syrk is used. However, it looks like that in most
    cases, syrk performs much worse than gemm (the next function).
    Here we assume that the resulting matrix is stored in a compact way,
    i.e. py_ldc == py_n.

    Parameters
    ----------
    py_uplo: str
    getting the upper or lower triangle of the matrix

    py_trans: str
    do transpose or not for the input matrix A

    py_n: int
    the row and column of the resulting matrix C
    in our case, is num_selected_voxels

    py_k: int
    the collapsed dimension of the multiplying matrices
    i.e. the column of the first matrix after transpose if necessary
    the row of the second matrix after transpose if necessary
    in our case, is num_TRs

    py_alpha: float
    the weight applied to the input matrix A

    py_a: 2D array in shape [num_TRs, num_selected_voxels]
    in our case the normalized activity values

    py_lda: int
    the stride of the input matrix A

    py_beta: float
    the weight applied to the resulting matrix C

    py_c: 3D array
    in shape [num_samples, num_selected_voxels, num_selected_voxels]
    place to store the resulting kernel matrix

    py_ldc: int
    the stride of the resulting matrix

    py_start_sample: int
    the processed sample
    used to locate the resulting matrix C

    Returns
    -------
    py_c: 3D array
    in shape [num_samples, num_selected_voxels, num_selected_voxels]
    write the resulting correlation matrices
    for the processed sample
    """
    cdef bytes by_uplo=py_uplo.encode()
    cdef bytes by_trans=py_trans.encode()
    cdef char* uplo = by_uplo
    cdef char* trans = by_trans
    cdef int N, K, lda, ldc
    N = py_n
    K = py_k
    lda = py_lda
    ldc = py_ldc
    cdef float alpha, beta
    alpha = py_alpha
    beta = py_beta
    cdef float[:, ::1] A
    A = py_a
    cdef float[:, :, ::1] C
    C = py_c
    blas.ssyrk(uplo, trans, &N, &K, &alpha, &A[0, 0], &lda,
               &beta, &C[py_start_sample, 0, 0], &ldc)
    # complete the other half of the kernel matrix
    if py_uplo == 'L':
        for j in range(py_c.shape[1]):
            for k in range(j):
                py_c[py_start_sample, j, k] = py_c[py_start_sample, k, j]
    else:
        for j in range(py_c.shape[1]):
            for k in range(j):
                py_c[py_start_sample, k, j] = py_c[py_start_sample, j, k]


def compute_single_self_corr_gemm(py_trans_a, py_trans_b, py_m, py_n,
                                  py_k, py_alpha, py_a, py_lda,
                                  py_ldb, py_beta, py_c, py_ldc,
                                  int py_start_sample):
    """ use blas API gemm wrapped by scipy to compute correlation matrix

    This is to compute the correlation between selected voxels for
    final training and classification. Although the resulting correlation
    matrix is symmetric, in most cases, gemm performs better than syrk.
    Here we assume that the resulting matrix is stored in a compact way,
    i.e. py_ldc == py_n.

    Parameters
    ----------
    py_trans_a: str
    do transpose or not for the first matrix A

    py_trans_b: str
    do transpose or not for the first matrix B

    py_m: int
    the row of the resulting matrix C
    in our case, is num_selected_voxels

    py_n: int
    the column of the resulting matrix C
    in our case, is num_selected_voxels

    py_k: int
    the collapsed dimension of the multiplying matrices
    i.e. the column of the first matrix after transpose if necessary
    the row of the second matrix after transpose if necessary
    in our case, is num_TRs

    py_alpha: float
    the weight applied to the input matrix A

    py_a: 2D array in shape [num_TRs, num_selected_voxels]
    in our case the normalized activity values
    both multipliers are specified here as the same one

    py_lda: int
    the stride of the input matrix A

    py_ldb: int
    the stride of the input matrix B
    in our case, the same as py_lda

    py_beta: float
    the weight applied to the resulting matrix C

    py_c: 3D array
    in shape [num_samples, num_selected_voxels, num_selected_voxels]
    place to store the resulting kernel matrix

    py_ldc: int
    the stride of the resulting matrix

    py_start_sample: int
    the processed sample
    used to locate the resulting matrix C

    Returns
    -------
    py_c: 3D array
    in shape [num_samples, num_selected_voxels, num_selected_voxels]
    write the resulting correlation matrices
    for the processed sample
    """
    cdef bytes by_trans_a=py_trans_a.encode()
    cdef bytes by_trans_b=py_trans_b.encode()
    cdef char* trans_a = by_trans_a
    cdef char* trans_b = by_trans_b
    cdef int M, N, K, lda, ldb, ldc
    M = py_m
    N = py_n
    K = py_k
    lda = py_lda
    ldb = py_ldb
    ldc = py_ldc
    cdef float alpha, beta
    alpha = py_alpha
    beta = py_beta
    cdef float[:, ::1] A
    A = py_a
    cdef float[:, :, ::1] C
    C = py_c
    blas.sgemm(trans_a, trans_b, &M, &N, &K, &alpha, &A[0, 0], &lda,
               &A[0, 0], &ldb, &beta, &C[py_start_sample, 0, 0], &ldc)


def compute_corr_vectors(py_trans_a, py_trans_b, py_m, py_n,
                         py_k, py_alpha, py_a, py_lda,
                         py_b, py_ldb, py_beta, py_c, py_ldc,
                         int py_start_voxel,
                         int py_start_sample):
    """ use blas API gemm wrapped by scipy to construct a correlation vector

    The correlation vector is essentially correlation matrices computed
    from two activity matrices. It will be placed in the corresponding place
    of the resulting correlation data set.
    The blas APIs process matrices in column-major,
    but our matrices are in row-major, so we play the transpose trick here,
    i.e. A*B=(B^T*A^T)^T

    py_trans_a: str
    do transpose or not for the first matrix A

    py_trans_b: str
    do transpose or not for the first matrix B

    py_m: int
    the row of the resulting matrix C

    py_n: int
    the column of the resulting matrix C

    py_k: int
    the collapsed dimension of the multiplying matrices
    i.e. the column of the first matrix after transpose if necessary
    the row of the second matrix after transpose if necessary

    py_alpha: float
    the weight applied to the input matrix A

    py_a: 2D array

    py_lda: int
    the stride of the input matrix A

    py_b: 2D array

    py_ldb: int
    the stride of the input matrix B

    py_beta: float
    the weight applied to the resulting matrix C

    py_c: 2D array
    in shape [py_m, py_n] of column-major
    in fact it is
    in shape [py_n, py_m] of row-major

    py_ldc: int
    the stride of the resulting matrix

    py_start_voxel: int
    the starting voxel of assigned voxels
    used to locate the second matrix B

    py_start_sample: int
    the processed sample
    used to locate the resulting matrix C

    Returns
    -------
    py_c: 2D array
    in shape [py_m, py_n] of column-major
    write the resulting matrix to the place indicated by py_start_sample
    """
    cdef bytes by_trans_a=py_trans_a.encode()
    cdef bytes by_trans_b=py_trans_b.encode()
    cdef char* trans_a = by_trans_a
    cdef char* trans_b = by_trans_b
    cdef int M, N, K, lda, ldb, ldc
    M = py_m
    N = py_n
    K = py_k
    lda = py_lda
    ldb = py_ldb
    ldc = py_ldc
    cdef float alpha, beta
    alpha = py_alpha
    beta = py_beta
    cdef float[:, ::1] A
    A = py_a
    cdef float[:, ::1] B
    B = py_b
    cdef float[:, :, ::1] C
    C = py_c
    blas.sgemm(trans_a, trans_b, &M, &N, &K, &alpha, &A[0, 0], &lda,
               &B[0, py_start_voxel], &ldb, &beta,
               &C[py_start_sample, 0, 0], &ldc)


def compute_single_matrix_multiplication(py_trans_a, py_trans_b, py_m, py_n,
                                         py_k, py_alpha, py_a, py_lda,
                                         py_b, py_ldb, py_beta, py_c, py_ldc):
    """ use blas API gemm wrapped by scipy to do matrix multiplication

    This is to compute the matrix multiplication.
    The blas APIs process matrices in column-major,
    but our matrices are in row-major, so we play the transpose trick here,
    i.e. A*B=(B^T*A^T)^T

    Parameters
    ----------
    py_trans_a: str
    do transpose or not for the first matrix A

    py_trans_b: str
    do transpose or not for the first matrix B

    py_m: int
    the row of the resulting matrix C

    py_n: int
    the column of the resulting matrix C

    py_k: int
    the collapsed dimension of the multiplying matrices
    i.e. the column of the first matrix after transpose if necessary
    the row of the second matrix after transpose if necessary

    py_alpha: float
    the weight applied to the input matrix A

    py_a: 2D array

    py_lda: int
    the stride of the input matrix A

    py_b: 2D array

    py_ldb: int
    the stride of the input matrix B

    py_beta: float
    the weight applied to the resulting matrix C

    py_c: 2D array
    in shape [py_m, py_n] of column-major
    in fact it is
    in shape [py_n, py_m] of row-major

    py_ldc: int
    the stride of the resulting matrix

    Returns
    -------
    py_c: 2D array
    in shape [py_m, py_n] of column-major
    write the resulting matrix
    """
    cdef bytes by_trans_a=py_trans_a.encode()
    cdef bytes by_trans_b=py_trans_b.encode()
    cdef char* trans_a = by_trans_a
    cdef char* trans_b = by_trans_b
    cdef int M, N, K, lda, ldb, ldc
    M = py_m
    N = py_n
    K = py_k
    lda = py_lda
    ldb = py_ldb
    ldc = py_ldc
    cdef float alpha, beta
    alpha = py_alpha
    beta = py_beta
    cdef float[:, ::1] A
    A = py_a
    cdef float[:, ::1] B
    B = py_b
    cdef float[:, ::1] C
    C = py_c
    blas.sgemm(trans_a, trans_b, &M, &N, &K, &alpha, &A[0, 0], &lda,
               &B[0, 0], &ldb, &beta, &C[0, 0], &ldc)
