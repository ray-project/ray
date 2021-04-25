import os
import subprocess
from itertools import combinations


def combine(temp_list, n):
    '''根据n获得列表中的所有可能组合（n个元素为一组）'''
    temp_list2 = []
    for c in combinations(temp_list, n):
        temp_list2.append(c)
    return temp_list2


def find_conbine(cuda_list, num_cuda=1):
    end_list = []
    for i in range(len(list1)):
        end_list.extend(combine(list1, i))

    new_list = []
    for i in end_list:
        if len(i) == num_cuda:
            new_list.append(",".join(i))
    return new_list


def find_wored_device():
    # find the cuda devices without cuda unhandled error
    worked_cuda = []
    num_workers = 4
    for cuda_aval in find_conbine(list1, num_workers):
        cmd =  f"CUDA_VISIBLE_DEVICES={cuda_aval} NCCL_SHM_DISABLE=1 python mnist_jax_example.py --num-epochs 1 --num-workers {num_workers} --smoke-test True"
        print(f"running command `{cmd}`")
        res = os.system(cmd)
        if res == 0:
            worked_cuda.append(cuda_aval)
    print("avaliable cuda list", worked_cuda)

# =================================================================

default_epoches = 5
cuda_list = ['0', '1', '2', '3', '4', '5', '6', '7']


def run_mnist_ar_single_node():
    num_workers = "--num-workers {}"
    num_workers_candidate = range(2, len(cuda_list) + 1)

    models = "--model-name {}"
    models_candidate = ["resnet18", "resnet101"]

    cuda_aval = ",".join(cuda_list)

    for workers in num_workers_candidate:
        for model in models_candidate:
            cmd =  f"CUDA_VISIBLE_DEVICES={cuda_aval} NCCL_SHM_DISABLE=1 python mnist_jax_example.py --num-epochs {default_epoches} {num_workers.format(workers)} --trainer ar {models.format(model)}"
            res = os.system(cmd)


def run_wiki_ar_single_node():
    num_workers = "--num-workers {}"
    num_workers_candidate = range(2, len(cuda_list) + 1)

    cuda_aval = ",".join(cuda_list)

    for workers in num_workers_candidate:
        cmd =  f"CUDA_VISIBLE_DEVICES={cuda_aval} NCCL_SHM_DISABLE=1 python wiki_flax_example.py --num-epochs {default_epoches} {num_workers.format(workers)} --trainer ar"
        res = os.system(cmd)


def run_mnist_ps_single_node(num_ps=1):
    num_workers = "--num-workers {}"
    num_workers_candidate = range(num_ps*2, len(cuda_list) + 1)

    num_ps_str = f"--num-ps {num_ps}"

    models = "--model-name {}"
    models_candidate = ["resnet18", "resnet101"]

    cuda_aval = ",".join(cuda_list)

    for workers in num_workers_candidate:
        for model in models_candidate:
            cmd =  f"CUDA_VISIBLE_DEVICES={cuda_aval} NCCL_SHM_DISABLE=1 python mnist_jax_example.py --num-epochs {default_epoches} {num_workers.format(workers)} {num_ps_str} --trainer ps {models.format(model)}"
            res = os.system(cmd)


def run_wiki_ps_single_node(num_ps=1):
    num_workers = "--num-workers {}"
    num_workers_candidate = range(num_ps*2, len(cuda_list) + 1)

    num_ps_str = f"--num-ps {num_ps}"

    cuda_aval = ",".join(cuda_list)

    for workers in num_workers_candidate:
        cmd =  f"CUDA_VISIBLE_DEVICES={cuda_aval} NCCL_SHM_DISABLE=1 python wiki_flax_example.py --num-epochs {default_epoches} {num_workers.format(workers)} {num_ps_str} --trainer ps"
        res = os.system(cmd)


if __name__ == "__main__":
    run_mnist_ar_single_node()
    run_mnist_ps_single_node(num_ps=1)
    run_mnist_ps_single_node(num_ps=2)

    run_wiki_ar_single_node()
    run_wiki_ps_single_node(num_ps=1)
    run_wiki_ps_single_node(num_ps=2)