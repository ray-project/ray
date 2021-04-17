import os
import subprocess
from itertools import combinations


codes = ["mnist_jax_example.py", "sst_jax_example.py", "mnist_flax_example.py", "wiki_flax_example.py"]

# 2,3,4,5,6, 7,8
num_workers = "--num-workers {}"
num_workers_candidate = range(2,9)

# few
model_names = "--model-name {}"
models_candidate = ["resnet18", "resnet50", "resnet101"]

# strategy
strategy = "--trainer {}"
strategy_candidate = ["ar", "ps"]

woker2cuda = {2: "0,2", 3: "0,2,6"}
# ['0,2,6', '0,3,6']

cuda_list = ['0', '1', '2', '3', '6', '7']

# for trainer in strategy_candidate:
#     for model in models_candidate:
#         cmd = f"NCCL_SHM_DISABLE=1 CUDA_VISIBLE_DEVICES python mnist_jax_example.py --num-epochs 5 --num-workers 2 {model_names.format(model)} {strategy.format(trainer)}")
#         res = result.read()

# cmd = f"CUDA_VISIBLE_DEVICES=0,2,1 python mnist_jax_example.py --num-epochs 5 --num-workers 3"
# # cmd = "conda info --envs"
# res = os.system(cmd)
# print(res)


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


def run_mnist_ar_single_node():
    num_workers = "--num-workers {}"
    num_workers_candidate = range(2,9)

    models = "--model_name {}"
    models_candidate = ["resnet18", "resnet50", "resnet101"]

    cuda_aval = ",".join(cuda_list)

    for workers in num_workers_candidate:
        for model in models_candidate:
            cmd =  f"CUDA_VISIBLE_DEVICES={cuda_aval} NCCL_SHM_DISABLE=1 python mnist_jax_example.py --num-epochs 3 {num_workers.format(workers)} --trainer ar {models.format(model)}"
            res = os.system(cmd)
            if res != 0:
                raise


def run_wiki_ar_single_node():
    num_workers = "--num-workers {}"
    num_workers_candidate = range(2,9)

    cuda_aval = ",".join(cuda_list)

    for workers in num_workers_candidate:
        cmd =  f"CUDA_VISIBLE_DEVICES={cuda_aval} NCCL_SHM_DISABLE=1 python wiki_flax_example.py --num-epochs 3 {num_workers.format(workers)} --trainer ar"
        res = os.system(cmd)
        # if res != 0:
        #     raise

def run_mnist_ps_single_node(num_ps=1):
    num_workers = "--num-workers {}"
    num_workers_candidate = range(1+num_ps,9)

    num_ps_str = f"--num-workers {num_ps}"

    models = "--model_name {}"
    models_candidate = ["resnet18", "resnet50", "resnet101"]

    cuda_aval = ",".join(cuda_list)

    for workers in num_workers_candidate:
        for model in models_candidate:
            cmd =  f"CUDA_VISIBLE_DEVICES={cuda_aval} NCCL_SHM_DISABLE=1 python mnist_jax_example.py --num-epochs 3 {num_workers.format(workers)} {num_ps_str} --trainer ps {models.format(model)}"
            res = os.system(cmd)
            # if res != 0:
            #     raise

def run_wiki_ps_single_node(num_ps=1):
    num_workers = "--num-workers {}"
    num_workers_candidate = range(1+num_ps,9)

    num_ps_str = f"--num-workers {num_ps}"

    cuda_aval = ",".join(cuda_list)

    for workers in num_workers_candidate:
        cmd =  f"CUDA_VISIBLE_DEVICES={cuda_aval} NCCL_SHM_DISABLE=1 python wiki_flax_example.py --num-epochs 3 {num_workers.format(workers)} {num_ps_str} --trainer ps"
        res = os.system(cmd)
            # if res != 0:
            #     raise


if __name__ == "__main__":
    run_mnist_ar_single_node()
    run_mnist_ps_single_node(num_ps=1)
    run_mnist_ps_single_node(num_ps=2)

    run_wiki_ar_single_node()
    run_wiki_ps_single_node(num_ps=1)
    run_wiki_ps_single_node(num_ps=2)