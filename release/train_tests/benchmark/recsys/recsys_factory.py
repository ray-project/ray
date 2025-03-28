from typing import Dict, Iterator, List, Optional
import logging
import time

from pydantic import BaseModel
import torch
import torchvision
from torch.utils.data import IterableDataset

from pyre_extensions import none_throws
from torch import distributed as dist
from torch.utils.data import DataLoader
from torchrec import EmbeddingBagCollection
from torchrec.datasets.criteo import DEFAULT_CAT_NAMES, DEFAULT_INT_NAMES
from torchrec.distributed import TrainPipelineSparseDist
from torchrec.distributed.comm import get_local_size
from torchrec.distributed.model_parallel import (
    DistributedModelParallel,
    get_default_sharders,
)
from torchrec.distributed.planner import EmbeddingShardingPlanner, Topology
from torchrec.distributed.planner.storage_reservations import (
    HeuristicalStorageReservation,
)
from torchrec.models.dlrm import DLRM, DLRM_DCN, DLRM_Projection, DLRMTrain
from torchrec.modules.embedding_configs import EmbeddingBagConfig
from torchrec.optim.apply_optimizer_in_backward import apply_optimizer_in_backward
from torchrec.optim.keyed import CombinedOptimizer, KeyedOptimizerWrapper
from torchrec.optim.optimizers import in_backward_optimizer_filter

import ray.train
import ray.train.torch

from config import DataloaderType, BenchmarkConfig
from factory import BenchmarkFactory
from dataloader_factory import (
    BaseDataLoaderFactory,
)
from ray_dataloader_factory import RayDataLoaderFactory
from recsys.criteo import DEFAULT_CAT_NAMES, convert_to_torchrec_batch_format, get_ray_dataset


logger = logging.getLogger(__name__)


class RecsysMockDataLoaderFactory(BaseDataLoaderFactory):
    def get_train_dataloader(self):
        raise NotImplementedError

    def get_val_dataloader(self):
        raise NotImplementedError


class RecsysRayDataLoaderFactory(RayDataLoaderFactory):
    def get_ray_datasets(self) -> Dict[str, ray.data.Dataset]:
        return {stage: get_ray_dataset(stage) for stage in ("train", "valid")}

    def collate_fn(self, batch):
        return convert_to_torchrec_batch_format(batch)


class TorchRecConfig(BaseModel):
    embedding_dim: int = 128
    num_embeddings_per_feature: List[int] = [40000000,39060,17295,7424,20265,3,7122,1543,63,40000000,3067956,405282,10,2209,11938,155,4,976,14,40000000,40000000,40000000,590152,12973,108,36]
    over_arch_layer_sizes: List[int] = [1024,1024,512,256,1]
    dense_arch_layer_sizes: List[int] = [512,256,128]
    interaction_type: str = "dcn"
    dcn_num_layers: int = 3
    dcn_low_rank_dim: int = 512


class RecsysFactory(BenchmarkFactory):
    def __init__(self, benchmark_config: BenchmarkConfig):
        super().__init__(benchmark_config)

        self.torchrec_config = TorchRecConfig()

    def get_dataloader_factory(self) -> BaseDataLoaderFactory:
        data_factory_cls = {
            DataloaderType.MOCK: RecsysMockDataLoaderFactory,
            DataloaderType.RAY_DATA: RecsysRayDataLoaderFactory,
        }[self.benchmark_config.dataloader_type]

        return data_factory_cls(self.benchmark_config)

    def get_model(self) -> torch.nn.Module:
        args = self.torchrec_config
        device = ray.train.torch.get_device()
        local_world_size = ray.train.get_context().get_local_world_size()
        global_world_size = ray.train.get_context().get_world_size()

        eb_configs = [
            EmbeddingBagConfig(
                name=f"t_{feature_name}",
                embedding_dim=args.embedding_dim,
                num_embeddings=args.num_embeddings_per_feature[feature_idx],
                feature_names=[feature_name],
            )
            for feature_idx, feature_name in enumerate(DEFAULT_CAT_NAMES)
        ]
        sharded_module_kwargs = {}
        if args.over_arch_layer_sizes is not None:
            sharded_module_kwargs["over_arch_layer_sizes"] = args.over_arch_layer_sizes

        if args.interaction_type == "original":
            dlrm_model = DLRM(
                embedding_bag_collection=EmbeddingBagCollection(
                    tables=eb_configs, device=torch.device("meta")
                ),
                dense_in_features=len(DEFAULT_INT_NAMES),
                dense_arch_layer_sizes=args.dense_arch_layer_sizes,
                over_arch_layer_sizes=args.over_arch_layer_sizes,
                dense_device=device,
            )
        elif args.interaction_type == "dcn":
            dlrm_model = DLRM_DCN(
                embedding_bag_collection=EmbeddingBagCollection(
                    tables=eb_configs, device=torch.device("meta")
                ),
                dense_in_features=len(DEFAULT_INT_NAMES),
                dense_arch_layer_sizes=args.dense_arch_layer_sizes,
                over_arch_layer_sizes=args.over_arch_layer_sizes,
                dcn_num_layers=args.dcn_num_layers,
                dcn_low_rank_dim=args.dcn_low_rank_dim,
                dense_device=device,
            )
        elif args.interaction_type == "projection":
            raise NotImplementedError

            dlrm_model = DLRM_Projection(
                embedding_bag_collection=EmbeddingBagCollection(
                    tables=eb_configs, device=torch.device("meta")
                ),
                dense_in_features=len(DEFAULT_INT_NAMES),
                dense_arch_layer_sizes=args.dense_arch_layer_sizes,
                over_arch_layer_sizes=args.over_arch_layer_sizes,
                interaction_branch1_layer_sizes=args.interaction_branch1_layer_sizes,
                interaction_branch2_layer_sizes=args.interaction_branch2_layer_sizes,
                dense_device=device,
            )
        else:
            raise ValueError(
                "Unknown interaction option set. Should be original, dcn, or projection."
            )

        train_model = DLRMTrain(dlrm_model)
        embedding_optimizer = torch.optim.Adagrad
        # This will apply the Adagrad optimizer in the backward pass for the embeddings (sparse_arch). This means that
        # the optimizer update will be applied in the backward pass, in this case through a fused op.
        # TorchRec will use the FBGEMM implementation of EXACT_ADAGRAD. For GPU devices, a fused CUDA kernel is invoked. For CPU, FBGEMM_GPU invokes CPU kernels
        # https://github.com/pytorch/FBGEMM/blob/2cb8b0dff3e67f9a009c4299defbd6b99cc12b8f/fbgemm_gpu/fbgemm_gpu/split_table_batched_embeddings_ops.py#L676-L678

        # Note that lr_decay, weight_decay and initial_accumulator_value for Adagrad optimizer in FBGEMM v0.3.2
        # cannot be specified below. This equivalently means that all these parameters are hardcoded to zero.
        optimizer_kwargs = {"lr": 15.0, "eps": 1e-8}
        apply_optimizer_in_backward(
            embedding_optimizer,
            train_model.model.sparse_arch.parameters(),
            optimizer_kwargs,
        )
        planner = EmbeddingShardingPlanner(
            topology=Topology(
                local_world_size=local_world_size,
                world_size=global_world_size,
                compute_device=device.type,
            ),
            batch_size=self.benchmark_config.dataloader_config.train_batch_size,
            # If experience OOM, increase the percentage. see
            # https://pytorch.org/torchrec/torchrec.distributed.planner.html#torchrec.distributed.planner.storage_reservations.HeuristicalStorageReservation
            storage_reservation=HeuristicalStorageReservation(percentage=0.05),
        )
        plan = planner.collective_plan(
            train_model, get_default_sharders(), dist.GroupMember.WORLD
        )

        model = DistributedModelParallel(
            module=train_model,
            device=device,
            plan=plan,
        )

        if ray.train.get_context().get_world_rank() == 0:
            for collectionkey, plans in model._plan.plan.items():
                print(collectionkey)
                for table_name, plan in plans.items():
                    print(table_name, "\n", plan, "\n")

        return model

    def get_loss_fn(self) -> torch.nn.Module:
        raise NotImplementedError("torchrec model should return the loss directly in forward")