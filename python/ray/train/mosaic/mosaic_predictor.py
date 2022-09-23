from ray.train.torch import TorchPredictor
from ray.train.batch_predictor import BatchPredictor
from ray.air.checkpoint import Checkpoint
from pathlib import Path
import torch


class MosaicPredictor(BatchPredictor):
    @classmethod
    def from_save_path(cls, path: Path, model: torch.nn.Module, strict: bool=False, **predictor_kwargs):
        model_state_dict=torch.load(path)['state']['model']    

        # remove module prefixes when loading the model weights        
        while True:
            prefix_removed=False
            prefix = 'module.'
            keys = sorted(model_state_dict.keys())
            for key in keys:
                if key.startswith(prefix):
                    newkey = key[len(prefix) :]
                    model_state_dict[newkey] = model_state_dict.pop(key)
                    prefix_removed=True
            
            if not prefix_removed:
                break
        model.load_state_dict(model_state_dict, strict=strict)
        checkpoint = Checkpoint.from_dict({"model" : model.state_dict()})
        return cls(checkpoint=checkpoint, predictor_cls=TorchPredictor, model=model, **predictor_kwargs)

    
