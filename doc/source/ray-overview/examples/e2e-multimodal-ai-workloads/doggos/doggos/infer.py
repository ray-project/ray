import json
import os

from doggos.data import Preprocessor
from doggos.model import ClassificationModel, collate_fn


class TorchPredictor:
    def __init__(self, preprocessor, model):
        self.preprocessor = preprocessor
        self.model = model
        self.model.eval()

    def __call__(self, batch, device="cuda"):
        self.model.to(device)
        batch["prediction"] = self.model.predict(collate_fn(batch, device=device))
        return batch

    def predict_probabilities(self, batch, device="cuda"):
        self.model.to(device)
        predicted_probabilities = self.model.predict_probabilities(collate_fn(batch, device=device))
        batch["probabilities"] = [
            {
                self.preprocessor.label_to_class[i]: float(prob)
                for i, prob in enumerate(probabilities)
            }
            for probabilities in predicted_probabilities
        ]
        return batch

    @classmethod
    def from_artifacts_dir(cls, artifacts_dir):
        with open(os.path.join(artifacts_dir, "class_to_label.json"), "r") as fp:
            class_to_label = json.load(fp)
        preprocessor = Preprocessor(class_to_label=class_to_label)
        model = ClassificationModel.load(
            args_fp=os.path.join(artifacts_dir, "args.json"),
            state_dict_fp=os.path.join(artifacts_dir, "model.pt"),
        )
        return cls(preprocessor=preprocessor, model=model)
