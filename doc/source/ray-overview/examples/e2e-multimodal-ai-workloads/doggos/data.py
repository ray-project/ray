import json

from doggos.embed import EmbeddingGenerator


class Preprocessor:
    """Preprocessor class."""

    def __init__(self, class_to_label=None):
        self.class_to_label = class_to_label or {}  # mutable defaults
        self.label_to_class = {v: k for k, v in self.class_to_label.items()}

    def fit(self, ds, column):
        self.classes = ds.unique(column=column)
        self.class_to_label = {tag: i for i, tag in enumerate(self.classes)}
        self.label_to_class = {v: k for k, v in self.class_to_label.items()}
        return self

    def convert_to_label(self, row, class_to_label):
        if "class" in row:
            row["label"] = class_to_label[row["class"]]
        return row

    def transform(self, ds, concurrency=4, batch_size=64, num_gpus=1):
        ds = ds.map(
            self.convert_to_label,
            fn_kwargs={"class_to_label": self.class_to_label},
        )
        ds = ds.map_batches(
            EmbeddingGenerator,
            fn_constructor_kwargs={"model_id": "openai/clip-vit-base-patch32"},
            fn_kwargs={"device": "cuda"},
            concurrency=concurrency,
            batch_size=batch_size,
            num_gpus=num_gpus,
            accelerator_type="L4",
        )
        ds = ds.drop_columns(["image"])
        return ds

    def save(self, fp):
        with open(fp, "w") as f:
            json.dump(self.class_to_label, f)
