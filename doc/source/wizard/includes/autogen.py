import copy
import os

from jinja2 import Environment, FileSystemLoader

# 1. Crawl tf for all files
examples = {}


def collect_files_along_path(path: str, file_tree: dict) -> list:
    files = []
    path = tuple(path.split(os.sep))
    for i in range(len(path)):
        files += file_tree.get(path[: i + 1], [])

    # Include common modules
    common_path = ("modules", "_common") + path[2:]
    for i in range(len(common_path)):
        files += file_tree.get(common_path[: i + 1], [])

    return files


def sep_to_underscores(path):
    assert os.sep in path
    return "_".join(path.split(os.sep))


def convert_path_to_filename(path):
    return sep_to_underscores(path) + ".py"


def get_trainer_cls(example_path: str):
    framework = example_path.split(os.sep)[0]
    framework_to_trainer_cls = {
        "tf": "TensorflowTrainer",
        "torch": "TorchTrainer",
        "xgboost": "XGBoostTrainer",
    }
    return framework_to_trainer_cls[framework]


file_tree = {}
for root, dirs, files in os.walk("modules"):
    path = tuple(root.split(os.sep))
    file_tree[path] = [os.path.join(root, file) for file in files]

    if not dirs:
        # We're at a leaf node = this is a unique example
        rel_root = os.path.relpath(root, "modules")
        module = rel_root.split(os.sep)[0]
        if module.startswith("_"):
            continue
        examples[rel_root] = collect_files_along_path(root, file_tree)


environment = Environment(loader=FileSystemLoader("templates/"))
template = environment.get_template("template.txt")

for example_path, files in examples.items():
    example_filename = convert_path_to_filename(example_path)

    file_contents = {
        file.split(os.sep)[-1].replace(".py", ""): open(file, "r").read()
        for file in files
    }
    print(file_contents.keys())
    content = template.render(
        **file_contents,
        custom_train_loop="train_loop_body" in file_contents,
        trainer_cls=get_trainer_cls(example_path),
        use_ray_data="raydata" in example_path,
    )
    with open(os.path.join("generated", example_filename), "w", encoding="utf-8") as f:
        f.write(content)
        print("Wrote to: ", example_filename)
