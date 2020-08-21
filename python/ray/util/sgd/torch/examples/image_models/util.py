import random
import os
from os.path import join

import numpy as np
import PIL


def mock_data(train_dir, val_dir):
    os.makedirs(train_dir, exist_ok=True)
    os.makedirs(val_dir, exist_ok=True)

    max_cls_n = 99999999
    total_classes = 3
    per_cls = max_cls_n // total_classes

    max_img_n = 99999999
    total_imgs = 3
    per_img = max_img_n // total_imgs

    def mock_class(base, n):
        random_cls = random.randint(per_cls * n, per_cls * n + per_cls)
        sub_dir = join(base, f"n{random_cls:08d}")
        os.makedirs(sub_dir, exist_ok=True)

        for i in range(total_imgs):
            random_img = random.randint(per_img * i, per_img * i + per_img)
            file = join(sub_dir, f"ILSVRC2012_val_{random_img:08d}.JPEG")

            PIL.Image.fromarray(np.zeros((375, 500, 3),
                                         dtype=np.uint8)).save(file)

    existing_train_cls = len(os.listdir(train_dir))
    for i in range(existing_train_cls, total_classes):
        mock_class(train_dir, i)

    existing_val_cls = len(os.listdir(val_dir))
    for i in range(existing_val_cls, total_classes):
        mock_class(val_dir, i)
