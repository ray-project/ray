import os, argparse
import numpy as np
import cv2

def parse_opts():
    parser = argparse.ArgumentParser(description='Preparing Caltech-256 Dataset',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--data', type=str, required=True,
                        help='directory for the original data folder')
    opts = parser.parse_args()
    return opts

# Preparation
opts = parse_opts()

path = opts.data

# Create directories
src_path = os.path.join(path, 'Images')
train_path = os.path.join(path, 'train')
test_path = os.path.join(path, 'val')
os.makedirs(train_path)
os.makedirs(test_path)

labels = sorted(os.listdir(src_path))

for l in labels:
    os.makedirs(os.path.join(train_path, l))
    os.makedirs(os.path.join(test_path, l))

    label_path = os.path.join(src_path, l)
    img_list = os.listdir(label_path)

    count = 0

    for im in np.random.permutation(img_list):
        img = cv2.imread(os.path.join(label_path, im))
        try:
            tmp = img.shape
        except AttributeError as e:
            print('train: ' + im)
            continue
        if count < 60:
            cv2.imwrite(os.path.join(train_path, l, im),img)
        elif count < 80:
            cv2.imwrite(os.path.join(test_path, l, im),img)
        else:
            break
        count += 1

