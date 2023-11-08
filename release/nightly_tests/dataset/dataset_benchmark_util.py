import math


def _read_sysnet_mapping():
    """Read a mapping of WNID to its class label. Source file:
    https://github.com/formigone/tf-imagenet/blob/master/LOC_synset_mapping.txt"""
    wnid_to_label = {}
    with open("LOC_synset_mapping.txt", "r") as f:
        for line in f:
            wnid, label = line.strip().split(" ", 1)
            wnid_to_label[wnid] = label
    return wnid_to_label


IMAGENET_WNID_TO_LABEL = _read_sysnet_mapping()
SORTED_WNIDS = sorted(IMAGENET_WNID_TO_LABEL.keys())
IMAGENET_WNID_TO_ID = {wnid: SORTED_WNIDS.index(wnid) for wnid in SORTED_WNIDS}


def get_prop_raw_image_paths(num_workers, target_worker_gb):
    """Get a subset of imagenet raw image paths such that the dataset can be divided
    evenly across workers, with each receiving target_worker_gb GB of data.
    The resulting dataset size is roughly num_workers * target_worker_gb GB."""
    img_s3_root = "s3://anyscale-imagenet/ILSVRC/Data/CLS-LOC/train"
    if target_worker_gb == -1:
        # Return the entire dataset.
        return img_s3_root

    mb_per_file = 143  # averaged across 300 classes

    TARGET_NUM_DIRS = min(
        math.ceil(target_worker_gb * num_workers * 1024 / mb_per_file),
        len(IMAGENET_WNID_TO_LABEL),
    )
    sorted_class_ids = sorted(IMAGENET_WNID_TO_LABEL.keys())
    file_paths = [
        f"{img_s3_root}/{class_id}/" for class_id in sorted_class_ids[:TARGET_NUM_DIRS]
    ]
    return file_paths


def get_prop_parquet_paths(num_workers, target_worker_gb):
    parquet_s3_dir = "s3://anyscale-imagenet/parquet"
    parquet_s3_root = f"{parquet_s3_dir}/d76458f84f2544bdaac158d1b6b842da"
    if target_worker_gb == -1:
        # Return the entire dataset.
        return parquet_s3_dir

    mb_per_file = 128
    num_files = 200
    TARGET_NUM_FILES = min(
        math.ceil(target_worker_gb * num_workers * 1024 / mb_per_file), num_files
    )
    file_paths = []
    for fi in range(num_files):
        for i in range(5):
            if not (fi in [163, 164, 174, 181, 183, 190] and i == 4):
                # for some files, they only have 4 shards instead of 5.
                file_paths.append(f"{parquet_s3_root}_{fi:06}_{i:06}.parquet")
            if len(file_paths) >= TARGET_NUM_FILES:
                break
        if len(file_paths) >= TARGET_NUM_FILES:
            break
    return file_paths


def get_mosaic_epoch_size(num_workers, target_worker_gb=10):
    if target_worker_gb == -1:
        return None
    AVG_MOSAIC_IMAGE_SIZE_BYTES = 500 * 1024  # 500KiB.
    epoch_size = math.ceil(
        target_worker_gb
        * num_workers
        * 1024
        * 1024
        * 1024
        / AVG_MOSAIC_IMAGE_SIZE_BYTES
    )
    return epoch_size
