import boto3
import os
import rasterio

from prefect import flow, task, get_run_logger
from prefect_ray import RayTaskRunner
from prefect_ray.context import remote_options

from rasterio.session import AWSSession
from rio_cogeo import cog_profiles
from rio_cogeo.cogeo import cog_translate


# ======================================================================================================================
# ============================== Cloud-Optimized-GeoTiff
# ======================================================================================================================
def translate_to_cog(src_path, dst_s3_bucket, dst_s3_key, cog_config_profile, cog_config_predictor, profile_options={},
                     **options):
    logger = get_run_logger()

    # https://gdal.org/drivers/raster/gtiff.html#creation-options
    output_profile = cog_profiles.get(cog_config_profile)
    output_profile.update(dict(
        BIGTIFF="IF_SAFER",
        PREDICTOR=cog_config_predictor,
    ))
    output_profile.update(profile_options)

    config = dict(
        GDAL_NUM_THREADS="ALL_CPUS",
        GDAL_TIFF_INTERNAL_MASK=True,
    )

    with rasterio.io.MemoryFile() as mem_dst:
        cog_translate(
            src_path,
            mem_dst.name,
            output_profile,
            config=config,
            in_memory=False,  # should also work inMemory but crashes my laptop & ray cluster for some reason
            quiet=False,
            **options,
        )

        client = boto3.client("s3")
        client.upload_fileobj(mem_dst, dst_s3_bucket, dst_s3_key)

    # Just to validate whether everything worked
    with rasterio.open(f"s3://{dst_s3_bucket}/{dst_s3_key}") as src:
        logger.info(src.profile)


# ======================================================================================================================
# ============================== Prefect
# ======================================================================================================================
@task
def tiff_to_cog(src_path, dst_s3_bucket, dst_s3_key, cog_config_profile, cog_config_predictor):
    logger = get_run_logger()

    aws_session = AWSSession(requester_pays=True)

    with rasterio.Env(aws_session):
        with rasterio.open(src_path) as src:
            logger.info(src.profile)
            translate_to_cog(src, dst_s3_bucket, dst_s3_key, cog_config_profile, cog_config_predictor)

    return True


@task
def get_src_files(src_bucket: str, src_prefix: str):
    logger = get_run_logger()

    logger.info(f"{src_bucket=}; {src_prefix=}")
    src_paths = []

    client = boto3.client('s3')
    resp = client.list_objects_v2(
        Bucket=src_bucket,
        Prefix=src_prefix,
        RequestPayer='requester',
    )

    input_images = filter(lambda obj: obj['Key'].endswith(('.jp2', '.tiff', '.tif')), resp['Contents'])

    for image in input_images:
        src_paths.append(dict(
            bucket=src_bucket,
            key=image['Key'],
            size=image['Size']
        ))

    return src_paths


@task
def get_sts_identity():
    logger = get_run_logger()

    sts_identity = boto3.client('sts').get_caller_identity()
    return sts_identity


@flow(name="cog-translate", task_runner=RayTaskRunner)
# @flow(name="cog-translate")
def main(
        src_bucket: str,
        dst_bucket: str,
        src_prefix: str = "",
        dry_run: bool = True,
        cog_config_profile: str = "DEFLATE",
        cog_config_predictor: str = "2",
):
    logger = get_run_logger()

    sts_identity = get_sts_identity.submit().result()
    logger.info(f"{sts_identity['UserId']=} on {sts_identity['Account']=}")

    src_objs = get_src_files.submit(src_bucket, src_prefix).result()

    src_size_gb = round((sum(image['size'] for image in src_objs)) / 1024 / 1024 / 1024, 4)
    src_obj_num = len(src_objs)
    logger.info(
        f"The supplied flow parameters are queuing {src_obj_num} input images, with a total size of {src_size_gb}GB "
        f"for conversion to COG"
    )

    if dry_run:
        logger.info("DRYRUN. Exiting Flow.")
        return True

    for src_obj in src_objs:
        src_path = f"s3://{src_obj['bucket']}/{src_obj['key']}"
        logger.info(f"{src_path=}")
        with remote_options(max_calls=1, memory=9*1024*1024*1024):
            tiff_to_cog.submit(
                src_path=src_path,
                dst_s3_bucket=dst_bucket,
                dst_s3_key=f"{src_obj['key']}.cog.tif",
                cog_config_profile=cog_config_profile,
                cog_config_predictor=cog_config_predictor
            )

    return True


# ======================================================================================================================
# ============================== Local Exec Config
# ======================================================================================================================
if __name__ == "__main__":
    main(src_bucket="spacenet-dataset", dst_bucket="pcm-test-bucket-2", dry_run=False)