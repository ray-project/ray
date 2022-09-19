import ray

path = "s3://air-example-data-2/movie-image-small-filesize-1GB"
ds = ray.data.read_images(path, size=(224, 224))
