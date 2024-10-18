# How runtime container images work? (go/byod-runtime)

runtime container images are built in a way that is different from OSS upstream,
because we do not want to give users release artifacts that can be downloaded
through the Internet (such as Python wheels). Instead, the compiled runtime code
is directly injected after the container image is loaded, but before customers'
ray application starts.

Technically, the injection is implemented through two interfaces supported
by Anyscale cluster infrastructure. One is the Ray application pre-start hook,
and the other is the Anyscale dataplane service.

This injection interface is **NOT** formally exposed to customers yet. We only
onboarded several important customers with this method. We have not yet made
the decision on if (and how) we want this to be a public Anyscale product
interface.

## Ray pre-start hook

The pre-start hook works like this: when Anyscale starts a cluster, either for
jobs, services or workspaces, before it starts the Ray application, it will
first look into the image for the `/opt/anyscale/ray-prestart` file. If such
file exists and it is executable, Anyscale will execute it with the default
user of the image (often the `ray` user).

## Anyscale dataplane service.

The Anyscale dataplane service is an HTTP service that is served at the unix
domain socket of `/tmp/anyscale/anyscaled/sockets/dataplane_service.sock`

The dataplane service provides a service endpoint called `/presigned_urls`,
which takes a JSON request and replies a JSON response. The requeust is a map
with one field called `"key"`, and the response is a map with keys
called `"url"` and `"method"`. It has some extra keys, but those are not used.
And currently the `"method"` is always `"GET"`. The `Content-Type` of both
the request and the response needs to be `application/json`. The service
endpoint returns a presigned URL that can be used to fetch a blob inside
Anyscale's common org-data bucket. This bucket is the same one that saves
common Anyscale images (i.e. the common images available through the container
registry that is served at `localhost:5555` ). Due to its credential life-cycle,
this presign service only works on the first couple of minutes after the cluster
starts. To learn more about the dataplane service or the org-data bucket,
please consult Anyscale product foundations team.

An example of how to use the unix domain socket:

````bash
$ curl --unix-socket /tmp/anyscale/anyscaled/sockets/dataplane_service.sock \
  -XPOST http://uds/presigned_urls \
  -H "Content-Type: application/json" \
  -d '{"key": "common/ray-opt/2.23.0/d2b9f42cd72aa9057ba5b36fbcf7ad088ef887bd/ray-opt-py310.tar.gz"}'

... returns a JSON blob with a signed URL, and fetching the URL

$ curl -sfL https://anyscale-production-organization-data-us-west-2.s3-accelerate.amazonaws.com/common/... | sha256sum
e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855  -

````

## Runtime injection

Anyscale's default, official Ray base images have
`/opt/anyscale/ray-prestart` and several other files built-in inside the image.
Those files setup the prestart hook process that can fetch and install the
related version of the runtime package. The required files all reside under the
`/opt/anyscale` directory, and the source files mostly reside in the same
directory of this README file. Specifically:

- `/opt/anyscale/ray-prestart` : [source](./ray-prestart) a bash script that serves as the pre-start
  hook executable.
- `/opt/anyscale/download_anyscale_data` :
  [source](../go/download_anyscale_data/main.go) a helper Go language binary
  that can communicate to the dataplane service Unix domain socket. We added
  this because our dataplane Ray images previously do not contain `curl`, and
  `wget` does not support accessing Unix domain sockets..
- `/opt/anyscale/version-envs.sh` : a generated bash script that sets default
  environment variables that specifies the Ray version and commit to load, and
  which directory to extract the runtime site-package archive into.
- `/opt/anyscale/runtime-requirements.txt` :
  [source](./runtime-requirements.txt) additional Python dependencies
  (to OSS Ray dependencies) that the image needs to install for runtime to work.
- `/opt/anyscale/ray-opt.tgz` : an optional gzipped archive only available in
  internal development versions of the images. When this file exists, the
  pre-start script directly extracts this file, rather than fetching it through
  the dataplane service. This avoids saving a lot of dev-use-only runtime
  archives in our org-data bucket.

The pre-start script supports the following environment variables that can
adjust its behavior:

- `ANYSCALE_DISABLE_OPTIMIZED_RAY` : when set to `true` or `1`, runtime
  injection will be completely disabled.
- `ANYSCALE_RAY_SITE_PKG_DIR` : filesystem path location to extract the site
  package archive into. No ending slash required. The directory needs to exist
  in the container image. In our released images, the default value is set
  to the miniconda's site-package directory of the `default` env in the image.
- `ANYSCALE_PRESTART_DATA_PATH` : the `key` value to be used for fetching
  the site-package archive, often in the form of
  `common/ray-opt/<ray version>/<ray commit>/ray-opt-<py3xx>.tar.gz` . This
  archive is uploaded to Anyscale backend during the release building proccess.
  We only guarantee that formally released Ray versions have the respective
  archives available. To support versions or commits that are not officially
  released, please consult Release and Engineering Efficiency (REEf) team for
  support first. For dev / non-release images, a site-package tarball archive
  is saved inside the image, and this environment variable is not used.
- `ANYSCALE_RAY_VERSION` : the full Ray version, e.g. `2.23.0`. This is required
  to determine which `.dist-info` directory to overwrite. Must be consistent
  with `ANYSCALE_RAY_SITE_PKG_DIR`
- `ANYSCALE_RAY_COMMIT` : the full runtime commit, currently not used for
  anything but must be consistent with the part that
  is used in `ANYSCALE_RAY_SITE_PKG_DIR`. It is set in `versions-envs.sh` so
  that customized pre-start hook can use it.

## Example to enable runtime for BYOD users

Given this, it is technically possible to enable runtime for BYOD
(Bring Your Own Docker/Container) users, by copying the files under
`/opt/anyscale` into run customer's own images:

Here is an example Dockerfile:

````Dockerfile
# syntax=docker/dockerfile:1.3-labs


# A custom image based on Ubuntu 22.04 and Python 3.10
ARG BASE_IMAGE=example.com/custom-byod

# The image to copy files that enables runtime injection.
ARG ANYSCALE_RAY_IMAGE=anyscale/ray:2.10.0-py310

FROM $ANYSCALE_RAY_IMAGE as anyscale-ray
FROM $BASE_IMAGE

# Assuming user does not have anything under /opt/anyscale.
COPY --from=anyscale-ray /opt/anyscale /opt/anyscale
RUN pip install -r /opt/anyscale/runtime-requirements.txt

# This directory needs to point to where the ray is installed on the image.
ENV ANYSCALE_RAY_SITE_PKG_DIR=/home/ray/virtualenv/lib/python3.10/site-packages

# Replace the following envs with the runtime commit that we want customer to use.
# Make sure the images are built on runtime's pipeline on a release branch.
# This can also be configured with `--build-arg ANYSCALE_RAY_COMMIT=...`
#
# Example build: https://buildkite.com/anyscale/rayturbo/builds/1972
# which has commit https://github.com/anyscale/rayturbo/commit/eb4c7109391229e9e9ca6b2dfca955c13dc45eb7
# One can also overwrite both the values of ANYSCALE_RAY_COMMIT and ANYSCALE_PRESTART_DATA_PATH
# env vars in Anyscale's custom image or cluster runtime.
ARG ANYSCALE_RAY_COMMIT=eb4c7109391229e9e9ca6b2dfca955c13dc45eb7
ENV ANYSCALE_RAY_COMMIT=$ANYSCALE_RAY_COMMIT
ENV ANYSCALE_PRESTART_DATA_PATH=common/ray-opt/2.10.0/$ANYSCALE_RAY_COMMIT/ray-opt-py310.tar.gz
````

With this Dockerfile build, a customer can dynamically set `ANYSCALE_RAY_COMMIT`
in cluster runtime to pick another released commit if they know the commit's
full digest, without rebuilding the image. This allows us to provide the
customer custom cherrypicks in Ray package that lays on top of released
versions of runtime images.

## Image build process

When building the runtime image:

- We first build the image with both OSS Ray and runtime installed with `pip`,
  and tarball the site-pacakge directories into archives. The `.pyc` files
  in the site-package directories are removed, as the generation of `.pyc` files
  is not deterministic.
- After that, we rebuild the image with runtime installed with `pip`
  (with package caching disabled), removes the runtime site-package immediately,
  and replaces it with the OSS site-package files. This makes sure that the
  runtime site-package bits are not saved in the image in any layer.
- The runtime site-package archive is then uploaded to the org-data S3 buckets
  at the related path, so that they are accessible from Anyscale through
  the pre-start hook and the Anyscale dataplane service.
- The images are uploaded to public registries such as DockerHub so that they
  can be used as base images for BYOD users.

## Why not `pip install` the runtime wheel?

Customers might have different versions of `pip` in their images, and
`pip install` might query the Internet for resolving dependencies, which
might make the runtime injection unstable. Directly extracting the Python files
into the site-package directories works perfectly fine with Python, guaratees
that the filesystem is the same end state after `pip install` the runtime wheel
during our container building, and makes sure that no Python dependency
changes.

That said, if in the future, we have a requirement to provide the runtime wheel
through the dataplane service, we are free add that feature at any time.

## Reference

- [Original design doc][design-doc]


[design-doc]: https://docs.google.com/document/d/1tQTYaxteOUenWq974kRxh61qyhYEh46zmbG0WfVH1l8
