
# Installing Ray

We highly recommand you to use [Ray Cluster Lancher](./launching-clusters/aws.html) to set up a Ray cluster.
However, you can also manually install Ray via Python package installer `pip`, or using Ray docker images.

## Install Ray via pip

You can install the latest official version of Ray as follows.

```

  pip install -U ray  # minimal install

  # To install Ray with support for the dashboard + cluster launcher, run
  # `pip install -U "ray[default]"`
```

To install Ray libraries:

```
  pip install -U "ray[air]"   # installs Ray + dependencies for Ray AI Runtime
  pip install -U "ray[tune]"  # installs Ray + dependencies for Ray Tune
  pip install -U "ray[rllib]" # installs Ray + dependencies for Ray RLlib
  pip install -U "ray[serve]" # installs Ray + dependencies for Ray Serve
```


## Use the Ray docker images

You can also pull a Ray Docker image from the [Ray Docker Hub.](https://hub.docker.com/r/rayproject/)
The `rayproject/ray` [image has ray and all required dependencies. It comes with anaconda and Python 3.7.](https://hub.docker.com/r/rayproject/ray)
The `rayproject/ray-ml` [image has the above features as well as many additional libraries.](https://hub.docker.com/r/rayproject/ray-ml)
The `rayproject/base-deps` and `rayproject/ray-deps` are for the linux and python dependencies respectively.
Image releases are tagged using the following format:

|Tag	|Description	|
|---	|---	|
|latest	|The most recent Ray release.	|
|---	|---	|
|1.x.x	| A specific Ray release.	|
|nightly	| The most recent Ray build (the most recent commit on Github `master`)	|
|Git SHA	|A specific nightly build (uses a SHA from the Github `master`).	|

Some tags also have variants that add or change functionality:

|Variant	|Description	|
|---	|---	|
|-cpu	|These are based off of an Ubuntu image.	|
|---	|---	|
|-cuXX	| These are based off of an NVIDIA CUDA image with the specified CUDA version. They require the Nvidia Docker Runtime.	|
|-gpu	| Aliases to a specific `-cuXX` tagged image.	|
|<no tag>	|
Aliases to `-cpu` tagged images. For `ray-ml` image, aliases to `-gpu` tagged image.	|

If you want to tweak some aspect of these images and build them locally, refer to the following script:


```
cd ray
./build-docker.sh
```

Beyond creating the above Docker images, this script can also produce the following two images.
The `rayproject/development` image has the ray source code included and is setup for development.
The `rayproject/examples` image adds additional libraries for running examples.
Review images by listing them:


```
docker images
```

Output should look something like the following:


```
REPOSITORY                          TAG                 IMAGE ID            CREATED             SIZE
rayproject/ray                      latest              7243a11ac068        2 days ago          1.11 GB
rayproject/ray-deps                 latest              b6b39d979d73        8 days ago          996  MB
rayproject/base-deps                latest              5606591eeab9        8 days ago          512  MB
ubuntu                              focal               1e4467b07108        3 weeks ago         73.9 MB
```



### Launch Ray in Docker

Start out by launching the deployment container.


```
docker run --shm-size=<shm-size> -t -i rayproject/ray
```

Replace `<shm-size>` with a limit appropriate for your system, for example `512M` or `2G`. A good estimate for this is to use roughly 30% of your available memory (this is what Ray uses internally for its Object Store). The `-t` and `-i` options here are required to support interactive use of the container.
If you use a GPU version Docker image, remember to add `--gpus all` option. Replace `<ray-version>` with your target ray version in the following command:


```
docker run --shm-size=<shm-size> -t -i --gpus all rayproject/ray:<ray-version>-gpu
```

**Note:** Ray requires a **large** amount of shared memory because each object store keeps all of its objects in shared memory, so the amount of shared memory will limit the size of the object store.
You should now see a prompt that looks something like:


```
root@ebc78f68d100:/ray#
```


