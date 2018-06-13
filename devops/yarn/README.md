# Prerequisites

1. Yarn app of Ray is run under the Hadoop environment version 2.8.0. You can get the Hadoop binary from [here](http://archive.apache.org/dist/hadoop/common/hadoop-2.8.0/hadoop-2.8.0.tar.gz).

# Workthrough

#### Yarn environment and configuration

Firstly, you should have an available hadoop yarn environment and configuration.

#### Prepare ray on yarn jar file

```shell
$ make clean package
```

#### Prepare deploy zip file

```shell
$ cd deploy
$ zip -r deploy.zip .
```
Please modify the script 'deploy/run.sh' on-demand.

#### Run

```shell
$ /path/to/hadoop-2.8.0/bin/yarn jar ./target/ray-on-yarn-1.0.jar  org.ray.on.yarn.Client  --jar ./target/ray-on-yarn-1.0.jar --rayArchive ./deploy/deploy.zip --containerVcores 2 --containerMemory 2048 --priority 10 --shellCmdPriority 10 --numRoles 1 1 --queue ray --headNodeStaticArgs "'--num-cpus 4 --num-gpus 4'" --workNodeStaticArgs "'--num-cpus 2 --num-gpus 2'"
```

Please modify the command line on-demand. Some detail about the input args is in the help doc.

```shell
/path/to/hadoop-2.8.0/bin/yarn jar ./target/ray-on-yarn-1.0.jar  org.ray.on.yarn.Client --help
```

#### Monitoring

Please check the logs depend on your yarn platform.

#### Stop

```shell
$ /path/to/hadoop-2.8.0/bin/yarn application -kill {app_id}
```

`{app_id}` shall be replaced by the ID of the corresponding Yarn application, e.g. `application_1505745052163_0107`.
