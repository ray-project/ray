(vm-logging)=
# Log Persistence

Logs are useful for troubleshooting Ray applications and Clusters. For example, you may want to access system logs if a node terminates unexpectedly.

Ray does not provide a native storage solution for log data. Users need to manage the lifecycle of the logs by themselves. The following sections provide instructions on how to collect logs from Ray Clusters running on VMs.

## Ray log directory
By default, Ray writes logs to files in the directory `/tmp/ray/session_*/logs` on each Ray node's file system, including application logs and system logs. Learn more about the {ref}`log directory and log files <logging-directory>` and the {ref}`log rotation configuration <log-rotation>` before you start to collect logs.


## Log processing tools

A number of open source log processing tools are available, such as [Vector][Vector], [FluentBit][FluentBit], [Fluentd][Fluentd], [Filebeat][Filebeat], and [Promtail][Promtail].

[Vector]: https://vector.dev/
[FluentBit]: https://docs.fluentbit.io/manual
[Filebeat]: https://www.elastic.co/guide/en/beats/filebeat/7.17/index.html
[Fluentd]: https://docs.fluentd.org/
[Promtail]: https://grafana.com/docs/loki/latest/clients/promtail/

## Log collection

After choosing a log processing tool based on your needs, you may need to perform the following steps:

1. Ingest log files on each node of your Ray Cluster as sources.
2. Parse and transform the logs. You may want to use {ref}`Ray's structured logging <structured-logging>` to simplify this step.
3. Ship the transformed logs to log storage or management systems.
