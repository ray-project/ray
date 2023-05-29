# Log Persistence

Logs (both system and application logs) are useful for troubleshooting Ray applications and system. For example, you may want to access system logs if a node dies unexpectedly.

Similar to Kubenetes, Ray does not provide a native storage solution for log data. Users need to manage the lifecycle of the logs by themselves. The following sections provide some instructions on how to collect logs from Ray clusters running on VMs.

## Ray log directory
By default, Ray writes logs to files in the directory `/tmp/ray/session_*/logs` on each Ray node's file system, including application logs and system logs. Learn more about the {ref}`log directory and log files <logging-directory>` before you start to collect the logs.


## Log processing tools

There are a number of open source log processing tools available, e.g, [Vector][Vector] and [Fluent Bit][FluentBit].
Other popular tools include [Fluentd][Fluentd], [Filebeat][Filebeat], and [Promtail][Promtail].

[Vector]: https://vector.dev/
[FluentBit]: https://docs.fluentbit.io/manual
[Filebeat]: https://www.elastic.co/guide/en/beats/filebeat/7.17/index.html
[Fluentd]: https://docs.fluentd.org/
[Promtail]: https://grafana.com/docs/loki/latest/clients/promtail/

## Log collection

After choosing a log processing tool based on your needs, you may need to perform the following steps:

1. Ingest log files on each node of your Ray cluster as sources.
2. Parse and transform the logs. You may want to use {ref}`Ray's structured logging <structured-logging>` to simplify this step.
3. Ship the transformed logs to log storage or management systems.


## Redirecting Ray logs to stderr
By default, Ray logs are written to files under the ``/tmp/ray/session_*/logs`` directory. It may not be ideal if the log processing tool needs log to be written to stderr in order for them to be captured. View {ref}`configuring logging <redirect-to-stderr>` for details on how to redirect all the logs to stderr of the host nodes instead.