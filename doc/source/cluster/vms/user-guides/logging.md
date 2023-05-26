# Log Persistence

Logs (both system and application logs) are useful for monitoring and troubleshooting your applications and the Ray system. For example, you may want to access your application's logs if a node dies.

Similar to Kubenetes, Ray does not provide a native storage solution for log data. Users need to manage the lifecycle of the logs by themselves. The following sections provide some guidan e on how to collect logs from Ray clusters running on VMs.

## The Ray log directory
By default, Ray writes logs to files in the directory `/tmp/ray/session_*/logs` on each Ray node's file system, including application logs and Ray system logs. Learn more about the [log directory and log file structure](../../../ray-observability/user-guides/configure-logging.rst) before you start to collect the logs.

Extracting and persisting these logs requires some setup.


## Log processing tools

There are a number of open source log processing tools available, e.g, [Vector][Vector] and [Fluent Bit][FluentBit].
Other popular tools include [Fluentd][Fluentd], [Filebeat][Filebeat], and [Promtail][Promtail].

[Vector]: https://vector.dev/
[FluentBit]: https://docs.fluentbit.io/manual
[Filebeat]: https://www.elastic.co/guide/en/beats/filebeat/7.17/index.html
[Fluentd]: https://docs.fluentd.org/
[Promtail]: https://grafana.com/docs/loki/latest/clients/promtail/

## Log collection

After choosing a log processing tool based on your needs, usually you need to perform the following step:

1. Ingest log files on each node of your Ray cluster as sources.
2. Parse and transform the logs based on your needs. You may want to use [Ray's structured logging](../../../ray-observability/user-guides/configure-logging.rst) to simplify this step.
3. Ship the transformed logs to log storage or management systems.