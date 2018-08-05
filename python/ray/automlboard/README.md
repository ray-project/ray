## About AutoMLBoard

AutoMLBoard can be used as a monitor who collect information about ray tune jobs and
show the trial information to the frontend.

Usage:

```
usage: run.py [-h] [--logdir LOGDIR] [--port PORT] [--db_address DB_ADDRESS]
              [--sleep_interval SLEEP_INTERVAL] [--log_level LOG_LEVEL]

optional arguments:
  -h, --help                        show this help message and exit
  --logdir LOGDIR                   directory of logs about the jobs' status
  --port PORT                       port of the service, 8008 as default
  --db_address DB_ADDRESS           addaress of the database, use a local sqlite3 if not set
  --sleep_interval SLEEP_INTERVAL   time period of polling, 30 seconds as default     
  --log_level LOG_LEVEL             level of the log, "info" as default
```

As default, sqlite will be used as the database backend engine.
To use mysql as the backend engine, run command like this:

```bash
python run.py --db_address mysql://<hostname>:<port>/<db_name>?user=<user>&password=<password> ...
```

