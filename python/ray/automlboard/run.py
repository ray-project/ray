from django.core.management import execute_from_command_line
from common.exception import DatabaseError

import argparse
import django
import os
import re


def run_board(args):
    init_config(args)

    # backend service, should import after django settings initialized
    from backend.collector import CollectorService

    service = CollectorService(args.logdir, args.sleep_interval,
                               standalone=False, log_level=args.log_level)
    service.run()

    # frontend service
    print("try to start automlboard on port %s" % args.port)
    command = ['manage.py', 'runserver', '0.0.0.0:%s' % args.port, '--noreload']
    execute_from_command_line(command)


def init_config(args):
    """
    initialize the following settings:
    1. automl board settings
    2. database settings
    3. django settings
    """
    os.environ["AUTOMLBOARD_LOGDIR"] = args.logdir
    os.environ["AUTOMLBOARD_LOGLEVEL"] = args.log_level
    os.environ["AUTOMLBOARD_SLEEP_INTERVAL"] = str(args.sleep_interval)

    if args.db_address:
        try:
            db_address_reg = re.compile(r"(.*)://(.*):(.*)/(.*)\?user=(.*)&password=(.*)")
            match = re.match(db_address_reg, args.db_address)
            os.environ["AUTOMLBOARD_DB_ENGINE"] = match.group(1)
            os.environ["AUTOMLBOARD_DB_HOST"] = match.group(2)
            os.environ["AUTOMLBOARD_DB_PORT"] = match.group(3)
            os.environ["AUTOMLBOARD_DB_NAME"] = match.group(4)
            os.environ["AUTOMLBOARD_DB_USER"] = match.group(5)
            os.environ["AUTOMLBOARD_DB_PASSWORD"] = match.group(6)
            print("Using %s as the database backend." % os.environ["AUTOMLBOARD_DB_ENGINE"])
        except StandardError, e:
            raise DatabaseError(e)
    else:
        print("Using sqlite3 as the database backend, information will be stored in automlboard.db")

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "frontend.settings")
    django.setup()
    command = ['manage.py', 'migrate', '--run-syncdb']
    execute_from_command_line(command)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--logdir", type=str, help="directory of logs about the trials' status")
    parser.add_argument("--port", type=str, help="port of the service", default="8008")
    parser.add_argument("--db_address", type=str,
                        help="addaress of the database, use a local sqlite3 if not set", default=None)
    parser.add_argument("--reload_interval", type=int, help="time period of polling", default=5)
    parser.add_argument("--log_level", type=str, help="level of the log", default="INFO")
    cmd_args = parser.parse_args()
    run_board(cmd_args)
