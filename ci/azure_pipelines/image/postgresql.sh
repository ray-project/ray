#!/bin/bash
################################################################################
##  File:  postgresql.sh
##  Desc:  Installs Postgresql
################################################################################

# Source the helpers for use with the script
source $HELPER_SCRIPTS/document.sh

echo "Install libpq-dev"
apt-get install libpq-dev

echo "Install Postgresql Client"
apt-get install postgresql-client

DocumentInstalledItem "$(psql -V 2>&1 | cut -d ' ' -f 1,2,3)"
