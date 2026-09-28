#!/bin/sh
set -eu
umask 077
if [ ! -s /run/coral/admin-key ]; then
    od -An -N32 -tx1 /dev/urandom | tr -d ' \n' > /run/coral/admin-key
fi
printf '[auth]\nkeys = ["%s"]\n' "$(cat /run/coral/admin-key)" > "$CONFIG_FILE_PATH"
exec java -Xms64m -Xmx384m -Duser.home=/home/coral -Dfile.encoding=UTF-8 \
    -jar /opt/coral/server.jar \
    --network.bind_address=0.0.0.0 --network.bind_port=5555 \
    --registry.include_coral_home_agents=false \
    --registry.local_agents=/opt/coral/endpoint \
    --logging.log_to_file_enabled=false --logging.console_log_level=WARN
