#!/bin/sh
set -eu
# Keep the generated key out of process arguments and healthcheck output.
printf 'header = "Authorization: Bearer %s"\n' "$(cat /run/coral/admin-key)" |
    curl --config - --fail --silent --max-time 3 \
        http://localhost:5555/api/v1/local/namespace > /dev/null
