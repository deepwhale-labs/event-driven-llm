#!/bin/sh
set -eu
umask 077
case "$CORAL_AGENT_ID" in notifier|observer|writer|reviewer) ;; *) exit 1 ;; esac
case "$CORAL_SESSION_ID" in ''|*[!a-zA-Z0-9-]*) exit 1 ;; esac
endpoint="/run/coral/${CORAL_SESSION_ID}-${CORAL_AGENT_ID}.url"
printf '%s' "$CORAL_CONNECTION_URL" > "${endpoint}.tmp"
mv "${endpoint}.tmp" "$endpoint"
# Coral owns this endpoint process; the Java app performs its MCP calls.
trap 'exit 0' TERM INT
while :; do sleep 3600 & wait "$!"; done
