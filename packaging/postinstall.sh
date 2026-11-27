#!/bin/sh
# The service's directories. The service is neither enabled nor started: it
# needs /etc/drun/drun.yaml, the transport's certificates and a pinned
# runnable first (the Setup Guide).
set -e
install -d -o root -g drun -m 0750 /etc/drun
install -d -o drun -g drun -m 0700 /var/lib/drun
if command -v systemctl >/dev/null && [ -d /run/systemd/system ]; then
	systemctl daemon-reload
fi
