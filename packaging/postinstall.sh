#!/bin/sh
# The service's directories. The service is neither enabled nor started: it
# needs /etc/resurgo/resurgo.yaml, the transport's certificates and a pinned
# runnable first (the Setup Guide).
set -e
install -d -o root -g resurgo -m 0750 /etc/resurgo
install -d -o resurgo -g resurgo -m 0700 /var/lib/resurgo
if command -v systemctl >/dev/null && [ -d /run/systemd/system ]; then
	systemctl daemon-reload
fi
