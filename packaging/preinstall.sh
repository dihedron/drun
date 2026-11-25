#!/bin/sh
# The service's user, before the package installs files owned by its group.
set -e
getent passwd resurgo >/dev/null || useradd --system --user-group --home-dir /var/lib/resurgo --no-create-home --shell /usr/sbin/nologin resurgo
