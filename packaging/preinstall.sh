#!/bin/sh
# The service's user, before the package installs files owned by its group,
# and the group whose members may use the admin socket (SEC-6). Alpine has
# BusyBox's addgroup and adduser instead of groupadd and useradd.
set -e
if ! getent group resurgo-admin >/dev/null; then
	if command -v groupadd >/dev/null; then groupadd --system resurgo-admin; else addgroup -S resurgo-admin; fi
fi
if ! getent passwd resurgo >/dev/null; then
	if command -v useradd >/dev/null; then
		useradd --system --user-group --home-dir /var/lib/resurgo --no-create-home --shell /usr/sbin/nologin resurgo
	else
		addgroup -S resurgo
		adduser -S -D -H -h /var/lib/resurgo -s /sbin/nologin -G resurgo resurgo
	fi
fi
