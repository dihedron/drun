#!/bin/sh
# The example runnables are pinned by their SHA-256 in each node's
# configuration (SEC-4): print the digests to copy there.
set -e
echo "resurgo example runnables; pin them in /etc/resurgo/resurgo.yaml with:"
for f in /usr/libexec/resurgo/runnables/resurgo-runnable-ticker /usr/libexec/resurgo/runnables/resurgo-runnable-fenced-counter; do
	[ -f "$f" ] && echo "  $(sha256sum "$f")"
done
exit 0
