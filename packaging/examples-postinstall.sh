#!/bin/sh
# The example runnables are pinned by their SHA-256 in each node's
# configuration (SEC-4): print the digests to copy there.
set -e
echo "drun example runnables; pin them in /etc/drun/drun.yaml with:"
for f in /usr/libexec/drun/runnables/drun-runnable-ticker /usr/libexec/drun/runnables/drun-runnable-fenced-counter; do
	[ -f "$f" ] && echo "  $(sha256sum "$f")"
done
exit 0
