#!/bin/sh
# The example runnables come with manifests signed by drun's release key
# (/etc/drun/runnable-keys/drun.pub, drun_specification.md §8.8): configure
# them with "key: drun" instead of a sha256.
set -e
echo "drun example runnables; configure them in /etc/drun/drun.yaml with key: drun, e.g."
echo "  - { name: ticker, kind: service, binary: /usr/libexec/drun/runnables/drun-runnable-ticker, key: drun }"
exit 0
