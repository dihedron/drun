#!/usr/bin/env bash
# Checks the packages of a snapshot or release build in dist/ (make
# package-check, after make snapshot): every binary statically linked
# (resurgo-store.md §4.1), and the .deb packages holding the expected
# paths, owners and modes (SEC-4, SEC-6, SEC-9).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

fail=0
bad() { echo "FAIL: $*" >&2; fail=1; }

shopt -s nullglob
bins=(dist/resurgo_linux_*/resurgo dist/ticker_linux_*/resurgo-runnable-ticker dist/fenced-counter_linux_*/resurgo-runnable-fenced-counter)
[[ ${#bins[@]} -ge 6 ]] || bad "expected the node and both examples for amd64 and arm64 in dist/, found ${#bins[@]} binaries"
for b in "${bins[@]}"; do
	file -b "$b" | grep -q 'statically linked' || bad "$b is not statically linked: $(file -b "$b")"
done

# expect PACKAGE "MODE OWNER PATH"...: each entry is in the package
expect() {
	local deb="$1"
	shift
	local listing
	listing="$(dpkg-deb -c "$deb" | awk '{ sub(/^\.\//, "/", $6); print $1, $2, $6 }')"
	for want in "$@"; do
		grep -qxF -- "$want" <<<"$listing" || bad "$deb lacks \"$want\""
	done
}
for deb in dist/resurgo_*_linux_amd64.deb dist/resurgo_*_linux_arm64.deb; do
	expect "$deb" \
		"-rwxr-xr-x root/root /usr/bin/resurgo" \
		"-rw-r--r-- root/root /usr/lib/systemd/system/resurgo.service" \
		"-rw-r----- root/resurgo /etc/resurgo/resurgo.yaml" \
		"drwxr-xr-x root/root /usr/libexec/resurgo/runnables/"
	dpkg-deb -I "$deb" preinst | grep -q 'resurgo-admin' || bad "$deb: the preinstall script does not create resurgo-admin"
done
for deb in dist/resurgo-runnable-examples_*.deb; do
	expect "$deb" \
		"-rwxr-xr-x root/root /usr/libexec/resurgo/runnables/resurgo-runnable-ticker" \
		"-rwxr-xr-x root/root /usr/libexec/resurgo/runnables/resurgo-runnable-fenced-counter"
done
[[ $fail -eq 0 ]] && echo "packages: ok"
exit $fail
