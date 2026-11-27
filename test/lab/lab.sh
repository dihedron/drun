#!/usr/bin/env bash
# The drun lab: five voters and a spare in three regions of one LXD VM,
# PostgreSQL for the fenced-counter, an emulated WAN (.specs/drun-lab.md).
set -euo pipefail

LAB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$LAB_DIR/../.." && pwd)"
for lib in common lxd inner pki wan nodes; do
	# shellcheck source=/dev/null
	source "$LAB_DIR/lib/$lib.sh"
done

usage() {
	cat <<USAGE
usage: lab.sh <command>
  up [--release vX.Y.Z]     build the lab (idempotent), then snapshot it
  down                      destroy everything the lab created
  status                    instances, members, WAN
  wan show|set|partition|heal ...   the WAN (one-way ms between regions)
  cut isolate NODE | oneway SRC DST | clear   node and one-way cuts
  node start|stop|restart|kill|pause|resume|wipe NAME
  profile lan|wan           rewrite raft.profile on every node (no restart)
  drun NAME ARGS...         the drun CLI on a node
  pg kill|start             PostgreSQL
  ssh NAME [cmd]            a shell or a command in the lab VM or a container
  snapshot | reset          the baseline (voters bootstrapped, spare idle)
USAGE
}

require_lab() { vm_exists lxd || die "the lab is not built: lab.sh up"; }

cmd_up() {
	while (($#)); do
		case "$1" in --release) LAB_RELEASE="$2" && shift 2 ;; *) die "up: unknown $1" ;; esac
	done
	mkdir -p "$LAB_STATE_DIR" && chmod 0700 "$LAB_STATE_DIR"
	[[ -f "$LAB_STATE_DIR/env.json" ]] || echo '{}' >"$LAB_STATE_DIR/env.json"
	pki_up
	lxd_up
	lxd_vm_up
	section "WAN"
	wan_restore
	pg_up
	nodes_up
	env_set .release "$(jq -n --arg r "$LAB_RELEASE" '$r')"
	section "Done"
	info "lab up with $LAB_RELEASE"
}

cmd_status() {
	require_lab
	section "Instances"
	inner list -c ns4 -f compact
	section "Members (from a running voter)"
	local n
	for n in $LAB_VOTERS; do
		inner_exec "$n" drun member show 2>/dev/null && break
	done
	cmd_wan show
}

cmd_snapshot() {
	section "Snapshot"
	local n
	for n in $(inner list -c n -f csv); do inner stop "$n" --timeout 60 >/dev/null 2>&1 || true; done
	lxd stop lxd --timeout 120
	lxd delete lxd/baseline >/dev/null 2>&1 || true
	lxd snapshot lxd baseline
	lxd start lxd
	vm_wait lxd
	for n in $(inner list -c n -f csv); do inner_start "$n"; done
	wan_restore
	info "baseline taken"
}

cmd_reset() {
	section "Reset"
	lxd stop lxd --force >/dev/null 2>&1 || true
	lxd restore lxd baseline
	lxd start lxd
	vm_wait lxd
	local n
	for n in $(inner list -c n -f csv); do inner_start "$n"; done
	env_set .wan '{}'
	wan_restore
	info "reset to the baseline"
}

cmd_down() {
	confirm "destroy the drun lab?" || die "not confirmed"
	lxd delete lxd --force >/dev/null 2>&1 || true
	lxd_down
	rm -rf "$LAB_STATE_DIR"
}

main() {
	load_settings
	local cmd="${1:-help}"
	(($#)) && shift
	case "$cmd" in
	up) cmd_up "$@" ;;
	down) cmd_down ;;
	status) cmd_status ;;
	wan) require_lab && cmd_wan "$@" ;;
	cut) require_lab && cmd_cut "$@" ;;
	node) require_lab && cmd_node "$@" ;;
	profile) require_lab && node_profile "${1:?lan or wan}" ;;
	drun) require_lab && cmd_drun "$@" ;;
	pg) require_lab && case "${1:-}" in kill) inner_exec pg-b systemctl kill -s SIGKILL postgresql@16-main ;; start) inner_exec pg-b systemctl start postgresql@16-main ;; *) die "pg kill|start" ;; esac ;;
	ssh)
		require_lab
		local name="${1:?ssh NAME}"
		shift
		if [[ "$name" == lxd ]]; then lxd exec lxd -- "${@:-bash}"; else inner_exec "$name" "${@:-bash}"; fi
		;;
	snapshot) require_lab && cmd_snapshot ;;
	reset) require_lab && cmd_reset ;;
	help | -h | --help) usage ;;
	*) usage && exit 1 ;;
	esac
}

main "$@"
