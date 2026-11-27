# shellcheck shell=bash
# The drun nodes: containers in the regions, the release's packages, their
# certificates and configuration, and PostgreSQL for the fenced-counter
# (.specs/drun-lab.md §2).

all_nodes() { echo "$LAB_VOTERS $LAB_SPARE"; }

# release_fetch: the release's packages for amd64, checked against its
# checksums, cached in LAB_CACHE_DIR/RELEASE
release_fetch() {
	local dir="$LAB_CACHE_DIR/$LAB_RELEASE" v="${LAB_RELEASE#v}"
	mkdir -p "$dir"
	if [[ ! -f "$dir/ok" ]]; then
		(cd "$dir" && gh release download "$LAB_RELEASE" --repo "$LAB_REPO" --clobber \
			-p "drun_${v}_linux_amd64.deb" -p "drun-runnable-examples_${v}_linux_amd64.deb" -p "drun_${v}_checksums.txt") ||
			die "cannot download $LAB_RELEASE"
		(cd "$dir" && sha256sum --check --ignore-missing "drun_${v}_checksums.txt" >/dev/null) || die "$LAB_RELEASE: checksums do not match"
		touch "$dir/ok"
	fi
	echo "$dir"
}

# node_config NAME PROFILE: the node's drun.yaml
node_config() {
	local name="$1" profile="$2" v n
	cat <<YAML
node: { id: $name, data_dir: /var/lib/drun }
cluster:
  name: drun-lab
  bind: 0.0.0.0:7350
  advertise: $(inner_ip "$name"):7350
  voters:
YAML
	for v in $LAB_VOTERS; do echo "    - { id: $v, address: $(inner_ip "$v"):7350 }"; done
	cat <<YAML
  tls:
    cert: /etc/drun/tls/node.crt
    key: /etc/drun/tls/node.key
    ca: /etc/drun/tls/ca.crt
    peers:
YAML
	for n in $(all_nodes); do echo "      $n: \"$(identity "$n")\""; done
	cat <<YAML
raft: { profile: $profile, snapshot_threshold: 2000, trailing_logs: 1000 }
admin:
  socket: /run/drun/admin.sock
  allowed_groups: [drun-admin]
metrics:
  listen: $(inner_ip "$name"):9350
runnables:
  - name: ticker
    kind: service
    binary: /usr/libexec/drun/runnables/drun-runnable-ticker
    key: drun
    config: { interval: 200ms }
  - name: fenced-counter
    kind: service
    binary: /usr/libexec/drun/runnables/drun-runnable-fenced-counter
    key: drun
    config: { dsn_file: /etc/drun/fenced-counter/dsn, interval: 200ms }
YAML
}

pg_dsn() { echo "postgres://drun:$(password pg-drun)@$(inner_ip pg-b):5432/drun?sslmode=disable&connect_timeout=5"; }

pg_up() {
	section "PostgreSQL (pg-b)"
	inner_create pg-b "$LAB_PG_MEMORY"
	inner_start pg-b
	inner_wait pg-b
	if ! inner_exec pg-b test -f /etc/drun-lab-pg; then
		inner_exec pg-b bash -euc "
			export DEBIAN_FRONTEND=noninteractive
			apt-get update -q >/dev/null && apt-get install -yq postgresql >/dev/null
			conf=\$(ls -d /etc/postgresql/*/main)
			echo \"listen_addresses = '*'\" >>\$conf/postgresql.conf
			echo 'host drun drun $LAB_REGION_PREFIX.0.0/16 scram-sha-256' >>\$conf/pg_hba.conf
			systemctl restart postgresql
			su postgres -c \"psql -qc \\\"create role drun login password '$(password pg-drun)'\\\"\"
			su postgres -c 'createdb -O drun drun'
			touch /etc/drun-lab-pg"
		info "pg-b: PostgreSQL installed, database drun"
	else
		info "pg-b: PostgreSQL installed"
	fi
}

# node_up NAME DIR: a node with the packages of DIR, its certificate and its
# configuration; the service is enabled only for the voters
node_up() {
	local name="$1" dir="$2" v="${LAB_RELEASE#v}" d
	d="$(pki_dir)"
	inner_create "$name" "$LAB_NODE_MEMORY"
	inner_start "$name"
	inner_wait "$name"
	pki_node "$name"
	if ! inner_exec "$name" test -x /usr/bin/drun; then
		inner_push "$dir/drun_${v}_linux_amd64.deb" "$name" /root/drun.deb 0644
		inner_push "$dir/drun-runnable-examples_${v}_linux_amd64.deb" "$name" /root/drun-examples.deb 0644
		inner_exec "$name" bash -euc "export DEBIAN_FRONTEND=noninteractive; apt-get install -yq /root/drun.deb /root/drun-examples.deb >/dev/null"
	fi
	inner_exec "$name" install -d -o root -g drun -m 0750 /etc/drun/tls /etc/drun/fenced-counter
	inner_push "$d/ca.crt" "$name" /etc/drun/tls/ca.crt 0644
	inner_push "$d/$name.crt" "$name" /etc/drun/tls/node.crt 0644
	inner_push "$d/$name.key" "$name" /etc/drun/tls/node.key 0640
	inner_exec "$name" chgrp drun /etc/drun/tls/node.key
	pg_dsn | inner_write "$name" /etc/drun/fenced-counter/dsn 0640
	inner_exec "$name" chgrp drun /etc/drun/fenced-counter/dsn
	node_config "$name" "${3:-lan}" | inner_write "$name" /etc/drun/drun.yaml 0640
	inner_exec "$name" chgrp drun /etc/drun/drun.yaml
	inner_exec "$name" usermod -aG drun-admin root
	info "$name: drun $(inner_exec "$name" drun version 2>/dev/null | head -1) configured"
}

nodes_up() {
	local dir n
	dir="$(release_fetch)"
	section "drun nodes ($LAB_RELEASE)"
	for n in $(all_nodes); do node_up "$n" "$dir"; done
	for n in $LAB_VOTERS; do inner_exec "$n" systemctl enable --now drun >/dev/null 2>&1; done
	info "voters started: $LAB_VOTERS"
}

# node_profile PROFILE: rewrites every node's raft.profile (no restart)
node_profile() {
	local n
	for n in $(all_nodes); do
		node_config "$n" "$1" | inner_write "$n" /etc/drun/drun.yaml 0640
		inner_exec "$n" chgrp drun /etc/drun/drun.yaml
	done
}

cmd_node() {
	local verb="${1:?node start|stop|restart|kill|pause|resume|wipe|profile NAME}" name="${2:?a node}"
	case "$verb" in
	start) inner_exec "$name" bash -c 'systemctl reset-failed drun 2>/dev/null; systemctl start drun' ;;
	stop) inner_exec "$name" systemctl stop drun ;;
	restart) inner_exec "$name" systemctl restart drun ;;
	# the whole unit's cgroup, the runnables' processes included (systemctl
	# kill fails on the auxiliary processes inside a container)
	kill) inner_exec "$name" bash -c 'c=/sys/fs/cgroup/system.slice/drun.service; [ -e $c/cgroup.kill ] && echo 1 >$c/cgroup.kill || true' ;;
	pause) inner_exec "$name" bash -c 'c=/sys/fs/cgroup/system.slice/drun.service; [ -e $c/cgroup.freeze ] && echo 1 >$c/cgroup.freeze || true' ;;
	resume) inner_exec "$name" bash -c 'c=/sys/fs/cgroup/system.slice/drun.service; [ -e $c/cgroup.freeze ] && echo 0 >$c/cgroup.freeze || true' ;;
	wipe) inner_exec "$name" bash -c 'systemctl stop drun; find /var/lib/drun -mindepth 1 -delete' ;;
	*) die "node: start, stop, restart, kill, pause, resume or wipe" ;;
	esac
}

# cmd_drun NAME ARGS: the drun CLI on a node
cmd_drun() {
	local name="${1:?a node}"
	shift
	inner_exec "$name" drun "$@"
}
