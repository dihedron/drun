# shellcheck shell=bash
# The lab's CA and the nodes' certificates: SPIFFE IDs as URI SANs, the
# node's address as an IP SAN, serverAuth and clientAuth (SEC-1, SEC-2).

pki_dir() { echo "$LAB_STATE_DIR/pki"; }

pki_up() {
	local d
	d="$(pki_dir)"
	mkdir -p "$d" && chmod 0700 "$d"
	[[ -f "$d/ca.crt" ]] && return
	openssl req -x509 -newkey ed25519 -nodes -days 3650 -subj "/CN=drun-lab CA" \
		-keyout "$d/ca.key" -out "$d/ca.crt" 2>/dev/null || die "pki: cannot create the CA"
	info "pki: CA created"
}

# identity NAME: the node's SPIFFE ID
identity() { echo "spiffe://drun-lab/node/$1"; }

# pki_node NAME: a certificate for a node, once
pki_node() {
	local name="$1" d
	d="$(pki_dir)"
	[[ -f "$d/$name.crt" ]] && return
	openssl req -newkey ed25519 -nodes -subj "/CN=$name" -keyout "$d/$name.key" -out "$d/$name.csr" 2>/dev/null
	printf 'subjectAltName=URI:%s,IP:%s\nextendedKeyUsage=serverAuth,clientAuth\nkeyUsage=digitalSignature\n' \
		"$(identity "$name")" "$(inner_ip "$name")" >"$d/$name.ext"
	openssl x509 -req -in "$d/$name.csr" -CA "$d/ca.crt" -CAkey "$d/ca.key" -CAcreateserial -days 3650 \
		-extfile "$d/$name.ext" -out "$d/$name.crt" 2>/dev/null || die "pki: cannot sign $name"
	rm -f "$d/$name.csr" "$d/$name.ext"
}
