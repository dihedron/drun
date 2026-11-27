# Installation

## Packages

Each release has `.deb`, `.rpm` and `.apk` packages for amd64 (in three
variants: `v1` runs on any x86-64 CPU, `v2` and `v3` are built for newer
ones) and arm64:

| Package | Holds |
| :--- | :--- |
| `drun` | The node and the command line (`/usr/bin/drun`), the systemd unit, an example `/etc/drun/drun.yaml`, shell completions |
| `drun-runnable-examples` | `drun-runnable-ticker` and `drun-runnable-fenced-counter` in `/usr/libexec/drun/runnables` |

Check the download before installing it: the release's
`checksums.txt` is signed with the project's packaging key
(`checksums.txt.asc`), and the `.deb` and `.rpm` packages are signed with
the same key.

```
gpg --verify checksums.txt.asc checksums.txt
sha256sum --check --ignore-missing checksums.txt
```

Then, on each node:

```
apt install ./drun_VERSION_linux_amd64.deb ./drun-runnable-examples_VERSION_linux_amd64.deb
# or: dnf install ./drun-VERSION.x86_64.rpm ...
```

The node binary is statically linked: it needs nothing else but
`ca-certificates`.

## What the package sets up

| What | Owner and mode | Why |
| :--- | :--- | :--- |
| user and group `drun` | | the service runs as `drun` |
| group `drun-admin` | | its members may use the admin socket |
| `/etc/drun` | `root:drun 0750` | the configuration and the certificates |
| `/var/lib/drun` | `drun:drun 0700` | the node's data (`node.data_dir`) |
| `/usr/libexec/drun/runnables` | `root:root 0755` | runnable binaries: root's only |
| `/run/drun` | `drun:drun-admin 0750` | the admin socket, made by systemd at start |

The service is installed but neither enabled nor started: it needs its
configuration and certificates first. To let a user run `drun`
commands on a node, add them to `drun-admin`:

```
usermod -a -G drun-admin alice
```

## The service

`drun.service` runs `drun serve` as `drun`, confined: no
capabilities, a read-only system, no access to home directories, its own
`/tmp`, a system call filter, no core dumps. Stopping it gives a leader up
to 90 s to stop its runnables and hand over; whatever is left of the
service's processes, the runnables' included, is then killed.

Logs go to the journal: `journalctl -u drun`.

## Encrypting the data directory

`/var/lib/drun` holds the Raft log, the snapshots and each runnable's
database. All three are in clear, and they contain everything the
runnables checkpoint and store. drun does not encrypt them itself.
Encrypt the filesystem under the data directory instead, so that a
stolen disk, a decommissioned host or a disk image reveals nothing:

- **LUKS (dm-crypt):** put `/var/lib/drun` on an encrypted volume,
  unlocked at boot by a TPM (`systemd-cryptenroll --tpm2-device=auto`) or
  by a network key server (Clevis and Tang), so that a node can restart
  unattended.
- **ZFS:** a dataset with `encryption=aes-256-gcm` and a key loaded at
  boot, mounted on `/var/lib/drun`.
- **fscrypt:** on ext4 or f2fs, an encrypted `/var/lib/drun`, created
  empty before the first start, with a protector unlocked at boot.

Prepare the volume before the first start: drun creates the data
directory's content and does not move it. Store backups
(`drun store backup`) are in clear too: keep them on encrypted storage,
or encrypt them (for example with `age`) before they leave the node.

Encryption at rest does not protect a running node: root, or the service
user, reads the data as drun does.
