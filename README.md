# resurgo

*Resurgo* (Latin, "I rise again") runs a plugin, a **runnable**, on the
leader of a small [Raft](https://github.com/hashicorp/raft) cluster and
nowhere else. When leadership moves, the runnable is stopped on the old
leader and started again on the new one, with a **fencing epoch** strictly
greater than any before it and the last **checkpoint** an earlier
incarnation committed through Raft.

Runnables are [go-plugin](https://github.com/hashicorp/go-plugin) binaries
implementing a standard interface; they can be stateless, or stateful with
their state in an external system (e.g. PostgreSQL) fenced by the epoch.

**Status:** design. The specification is in
[`.specs/resurgo_specification.md`](.specs/resurgo_specification.md); the
implementation starts once it is approved (see [`AGENTS.md`](AGENTS.md)).

## Build

```bash
make help      # the targets
make           # lint and build for linux/amd64
go test ./...
```

## License

GNU Affero General Public License v3.0; see [LICENSE](LICENSE).
