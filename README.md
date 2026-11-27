# drun

**drun** (*distributed run*; formerly *resurgo*) runs a plugin, a
**runnable**, on the leader of a small
[Raft](https://github.com/hashicorp/raft) cluster and nowhere else. When
leadership moves, the runnable is stopped on the old leader and started
again on the new one, with a **fencing epoch** strictly greater than any
before it and the last **checkpoint** an earlier incarnation committed
through Raft. Each runnable also gets its own replicated, fenced SQL
store.

Runnables are [go-plugin](https://github.com/hashicorp/go-plugin) binaries
implementing a standard interface; they can be stateless, or stateful with
their state in drun's store or in an external system (e.g. PostgreSQL)
fenced by the epoch.

**Status:** pre-1.0. The specification is in
[`.specs/drun_specification.md`](.specs/drun_specification.md); the
Setup, Operator's and Architecture guides are built from [`docs/`](docs/)
and attached to each release as PDFs.

## Build

```bash
make help      # the targets
make           # lint and build for linux/amd64
go test ./...
```

## License

MIT; see [LICENSE](LICENSE).
