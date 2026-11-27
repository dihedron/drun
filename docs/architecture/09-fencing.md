# Fencing external state

The epoch protects drun's own state by itself. A runnable that also
writes to a database of its own must make that database refuse an older
incarnation, because a cut-off leader's runnable may still be finishing a
write when its successor starts. The pattern, for PostgreSQL:

```sql
CREATE TABLE drun_fence (cluster text NOT NULL, runnable text NOT NULL,
  epoch bigint NOT NULL, PRIMARY KEY (cluster, runnable));

-- at the start of the incarnation: claim the fence, or find out it is stale
INSERT INTO drun_fence VALUES ($1, $2, $3)
  ON CONFLICT (cluster, runnable) DO UPDATE SET epoch = EXCLUDED.epoch
  WHERE drun_fence.epoch <= EXCLUDED.epoch;     -- 0 rows: fenced, return

-- in every transaction that writes
SELECT epoch FROM drun_fence WHERE cluster = $1 AND runnable = $2
  FOR SHARE;                                    -- must equal $3
-- ... the writes ...
COMMIT;
```

The claim's update waits for the row lock that the predecessor's open
transactions hold, and once the claim commits every predecessor
transaction fails its check. The database thus orders the two
incarnations, whatever their timing. The row is keyed by cluster and
runnable, so two clusters can share a database. The claim also succeeds
at the epoch already held, so an incarnation can claim again after it
reconnects.

## The helper

`pkg/runnable/fence/postgres` implements the pattern on `pgx/v5`:

```go
f, err := postgres.Claim(ctx, db, inc) // at the start of Run: creates the table if needed
if err != nil {
	return err // runnable.ErrFenced: a newer incarnation holds the fence
}
// every writing transaction
err = f.Tx(ctx, db, func(tx pgx.Tx) error {
	_, err := tx.Exec(ctx, "INSERT ...")
	return err
})
```

`db` is a `*pgx.Conn`, a `*pgxpool.Pool` or a `pgx.Tx`. A runnable that
manages its own transactions, for example at a stricter isolation level,
calls `f.Check(ctx, tx)` first in each one. All three return
`runnable.ErrFenced` as it is, and `Run` must then return it. Writes made
outside `Tx` or `Check` are not fenced.

If the database is restored from a backup, the fence can go back to an
older epoch. Restart the runnable afterwards (`drun runnable restart`),
so that a new epoch claims the fence (risk R-8).

## The example: the fenced counter

`drun-runnable-fenced-counter` appends `(epoch, seq)` rows to a table,
one transaction per interval, fenced through the helper, and checkpoints
`seq`. Its
configuration names a file holding the connection string, so that the
secret never enters the node's configuration or the Raft log:

```json
{"dsn_file": "/etc/drun/fenced-counter/dsn", "interval": "1s"}
```

`drun-runnable-fenced-counter check --dsn-file F` reads the rows in
commit order and fails if a row of an epoch was committed after a row of
a newer one. Rows written after the last acknowledged checkpoint and
written again by the successor are reported as repeats: the pattern gives
at least once, not exactly once.

Side effects in systems that cannot check an epoch, such as an e-mail or
a call to another service, may happen twice during an overlap: make them
idempotent at the receiver.
