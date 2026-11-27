# Fencing external state

The epoch protects resurgo's own state by itself. A runnable that also
writes to a database of its own must make that database refuse an older
incarnation, because a cut-off leader's runnable may still be finishing a
write when its successor starts. The pattern, for PostgreSQL:

```sql
CREATE TABLE resurgo_fence (runnable text PRIMARY KEY, epoch bigint NOT NULL);

-- at the start of the incarnation: claim the fence, or find out it is stale
INSERT INTO resurgo_fence VALUES ($1, $2)
  ON CONFLICT (runnable) DO UPDATE SET epoch = EXCLUDED.epoch
  WHERE resurgo_fence.epoch < EXCLUDED.epoch;      -- 0 rows: fenced, return

-- in every transaction that writes
SELECT epoch FROM resurgo_fence WHERE runnable = $1 FOR SHARE;  -- must equal $2
-- ... the writes ...
COMMIT;
```

The claim's update waits for the row lock that the predecessor's open
transactions hold, and once the claim commits every predecessor
transaction fails its check. The database thus orders the two
incarnations, whatever their timing.

## The example: the fenced counter

`resurgo-runnable-fenced-counter` appends `(epoch, seq)` rows to a table,
one transaction per interval, fenced as above, and checkpoints `seq`. Its
configuration names a file holding the connection string, so that the
secret never enters the node's configuration or the Raft log:

```json
{"dsn_file": "/etc/resurgo/fenced-counter/dsn", "interval": "1s"}
```

`resurgo-runnable-fenced-counter check --dsn-file F` reads the rows in
commit order and fails if a row of an epoch was committed after a row of
a newer one. Rows written after the last acknowledged checkpoint and
written again by the successor are reported as repeats: the pattern gives
at least once, not exactly once.

Side effects in systems that cannot check an epoch, such as an e-mail or
a call to another service, may happen twice during an overlap: make them
idempotent at the receiver.
