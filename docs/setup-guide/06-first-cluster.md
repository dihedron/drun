# A first cluster

This chapter starts three nodes, `node-a`, `node-b` and `node-c`, at
`10.0.1.11` to `10.0.1.13`, and runs the example ticker: a runnable that
counts, saving its counter every second.

## On every node

1. Install both packages (*Installation*).
2. Put the node's certificate, key and the CA in `/etc/drun/tls/`,
   readable by the group `drun` (*Certificates and identities*):

   ```
   install -d -o root -g drun -m 0750 /etc/drun/tls
   install -o root -g drun -m 0640 node.crt node.key ca.crt /etc/drun/tls/
   ```

3. Note the ticker's digest, printed when the examples package was
   installed and shown again by:

   ```
   sha256sum /usr/libexec/drun/runnables/drun-runnable-ticker
   ```

4. Write `/etc/drun/drun.yaml`; only `node.id` and
   `cluster.advertise` differ between the three nodes:

   ```yaml
   node:
     id: node-a
     data_dir: /var/lib/drun
   cluster:
     name: first
     bind: 0.0.0.0:7350
     advertise: 10.0.1.11:7350
     voters:
       - { id: node-a, address: 10.0.1.11:7350 }
       - { id: node-b, address: 10.0.1.12:7350 }
       - { id: node-c, address: 10.0.1.13:7350 }
     tls:
       cert: /etc/drun/tls/node.crt
       key: /etc/drun/tls/node.key
       ca: /etc/drun/tls/ca.crt
       peers:
         node-a: "spiffe://example.org/drun/node-a"
         node-b: "spiffe://example.org/drun/node-b"
         node-c: "spiffe://example.org/drun/node-c"
   admin:
     allowed_groups: [drun-admin]
   metrics:
     listen: 127.0.0.1:9350
   runnables:
     - name: ticker
       binary: /usr/libexec/drun/runnables/drun-runnable-ticker
       sha256: THE-DIGEST-FROM-STEP-3
       config: { interval: 1s }
   ```

5. Open TCP port 7350 between the nodes, and start the service:

   ```
   systemctl enable --now drun
   ```

The first node to start waits for a second one; with two of the three
up, they elect a leader and the leader starts the ticker.

## Check it

```
$ drun cluster show
cluster first, seen from node-a (Follower)
term 2, commit index 14, applied index 14, last contact 12ms

ID      ADDRESS         SUFFRAGE  LEADER  IDENTITY
node-a  10.0.1.11:7350  Voter     no      spiffe://example.org/drun/node-a
node-b  10.0.1.12:7350  Voter     yes     spiffe://example.org/drun/node-b
node-c  10.0.1.13:7350  Voter     no      spiffe://example.org/drun/node-c

$ drun runnable show
NAME    STATE    EPOCH  NODE    HEALTHY  CHECKPOINT        FAILURES  NOTES
ticker  running  1      node-b  yes      #12, 8 B, 0s ago  0
```

## Fail it over

Stop the leader (`systemctl stop drun` on `node-b`). It stops the
ticker, records that it stopped, and hands leadership to another node,
which starts the ticker at once, with epoch 2, from the last checkpoint:

```
$ drun runnable show ticker
name:         ticker
state:        running
healthy:      yes, n=13 (leader node-a, since 2026-11-27T10:02:11Z)
incarnation:  epoch 2, term 3, on node-a, started 2026-11-27T10:02:11Z, released no
...
```

Kill the new leader instead (`kill -9` of `drun serve`, or power it
off) and the remaining node takes over after the takeover delay, about
5 s later. Start the stopped nodes again; they rejoin as followers.

## Next

- Your own runnable: the *Architecture and Design Guide*, "Writing a
  runnable".
- Day-to-day operation, metrics, upgrades: the *Operator's Guide*.
