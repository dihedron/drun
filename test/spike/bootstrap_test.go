package spike

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// EXT-2: calling BootstrapCluster with identical configurations on all
// initial voters is safe.
func TestEXT2IdenticalBootstrapOnAllVoters(t *testing.T) {
	for _, size := range []int{3, 5} {
		t.Run(fmt.Sprintf("%d-nodes", size), func(t *testing.T) {
			for round := range *repeat {
				bootstrapAll(t, size, round)
			}
		})
	}
}

func bootstrapAll(t *testing.T, size, round int) {
	c := newNodes(t, size)
	defer c.shutdown()
	cfg := c.configuration()

	var wg sync.WaitGroup
	errs := make([]error, size)
	for _, i := range rand.Perm(size) {
		wg.Go(func() {
			time.Sleep(time.Duration(rand.IntN(50)) * time.Millisecond)
			n := c.nodes[i]
			c.start(n, nil)
			errs[i] = n.raft.BootstrapCluster(cfg).Error()
		})
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
			t.Fatalf("round %d: bootstrap of node %d: %v", round, i, err)
		}
	}

	l := c.leader(5 * time.Second)
	if err := l.raft.Barrier(2 * time.Second).Error(); err != nil {
		t.Fatalf("round %d: barrier: %v", round, err)
	}
	for _, n := range c.nodes {
		f := n.raft.GetConfiguration()
		if err := f.Error(); err != nil {
			t.Fatalf("round %d: configuration of %s: %v", round, n.id, err)
		}
		if got := f.Configuration(); len(got.Servers) != size {
			t.Fatalf("round %d: %s has %d servers, want %d", round, n.id, len(got.Servers), size)
		}
		if n.raft.CurrentTerm() == 0 {
			t.Fatalf("round %d: %s still in term 0", round, n.id)
		}
	}
}
