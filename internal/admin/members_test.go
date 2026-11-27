package admin

import (
	"strings"
	"testing"
	"time"

	"github.com/dihedron/drun/internal/admin/adminpb"
)

func TestJudge(t *testing.T) {
	ok := func() *adminpb.ClusterShowReply {
		return &adminpb.ClusterShowReply{LeaderId: "l", Term: 3, LastContact: "12.5ms"}
	}
	for _, c := range []struct {
		what   string
		view   func(*adminpb.ClusterShowReply)
		lag    uint64
		reason string // a substring; "" for healthy
	}{
		{"healthy", func(*adminpb.ClusterShowReply) {}, 10, ""},
		{"another leader", func(v *adminpb.ClusterShowReply) { v.LeaderId = "x" }, 0, `follows "x"`},
		{"another term", func(v *adminpb.ClusterShowReply) { v.Term = 2 }, 0, "term 2"},
		{"never", func(v *adminpb.ClusterShowReply) { v.LastContact = "never" }, 0, "never in contact"},
		{"stale contact", func(v *adminpb.ClusterShowReply) { v.LastContact = "2s" }, 0, "last contact 2s"},
		{"garbage contact", func(v *adminpb.ClusterShowReply) { v.LastContact = "soon" }, 0, `"soon"`},
		{"lagging", func(*adminpb.ClusterShowReply) {}, 300, "300 entries behind"},
		{"at the limit", func(*adminpb.ClusterShowReply) {}, 256, ""},
	} {
		v := ok()
		c.view(v)
		got := judge(v, "l", 3, time.Second, c.lag, defaultMaxLag)
		if (c.reason == "") != (got == "") || !strings.Contains(got, c.reason) {
			t.Errorf("%s: %q, want %q", c.what, got, c.reason)
		}
	}
}
