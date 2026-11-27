package admin

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dihedron/drun/internal/admin/adminpb"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// defaultMaxLag is a member's largest healthy lag, in entries (FR-17)
	defaultMaxLag = 256
	// checkEvery is how often the promotion gate checks a member
	checkEvery = time.Second
	// checkTimeout bounds one member's answer to a health check
	checkTimeout = 2 * time.Second
)

// MemberAdd adds a member; a voter goes through the promotion gate unless
// it is one already (FR-2, FR-17).
func (s *Server) MemberAdd(ctx context.Context, r *adminpb.MemberAddRequest) (*adminpb.Empty, error) {
	if r.GetId() == "" || r.GetAddress() == "" {
		return nil, status.Error(codes.InvalidArgument, "a member needs an ID and an address")
	}
	srv, err := s.member(r.GetId())
	if err != nil && status.Code(err) != codes.NotFound {
		return nil, err
	}
	switch {
	case r.GetNonVoter():
		err = s.o.Node.AddNonvoter(r.GetId(), r.GetAddress(), r.GetIdentity())
	case r.GetGate().GetForce() || (srv != nil && srv.Suffrage == raft.Voter):
		err = s.o.Node.AddVoter(r.GetId(), r.GetAddress(), r.GetIdentity())
	default:
		// a non-voter first, a voter once healthy
		if err = s.o.Node.AddNonvoter(r.GetId(), r.GetAddress(), r.GetIdentity()); err == nil {
			err = s.promote(ctx, r.GetId(), r.GetGate())
		}
	}
	return &adminpb.Empty{}, toStatus(err)
}

// MemberShow is every member's health, as this leader judges it.
func (s *Server) MemberShow(ctx context.Context, r *adminpb.MemberShowRequest) (*adminpb.MemberShowReply, error) {
	servers, err := s.members()
	if err != nil {
		return nil, err
	}
	out := &adminpb.MemberShowReply{}
	// shortcut: members are checked one after the other, each within
	// checkTimeout; check them concurrently if clusters grow large
	for _, srv := range servers {
		out.Members = append(out.Members, s.health(ctx, srv, r.GetMaxLag()))
	}
	return out, nil
}

// MemberPromote makes a non-voter a voter once healthy (FR-17).
func (s *Server) MemberPromote(ctx context.Context, r *adminpb.MemberPromoteRequest) (*adminpb.Empty, error) {
	return &adminpb.Empty{}, toStatus(s.promote(ctx, r.GetId(), r.GetGate()))
}

// MemberDemote makes a voter a non-voter; demoting the leader is a planned
// handover first, then the new leader demotes it (FR-17).
func (s *Server) MemberDemote(ctx context.Context, r *adminpb.MemberDemoteRequest) (*adminpb.Empty, error) {
	srv, err := s.member(r.GetId())
	if err != nil {
		return nil, err
	}
	if srv.Suffrage != raft.Voter {
		return nil, status.Errorf(codes.FailedPrecondition, "%s is not a voter", r.GetId())
	}
	servers, err := s.members()
	if err != nil {
		return nil, err
	}
	voters := 0
	for _, m := range servers {
		if m.Suffrage == raft.Voter {
			voters++
		}
	}
	if voters == 1 {
		return nil, status.Errorf(codes.FailedPrecondition, "%s is the last voter", r.GetId())
	}
	if r.GetId() != s.o.NodeID {
		return &adminpb.Empty{}, toStatus(s.o.Node.Demote(r.GetId()))
	}
	if err := s.o.Supervisor.Handover(ctx, ""); err != nil {
		return nil, toStatus(err)
	}
	reply := &adminpb.Empty{}
	return reply, s.forward(ctx, adminpb.Admin_MemberDemote_FullMethodName, r, reply)
}

// member is the configuration's entry of id
func (s *Server) member(id string) (*raft.Server, error) {
	servers, err := s.members()
	if err != nil {
		return nil, err
	}
	for _, srv := range servers {
		if string(srv.ID) == id {
			return &srv, nil
		}
	}
	return nil, status.Errorf(codes.NotFound, "%s is not a member", id)
}

// promote checks a non-voter every checkEvery and promotes it once it has
// been healthy for the gate's stable time, or fails when ctx ends
func (s *Server) promote(ctx context.Context, id string, gate *adminpb.Gate) error {
	srv, err := s.member(id)
	if err != nil {
		return err
	}
	if srv.Suffrage == raft.Voter {
		return status.Errorf(codes.FailedPrecondition, "%s is a voter already", id)
	}
	if !gate.GetForce() {
		stable := time.Duration(gate.GetStableMs()) * time.Millisecond
		var since time.Time // the start of the healthy streak
		for {
			h := s.health(ctx, *srv, gate.GetMaxLag())
			switch {
			case !h.GetHealthy():
				since = time.Time{}
			case since.IsZero():
				since = time.Now()
			}
			if !since.IsZero() && time.Since(since) >= stable {
				break
			}
			select {
			case <-ctx.Done():
				reason := h.GetReason()
				if reason == "" {
					reason = fmt.Sprintf("healthy for %s of %s", time.Since(since).Round(time.Millisecond), stable)
				}
				return fmt.Errorf("%s not promoted: %s: %w", id, reason, ctx.Err())
			case <-time.After(checkEvery):
			}
		}
	}
	if err := s.o.Node.AddVoter(id, string(srv.Address), ""); err != nil {
		return err
	}
	s.o.Logger.Info("member promoted", "member", id, "forced", gate.GetForce())
	return nil
}

// health judges a member from its own view against this leader's
func (s *Server) health(ctx context.Context, srv raft.Server, maxLag uint64) *adminpb.MemberHealth {
	if maxLag == 0 {
		maxLag = defaultMaxLag
	}
	_, leader := s.raft.LeaderWithID()
	h := &adminpb.MemberHealth{Id: string(srv.ID), Address: string(srv.Address), Suffrage: srv.Suffrage.String(), Leader: srv.ID == leader}
	if string(srv.ID) == s.o.NodeID {
		h.Healthy = true
		return h
	}
	view := &adminpb.ClusterShowReply{}
	cctx, cancel := context.WithTimeout(ctx, checkTimeout)
	defer cancel()
	if err := s.invokeOn(cctx, srv.ID, srv.Address, adminpb.Admin_ClusterShow_FullMethodName, &adminpb.Empty{}, view); err != nil {
		h.Reason = "unreachable: " + status.Convert(err).Message()
		return h
	}
	h.LastContact = view.GetLastContact()
	if commit := s.raft.CommitIndex(); commit > view.GetAppliedIndex() {
		h.Lag = commit - view.GetAppliedIndex()
	}
	h.Reason = judge(view, string(leader), parseUint(s.raft.Stats()["term"]), s.o.HeartbeatTimeout, h.Lag, maxLag)
	h.Healthy = h.Reason == ""
	return h
}

// judge returns why a member's view is not healthy, or ""
func judge(view *adminpb.ClusterShowReply, leader string, term uint64, maxContact time.Duration, lag, maxLag uint64) string {
	if view.GetLeaderId() != leader || view.GetTerm() != term {
		return fmt.Sprintf("follows %q in term %d, not %q in term %d", view.GetLeaderId(), view.GetTerm(), leader, term)
	}
	contact, err := lastContact(view.GetLastContact())
	if err != nil {
		return err.Error()
	}
	if contact > maxContact {
		return fmt.Sprintf("last contact %s ago, more than %s", contact, maxContact)
	}
	if lag > maxLag {
		return fmt.Sprintf("%d entries behind, more than %d", lag, maxLag)
	}
	return ""
}

// lastContact parses raft's last_contact stat: "never", "0" or a duration
func lastContact(s string) (time.Duration, error) {
	switch s {
	case "never", "":
		return 0, errors.New("never in contact with the leader")
	case "0":
		return 0, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("last contact %q: %w", s, err)
	}
	return d, nil
}
