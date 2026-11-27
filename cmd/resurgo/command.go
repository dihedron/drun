package main

import (
	"context"
	"errors"
	"io"
	"os"
	"time"

	"github.com/dihedron/resurgo/internal/admin"
)

// Command is the root of the command line (.specs/resurgo_specification.md
// §10.2): the process command (serve) and the clients of a node's admin
// API, as resurgo <object> <verb>.
type Command struct {
	Admin          string        `long:"admin" env:"RESURGO_ADMIN" default:"unix:///run/resurgo/admin.sock" description:"the node's admin API: unix:///path, or tls://host:port for a remote endpoint"`
	Cert           string        `long:"cert" env:"RESURGO_ADMIN_CERT" description:"client certificate for a tls:// admin endpoint"`
	Key            string        `long:"key" env:"RESURGO_ADMIN_KEY" description:"its private key"`
	CA             string        `long:"ca" env:"RESURGO_ADMIN_CA" description:"the CA of the node's certificate"`
	ServerIdentity string        `long:"server-identity" env:"RESURGO_ADMIN_SERVER_IDENTITY" description:"the identity the node's certificate must carry (default: any certificate of the CA)"`
	Timeout        time.Duration `long:"timeout" default:"90s" description:"how long a call may take (a handover waits for the runnables to stop)"`
	JSON           bool          `long:"json" description:"print replies as JSON"`

	Serve   ServeCommand `command:"serve" description:"run a node"`
	Cluster struct {
		Show ClusterShowCommand `command:"show" description:"members, suffrage, leader, term, commit index, last contact"`
	} `command:"cluster" description:"the cluster"`
	Member struct {
		Add    MemberAddCommand    `command:"add" description:"add a member (FR-2)"`
		Remove MemberRemoveCommand `command:"remove" description:"remove a member; removing the leader hands leadership over first"`
	} `command:"member" description:"the cluster's members"`
	Leader struct {
		Show     LeaderShowCommand     `command:"show" description:"the leader and its term"`
		Transfer LeaderTransferCommand `command:"transfer" description:"hand leadership over, stopping the runnables first (FR-3)"`
	} `command:"leader" description:"leadership"`
	Runnable struct {
		Show    RunnableShowCommand    `command:"show" description:"incarnation, epoch, digests, health, checkpoint, failures"`
		Enable  RunnableEnableCommand  `command:"enable" description:"enable a runnable"`
		Disable RunnableDisableCommand `command:"disable" description:"stop and disable a runnable"`
		Restart RunnableRestartCommand `command:"restart" description:"stop a runnable's incarnation and start a new one"`
		Reset   RunnableResetCommand   `command:"reset" description:"clear a runnable's checkpoint, completion or failures"`
	} `command:"runnable" description:"the runnables"`
	Checkpoint struct {
		Show CheckpointShowCommand `command:"show" description:"a runnable's checkpoint: its metadata, or with --data its bytes"`
	} `command:"checkpoint" description:"checkpoints"`
	Store struct {
		Show   StoreShowCommand   `command:"show" description:"size, tables, last index, and every member's digest"`
		Query  StoreQueryCommand  `command:"query" description:"a read-only statement, on the leader (or --local)"`
		Exec   StoreExecCommand   `command:"exec" description:"an operator's batch, through the log: the runnable stops and starts again after it"`
		Backup StoreBackupCommand `command:"backup" description:"a consistent copy of the database on the node called"`
		Reset  StoreResetCommand  `command:"reset" description:"drop the database (refused while the runnable runs)"`
	} `command:"store" description:"the runnables' stores"`
}

// root holds the global options, for the subcommands
var root *Command

// stdout is where replies go (tests replace it)
var stdout io.Writer = os.Stdout

// Execute runs when no subcommand is given.
func (cmd *Command) Execute(_ []string) error {
	return errors.New("no command given: run \"resurgo version\" or \"resurgo --help\"")
}

// call connects to the node's admin API and runs f within the timeout
func call(f func(ctx context.Context, c *admin.Client) error) error {
	c, err := admin.Dial(root.Admin, admin.ClientTLS{Cert: root.Cert, Key: root.Key, CA: root.CA, ServerIdentity: root.ServerIdentity})
	if err != nil {
		return err
	}
	defer c.Close() //nolint:errcheck // nothing to do about it
	ctx, cancel := context.WithTimeout(context.Background(), root.Timeout)
	defer cancel()
	return f(ctx, c)
}
