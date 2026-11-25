package main

import "errors"

// Command is the root of the command line (.specs/resurgo_specification.md
// §9): the process command (serve) and the clients of a node's admin socket.
// The subcommands are added with their implementation, once the
// specification is approved (AGENTS.md, rule 1).
type Command struct{}

// Execute runs when no subcommand is given.
func (cmd *Command) Execute(_ []string) error {
	return errors.New("no command given: run \"resurgo version\" or \"resurgo --help\"")
}
