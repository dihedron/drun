package main

import (
	_ "embed"
	"errors"
	"fmt"
	"os"

	"github.com/dihedron/resurgo/pkg/metadata"
	"github.com/jessevdk/go-flags"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

// run is main without the exit, so that the deferred cleanup runs.
func run(args []string) int {
	defer cleanup()

	if len(args) >= 1 && (args[0] == "version" || args[0] == "--version" || args[0] == "v" || args[0] == "--ver") {
		if len(args) >= 2 && (args[1] == "--verbose" || args[1] == "-v") {
			metadata.PrintFull(os.Stdout)
			return 0
		}
		metadata.Print(os.Stdout)
		return 0
	}

	if len(args) >= 1 && args[0] == "completion" {
		if len(args) != 2 || (args[1] != "bash" && args[1] != "zsh") {
			fmt.Fprintln(os.Stderr, "usage: resurgo completion bash|zsh")
			return 1
		}
		fmt.Print(completionScript)
		return 0
	}

	command := Command{}
	parser := flags.NewParser(&command, flags.HelpFlag|flags.PassDoubleDash)
	// without a subcommand, the root's Execute explains what to run
	parser.SubcommandsOptional = true
	if _, err := parser.ParseArgs(args); err != nil {
		var flagsErr *flags.Error
		if errors.As(err, &flagsErr) && flagsErr.Type == flags.ErrHelp {
			_, _ = fmt.Fprintln(os.Stdout, err)
			return 0
		}
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}
	if parser.Active == nil {
		if err := command.Execute(nil); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			return 1
		}
	}
	return 0
}

// completionScript completes resurgo's commands and options in bash, and in
// zsh through bashcompinit: go-flags answers when GO_FLAGS_COMPLETION is set,
// so the completions always match the binary.
//
//go:embed completion.bash
var completionScript string
