package command

import (
	"bytes"
	"fmt"
	"os"

	uuid "github.com/hashicorp/go-uuid"
	"github.com/jessevdk/go-flags"
	"github.com/mitchellh/cli"
	"github.com/nerdalize/s3sync/s3sync"
)

//PullOpts describes command options
type PullOpts struct {
	S3Opts
}

//Pull command
type Pull struct {
	ui     cli.Ui
	opts   *PullOpts
	parser *flags.Parser
}

//PullFactory returns a factory method for the join command
func PullFactory() func() (cmd cli.Command, err error) {
	cmd := &Pull{
		opts: &PullOpts{},
		ui:   &cli.BasicUi{Reader: os.Stdin, Writer: os.Stderr},
	}

	cmd.parser = flags.NewNamedParser("s3sync pull <UUID> <DIR>", flags.Default)
	_, err := cmd.parser.AddGroup("options", "options", cmd.opts)
	if err != nil {
		panic(err)
	}

	return func() (cli.Command, error) {
		return cmd, nil
	}
}

// Help returns long-form help text that includes the command-line
// usage, a brief few sentences explaining the function of the command,
// and the complete list of flags the command accepts.
func (cmd *Pull) Help() string {
	buf := bytes.NewBuffer(nil)
	cmd.parser.WriteHelp(buf)

	return fmt.Sprintf(`
  %s

%s`, cmd.Synopsis(), buf.String())
}

// Synopsis returns a one-line, short synopsis of the command.
// This should be less than 50 characters ideally.
func (cmd *Pull) Synopsis() string {
	return "commit and upload a new version of a directory"
}

// Run runs the actual command with the given CLI instance and
// command-line arguments. It returns the exit status when it is
// finished.
func (cmd *Pull) Run(args []string) int {
	a, err := cmd.parser.ParseArgs(args)
	if err != nil {
		cmd.ui.Error(err.Error())
		return 127
	}

	if err := cmd.DoRun(a); err != nil {
		cmd.ui.Error(err.Error())
		return 1
	}

	return 0
}

//DoRun is called by run and allows an error to be returned
func (cmd *Pull) DoRun(args []string) (err error) {
	if len(args) < 2 {
		return fmt.Errorf("not enough arguments, use --help for more information")
	}

	fi, err := os.Stat(args[1])
	if err != nil {
		return fmt.Errorf("failed to inspect '%s' for commit: %v", args[1], err)
	} else if !fi.IsDir() {
		return fmt.Errorf("provided path '%s' is not a directory", args[1])
	} else if _, err = uuid.ParseUUID(args[0]); err != nil {
		return fmt.Errorf("provided UUID '%v' is not valid: %v", args[0], err)
	}

	s3, err := cmd.opts.CreateS3Client()
	if err != nil {
		return err
	}

	//TODO: check for valid UUID (really stupid to blindly copy user input)
	err = s3sync.DownloadProject(args[0], args[1], 64, s3)
	if err != nil {
		return fmt.Errorf("could not download project: %v", err)
	}

	return nil
}
