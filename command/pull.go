package command

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/nerdalize/s3sync/s3sync"
	"github.com/jessevdk/go-flags"
	"github.com/mitchellh/cli"
	"github.com/restic/chunker"
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

	cmd.parser = flags.NewNamedParser("s3sync commit <DIR>", flags.Default)
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

	fi, err := os.Stat(args[0])
	if err != nil {
		return fmt.Errorf("failed to inspect '%s' for commit: %v", args[0], err)
	} else if !fi.IsDir() {
		return fmt.Errorf("provided path '%s' is not a directory", args[0])
	}

	s3, err := cmd.opts.CreateS3Client(args[1])
	if err != nil {
		return err
	}

	done := make(chan struct{})
	pr, pw := io.Pipe()
	cr := chunker.New(pr, chunker.Pol(0x3DA3358B4DC173))
	go func() {
		err = s3sync.Upload(cr, &stdoutkw{}, 64, s3)
		if err != nil {
			fmt.Println("ERROR", err)
		}

		done <- struct{}{}
	}()

	err = s3sync.Tar(args[0], pw)
	if err != nil {
		return fmt.Errorf("failed to tar '%s': %v", args[0], err)
	}

	pw.Close()
	<-done

	return nil
}
