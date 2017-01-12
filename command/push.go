package command

import (
	"bytes"
	"fmt"
	"os"

	"github.com/jessevdk/go-flags"
	"github.com/mitchellh/cli"
	"github.com/nerdalize/s3sync/s3sync"
)

//PushOpts describes command options
type PushOpts struct {
	S3Opts
}

//Push command
type Push struct {
	ui     cli.Ui
	opts   *PushOpts
	parser *flags.Parser
}

//PushFactory returns a factory method for the join command
func PushFactory() func() (cmd cli.Command, err error) {
	cmd := &Push{
		opts: &PushOpts{},
		ui:   &cli.BasicUi{Reader: os.Stdin, Writer: os.Stderr},
	}

	cmd.parser = flags.NewNamedParser("s3sync push <DIR>", flags.Default)
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
func (cmd *Push) Help() string {
	buf := bytes.NewBuffer(nil)
	cmd.parser.WriteHelp(buf)

	return fmt.Sprintf(`
  %s

%s`, cmd.Synopsis(), buf.String())
}

// Synopsis returns a one-line, short synopsis of the command.
// This should be less than 50 characters ideally.
func (cmd *Push) Synopsis() string {
	return "commit and upload a new version of a directory"
}

// Run runs the actual command with the given CLI instance and
// command-line arguments. It returns the exit status when it is
// finished.
func (cmd *Push) Run(args []string) int {
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
func (cmd *Push) DoRun(args []string) (err error) {
	if len(args) < 1 {
		return fmt.Errorf("not enough arguments, use --help for more information")
	}

	s3, err := cmd.opts.CreateS3Client()
	if err != nil {
		return err
	}

	cmd.ui.Info(fmt.Sprintf("pushing to %s", s3.KeyURL(s3sync.PrefixContent, "")))

	err = s3sync.UploadProject(args[0], &stdoutkw{}, 64, s3)
	if err != nil {
		return fmt.Errorf("failed to upload: %v", err)
	}

	return nil
}
