package command

import (
	"fmt"
	"os"

	"github.com/nerdalize/s3sync/s3sync"
)

type stdoutkw struct{}

func (kw *stdoutkw) Write(k s3sync.K) (err error) {
	_, err = fmt.Fprintf(os.Stdout, "%x\n", k)
	return err
}
