package command

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"

	"github.com/nerdalize/s3sync/s3sync"
	uuid "github.com/satori/go.uuid"
)

//ProjectIDFile contains the nerdalize storage project ID for a given folder.
const ProjectIDFile = ".nlz-storage-project"

type stdoutkw struct{}

func (kw *stdoutkw) Write(k s3sync.K) (err error) {
	_, err = fmt.Fprintf(os.Stdout, "%x\n", k)
	return err
}

//GetProjectID returns the nerdalize storage project ID for a given folder.
func GetProjectID(dirPath string) (uuid.UUID, error) {
	file := path.Join(dirPath, ProjectIDFile)
	if _, err := os.Stat(file); os.IsNotExist(err) {
		id := uuid.NewV4()
		f, err := os.Create(file)
		defer f.Close()
		if err != nil {
			return uuid.UUID{}, fmt.Errorf("could not create project ID file '%v': %v", file, err)
		}
		f.Write(id[:])
		f.Sync()
		return id, nil
	}
	dat, err := ioutil.ReadFile(file)
	if err != nil {
		return uuid.UUID{}, fmt.Errorf("could not read project ID file '%v': %v", file, err)
	}
	var id uuid.UUID
	copy(id[:], dat)
	return id, nil
}
