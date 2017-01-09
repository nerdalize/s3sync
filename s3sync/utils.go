package s3sync

import (
	"archive/tar"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/dchest/safefile"
	uuid "github.com/hashicorp/go-uuid"
)

//ProjectIDFile contains the nerdalize storage project ID for a given folder.
const ProjectIDFile = ".nlz-storage-project"

type keys struct {
	*sync.Mutex
	pos int
	M   map[K]struct{}
	L   []K
}

func KeyReadWriter() *keys {
	return &keys{Mutex: &sync.Mutex{}, M: map[K]struct{}{}}
}

func (kw *keys) Write(k K) error {
	kw.Lock()
	defer kw.Unlock()
	if _, ok := kw.M[k]; ok {
		return nil
	}

	kw.M[k] = struct{}{}
	kw.L = append(kw.L, k)
	return nil
}

func (kw *keys) Read() (k K, err error) {
	kw.Lock()
	defer kw.Unlock()
	if kw.pos == len(kw.L) {
		return ZeroKey, io.EOF
	}

	k = kw.L[kw.pos]
	kw.pos = kw.pos + 1
	return k, nil
}

//GetProjectID returns the nerdalize storage project ID for a given folder.
func GetProjectID(dirPath string) (string, error) {
	file := path.Join(dirPath, ProjectIDFile)
	if _, err := os.Stat(file); os.IsNotExist(err) {
		id, err := uuid.GenerateUUID()
		if err != nil {
			return "", fmt.Errorf("could not generate uuid: %v", err)
		}
		f, err := os.Create(file)
		defer f.Close()
		if err != nil {
			return "", fmt.Errorf("could not create project ID file '%v': %v", file, err)
		}
		f.Write([]byte(id))
		f.Sync()
		return id, nil
	}
	dat, err := ioutil.ReadFile(file)
	if err != nil {
		return "", fmt.Errorf("could not read project ID file '%v': %v", file, err)
	}
	return string(dat), nil
}

func untardir(dir string, r io.Reader) (err error) {
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err != nil {
			if err == io.EOF {
				break
			}

			return fmt.Errorf("failed to read next tar header: %v", err)
		}

		path := filepath.Join(dir, hdr.Name)
		err = os.MkdirAll(filepath.Dir(path), 0777)
		if err != nil {
			return fmt.Errorf("failed to create dirs: %v", err)
		}

		f, err := safefile.Create(path, os.FileMode(hdr.Mode))
		if err != nil {
			return fmt.Errorf("failed to create tmp safe file: %v", err)
		}

		defer f.Close()
		n, err := io.Copy(f, tr)
		if err != nil {
			return fmt.Errorf("failed to write file content to tmp file: %v", err)
		}

		if n != hdr.Size {
			return fmt.Errorf("unexpected nr of bytes written, wrote '%d' saw '%d' in tar hdr", n, hdr.Size)
		}

		err = f.Commit()
		if err != nil {
			return fmt.Errorf("failed to swap old file for tmp file: %v", err)
		}

		err = os.Chtimes(path, time.Now(), hdr.ModTime)
		if err != nil {
			return fmt.Errorf("failed to change times of tmp file: %v", err)
		}
	}

	return nil
}
