package s3sync

import (
	"archive/tar"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

//Tar archives the given directory and writes bytes to
func Tar(dir string, w io.Writer) (err error) {
	tw := tar.NewWriter(w)
	err = filepath.Walk(dir, func(path string, fi os.FileInfo, err error) error {
		if fi.Mode().IsDir() {
			return nil
		}

		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return fmt.Errorf("failed to determine path '%s' relative to '%s': %v", path, dir, err)
		}

		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("failed to open file '%s': %v", rel, err)
		}

		err = tw.WriteHeader(&tar.Header{
			Name:    rel,
			Mode:    int64(fi.Mode()),
			ModTime: fi.ModTime(),
			Size:    fi.Size(),
		})
		if err != nil {
			return fmt.Errorf("failed to write tar header for '%s': %v", rel, err)
		}

		defer f.Close()
		n, err := io.Copy(tw, f)
		if err != nil {
			return fmt.Errorf("failed to write tar file for '%s': %v", rel, err)
		}

		if n != fi.Size() {
			return fmt.Errorf("unexpected nr of bytes written to tar, saw '%d' on-disk but only wrote '%d', is directory '%s' in use?", fi.Size(), n, dir)
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to walk dir '%s': %v", dir, err)
	}

	if err = tw.Close(); err != nil {
		return fmt.Errorf("failed to write remaining data: %v", err)
	}

	return nil
}
