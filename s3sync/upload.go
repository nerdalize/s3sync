package s3sync

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"os"

	"github.com/restic/chunker"
)

func UploadProject(dir string, kw KeyWriter, concurrency int, s3 *S3) error {
	fi, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("failed to inspect '%s' for commit: %v", dir, err)
	} else if !fi.IsDir() {
		return fmt.Errorf("provided path '%s' is not a directory", dir)
	}

	id, err := GetProjectID(dir)
	if err != nil {
		return fmt.Errorf("could not get project ID: %v", err)
	}

	keyReader := KeyReadWriter()
	doneCh := make(chan error)
	pr, pw := io.Pipe()
	cr := chunker.New(pr, chunker.Pol(0x3DA3358B4DC173))
	go func() {
		doneCh <- Upload(cr, keyReader, concurrency, s3)
	}()

	err = Tar(dir, pw)
	if err != nil {
		return fmt.Errorf("failed to tar '%s': %v", dir, err)
	}

	pw.Close()
	err = <-doneCh
	if err != nil {
		return fmt.Errorf("failed to upload: %v", err)
	}
	//push index file
	buf := new(bytes.Buffer)
	for k, e := keyReader.Read(); e == nil; k, e = keyReader.Read() {
		err = kw.Write(k)
		if err != nil {
			return fmt.Errorf("failed to write key: %v", err)
		}
		buf.WriteString(fmt.Sprintf("%x\n", k))
	}
	err = s3.Put(PrefixMetadata, id, buf) //if not exists put
	if err != nil {
		return fmt.Errorf("failed to put metadata file with uuid %v: %v", id, err)
	}
	return nil
}

//Upload pushes chunks to s3 and writes them
func Upload(cr *chunker.Chunker, kw KeyWriter, concurrency int, s3 *S3) (err error) {
	type result struct {
		err error
		k   K
	}

	type item struct {
		chunk []byte
		resCh chan *result
		err   error
	}

	work := func(it *item) {
		var exists bool
		var k K = sha256.Sum256(it.chunk)                 //hash
		exists, err = s3.Has(PrefixContent, k.ToString()) //check existence
		if err != nil {
			it.resCh <- &result{fmt.Errorf("failed to check existence of '%x': %v", k, err), ZeroKey}
			return
		}

		if !exists {
			err = s3.Put(PrefixContent, k.ToString(), bytes.NewBuffer(it.chunk)) //if not exists put
			if err != nil {
				it.resCh <- &result{fmt.Errorf("failed to put chunk '%x': %v", k, err), ZeroKey}
				return
			}
		}

		it.resCh <- &result{nil, k}
	}

	//fan out
	itemCh := make(chan *item, concurrency)
	go func() {
		defer close(itemCh)
		buf := make([]byte, chunker.MaxSize)
		for {
			var chunk chunker.Chunk
			chunk, err = cr.Next(buf)
			if err != nil {
				if err != io.EOF {
					itemCh <- &item{err: err}
				}

				break
			}

			it := &item{
				chunk: make([]byte, chunk.Length),
				resCh: make(chan *result),
			}

			copy(it.chunk, chunk.Data) //underlying buffer is switched out

			go work(it)  //create work
			itemCh <- it //send to fan-in thread for syncing results
		}
	}()

	var keys []K

	//fan-in
	for it := range itemCh {
		if it.err != nil {
			return fmt.Errorf("failed to iterate: %v", it.err)
		}

		res := <-it.resCh
		if res.err != nil {
			return res.err
		}

		keys = append(keys, res.k)
		err = kw.Write(res.k)
		if err != nil {
			return fmt.Errorf("failed to write key: %v", err)
		}
	}
	return nil
}
