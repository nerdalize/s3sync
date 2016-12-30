package s3sync

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"

	"github.com/restic/chunker"
)

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
		k := sha256.Sum256(it.chunk) //hash
		exists, err = s3.Has(k[:])   //check existence
		if err != nil {
			it.resCh <- &result{fmt.Errorf("failed to check existence of '%x': %v", k, err), ZeroKey}
			return
		}

		if !exists {
			err = s3.Put(k[:], bytes.NewBuffer(it.chunk)) //if not exists put
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

	//fan-in
	for it := range itemCh {
		if it.err != nil {
			return fmt.Errorf("failed to iterate: %v", it.err)
		}

		res := <-it.resCh
		if res.err != nil {
			return res.err
		}

		err = kw.Write(res.k)
		if err != nil {
			return fmt.Errorf("failed to write key: %v", err)
		}
	}

	return nil
}
