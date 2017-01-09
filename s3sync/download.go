package s3sync

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
)

//Download pulls chunks from s3 and writes them
func Download(kr KeyReader, cw io.Writer, concurrency int, s3 *S3) (err error) {
	type result struct {
		err   error
		chunk []byte
	}

	type item struct {
		k     K
		resCh chan *result
		err   error
	}

	work := func(it *item) {
		var resp *http.Response
		resp, err = s3.Get(BucketContent, it.k[:])
		if err != nil {
			it.resCh <- &result{fmt.Errorf("failed to get key '%x': %v", it.k, err), nil}
			return
		}

		if resp.StatusCode != http.StatusOK {
			it.resCh <- &result{fmt.Errorf("unexpected status for '%x': %v", it.k, resp.Status), nil}
			return
		}

		var chunk []byte
		chunk, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			it.resCh <- &result{fmt.Errorf("failed to get read response body for '%x': %v", it.k, err), nil}
			return
		}

		it.resCh <- &result{nil, chunk}
	}

	//fan out
	itemCh := make(chan *item, concurrency)
	go func() {
		defer close(itemCh)
		for {
			var k K
			k, err = kr.Read()
			if err != nil {
				if err != io.EOF {
					itemCh <- &item{err: err}
				}

				break
			}

			it := &item{
				k:     k,
				resCh: make(chan *result),
			}

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

		_, err = cw.Write(res.chunk)
		if err != nil {
			return fmt.Errorf("failed to write key: %v", err)
		}
	}

	return nil
}
