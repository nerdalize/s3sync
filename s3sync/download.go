package s3sync

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
)

func DownloadProject(id string, dir string, concurrency int, s3 *S3) error {
	resp, err := s3.Get(PrefixMetadata, id)
	if err != nil {
		return fmt.Errorf("could not read metadata from object '%x': %v", id, err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status for downloading metadata '%x': %v", id, resp.Status)
	}

	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
	s := buf.String()
	var keys []K
	for _, keyString := range strings.Split(s, "\n") {
		if keyString == "" {
			break
		}
		bytes, e := hex.DecodeString(keyString)
		if e != nil {
			return fmt.Errorf("could not decode key string '%v': %v", keyString, err)
		}
		var key K
		copy(key[:], bytes)
		keys = append(keys, key)
	}

	kr := KeyReadWriter()
	kr.L = keys
	doneCh := make(chan error)
	pr, pw := io.Pipe()
	go func() {
		doneCh <- untardir(dir, pr)
	}()
	err = Download(kr, pw, concurrency, s3)
	if err != nil {
		return fmt.Errorf("failed to download project with uuid %s: %v", id, err)
	}
	return nil
}

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
		resp, err = s3.Get(PrefixContent, it.k.ToString())
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
