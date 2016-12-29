package main_test

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/restic/chunker"
	"github.com/smartystreets/go-aws-auth"
)

func randr(size int64, seed int64) io.Reader {
	if seed == 0 {
		seed = time.Now().UnixNano()
	}

	log.Printf("used seed '%d'", seed)
	return io.LimitReader(rand.New(rand.NewSource(seed)), size)
}

func randb(size int64, seed int64) []byte {
	b, err := ioutil.ReadAll(randr(size, seed))
	if err != nil {
		panic(err)
	}

	return b
}

func bucket(t interface {
	Skip(...interface{})
}) string {
	buf := bytes.NewBuffer(nil)
	cmd := exec.Command("terraform", "output", "s3_bucket")
	cmd.Stderr = os.Stderr
	cmd.Stdout = buf
	err := cmd.Run()
	if err != nil {
		t.Skip("skipping integration, couldnt get api endpoint through terraform: " + err.Error())
	}

	return strings.TrimSpace(buf.String())
}

//A boring s3 client
type S3 struct {
	scheme string
	host   string
	bucket string
	client *http.Client
	creds  awsauth.Credentials
}

//Has attempts to download header info for an S3 k
func (s3 *S3) Has(k []byte) (has bool, err error) {
	raw := fmt.Sprintf("%s://%s/%s/%x", s3.scheme, s3.host, s3.bucket, k)
	loc, err := url.Parse(raw)
	if err != nil {
		return false, fmt.Errorf("failed to parse '%s' as url: %v", raw, err)
	}

	req, err := http.NewRequest("HEAD", loc.String(), nil)
	if err != nil {
		return false, fmt.Errorf("failed to create HEAD request: %v", err)
	}

	if s3.creds.AccessKeyID != "" {
		awsauth.Sign(req, s3.creds)
	}

	resp, err := s3.client.Do(req)
	if err != nil {
		return false, fmt.Errorf("failed to perform HEAD request: %v", err)
	}

	if resp.StatusCode == http.StatusOK {
		return true, nil
	} else if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusForbidden {
		//AWS returns forbidden for a HEAD request if the one performing the operation does not have
		//list bucket permissions
		return false, nil
	} else {
		return false, fmt.Errorf("unexpected response from HEAD '%s' request: %s", loc, resp.Status)
	}
}

//Get attempts to download chunk 'k' from an S3 object store
func (s3 *S3) Get(k []byte) (resp *http.Response, err error) {
	raw := fmt.Sprintf("%s://%s/%s/%x", s3.scheme, s3.host, s3.bucket, k)
	loc, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("failed to parse '%s' as url: %v", raw, err)
	}

	req, err := http.NewRequest("GET", loc.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create GET request: %v", err)
	}

	if s3.creds.AccessKeyID != "" {
		awsauth.Sign(req, s3.creds)
	}

	resp, err = s3.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to perform GET request: %v", err)
	}

	return resp, nil
}

//Put uploads a chunk to an S3 object store under the provided key 'k'
func (s3 *S3) Put(k []byte, body io.Reader) error {
	raw := fmt.Sprintf("%s://%s/%s/%x", s3.scheme, s3.host, s3.bucket, k)
	loc, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("failed to parse '%s' as url: %v", raw, err)
	}

	req, err := http.NewRequest("PUT", loc.String(), body)
	if err != nil {
		return fmt.Errorf("failed to create PUT request: %v", err)
	}

	if s3.creds.AccessKeyID != "" {
		awsauth.Sign(req, s3.creds)
	}

	resp, err := s3.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to perform PUT request: %v", err)
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("failed to read response body for unexpected response: %s", resp.Status)
		}

		return fmt.Errorf("unexpected response from PUT '%s' response: %s, body: %v", loc, resp.Status, string(body))
	}

	return nil
}

func upload(cr *chunker.Chunker, concurrency int, s3 *S3) (err error) {
	type result struct {
		err error
	}

	type item struct {
		chunk []byte
		resCh chan *result
		err   error
	}

	work := func(it *item) {
		start := time.Now()

		k := sha256.Sum256(it.chunk) //hash

		exists, err := s3.Has(k[:]) //check existence
		if err != nil {
			it.resCh <- &result{fmt.Errorf("failed to check existence of '%x': %v", k, err)}
			return
		}

		if !exists {
			err = s3.Put(k[:], bytes.NewBuffer(it.chunk)) //if not exists put
			if err != nil {
				it.resCh <- &result{fmt.Errorf("failed to put chunk '%x': %v", k, err)}
				return
			}
		}

		log.Printf("k: %x size: %d KiB, exists: %v, t: %s", k, len(it.chunk)/1024, exists, time.Since(start))
		it.resCh <- &result{}
	}

	//fan out
	itemCh := make(chan *item, concurrency)
	go func() {
		defer close(itemCh)
		buf := make([]byte, chunker.MaxSize)
		for {
			chunk, err := cr.Next(buf)
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
	}

	return nil
}

func BenchmarkUpload(b *testing.B) {
	s3 := &S3{
		scheme: "http",
		host:   fmt.Sprintf("s3-%s.amazonaws.com", os.Getenv("AWS_REGION")),
		bucket: bucket(b),
		client: &http.Client{},
	}

	if s3.bucket == "" {
		b.Skip("`terraform output s3_bucket` not available")
	}

	size := int64(12 * 1024 * 1024)
	data := randb(size, 1483019052349111311)
	sha := sha256.Sum256(data)
	log.Printf("sha: %x", sha)

	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := bytes.NewReader(data)
		cr := chunker.New(r, chunker.Pol(0x3DA3358B4DC173))
		err := upload(cr, 64, s3)
		if err != nil {
			b.Error(err)
		}

	}
}
