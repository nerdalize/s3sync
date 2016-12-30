package main_test

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dchest/safefile"
	"github.com/restic/chunker"
	"github.com/smartystreets/go-aws-auth"
)

const KiB = 1024
const MiB = KiB * 1024

func randr(size int64, seed int64) io.Reader {
	if seed == 0 {
		seed = time.Now().UnixNano()
	}

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

func testfile(dir string, name string, size, seed int64, t interface {
	Fatalf(format string, args ...interface{})
}) (os.FileInfo, []byte) {
	path := filepath.Join(dir, name)
	err := os.MkdirAll(filepath.Dir(path), 0777)
	if err != nil {
		t.Fatalf("failed to create file dir for '%v': %v", path, err)
	}

	data := randb(size, seed)
	err = ioutil.WriteFile(path, data, 0666)
	if err != nil {
		t.Fatalf("failed to write file '%v': %v", path, err)
	}

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("failed to stat test file '%v': %v", path, err)
	}

	return fi, data
}

type fataller interface {
	Fatalf(format string, args ...interface{})
}

func testdir(seed int64, t fataller) (string, int64, func(string, fataller)) {
	dir, err := ioutil.TempDir("", "s3sync_")
	if err != nil {
		t.Fatalf("failed to setup tempdir: %v", err)
	}

	testfis := []struct {
		name string
		size int64
		fi   os.FileInfo
		data []byte
	}{
		{name: " weird name.bin", size: 12 * MiB},
		{name: "b.bin", size: 1 * MiB},
		{name: "small.bin", size: 1 * KiB},
		{name: filepath.Join("dir_a", "small2.bin"), size: 1 * KiB},
	}

	var total int64
	for idx, tfi := range testfis {
		testfis[idx].fi, testfis[idx].data = testfile(dir, tfi.name, tfi.size, seed, t)
		total += tfi.size
	}

	testfunc := func(dir string, t fataller) {
		for _, tfi := range testfis {
			p := filepath.Join(dir, tfi.name)
			fi, err := os.Stat(p)
			if err != nil {
				t.Fatalf("failed to stat in test func: %v", err)
			}

			data, err := ioutil.ReadFile(p)
			if err != nil {
				t.Fatalf("failed to read data: %v", err)
			}

			if fi.IsDir() != tfi.fi.IsDir() {
				t.Fatalf("expected dir, got file")
			}

			if fi.ModTime() != tfi.fi.ModTime() {
				t.Fatalf("modtime: expected '%v', got: '%v'", tfi.fi.ModTime(), fi.ModTime())
			}

			if fi.Mode() != tfi.fi.Mode() {
				t.Fatalf("mode: expected '%s', got: '%v'", tfi.fi.Mode(), fi.Mode())
			}

			if fi.Size() != tfi.fi.Size() {
				t.Fatalf("size: expected '%d', got: '%d'", tfi.fi.Size(), fi.Size())
			}

			if !bytes.Equal(data, tfi.data) {
				t.Fatalf("content: not equal")
			}

		}
	}

	testfunc(dir, t) //immediately after creation should always succeed
	return dir, total, testfunc
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

type K [sha256.Size]byte

var ZeroKey = K{}

type KeyWriter interface {
	Write(k K) error
}

type KeyReader interface {
	Read() (K, error)
}

func upload(cr *chunker.Chunker, kw KeyWriter, concurrency int, s3 *S3) (err error) {
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
		k := sha256.Sum256(it.chunk) //hash
		exists, err := s3.Has(k[:])  //check existence
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

		err = kw.Write(res.k)
		if err != nil {
			return fmt.Errorf("failed to write key: %v", err)
		}
	}

	return nil
}

func download(kr KeyReader, cw io.Writer, concurrency int, s3 *S3) (err error) {
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
		resp, err := s3.Get(it.k[:])
		if err != nil {
			it.resCh <- &result{fmt.Errorf("failed to get key '%x': %v", it.k, err), nil}
			return
		}

		if resp.StatusCode != http.StatusOK {
			it.resCh <- &result{fmt.Errorf("unexpected status for '%x': %v", it.k, resp.Status), nil}
			return
		}

		chunk, err := ioutil.ReadAll(resp.Body)
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
			k, err := kr.Read()
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

func tardir(dir string, w io.Writer) (err error) {
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

func BenchmarkTarUntarDirectory(b *testing.B) {
	s3 := &S3{
		scheme: "http",
		host:   fmt.Sprintf("s3-%s.amazonaws.com", os.Getenv("AWS_REGION")),
		bucket: bucket(b),
		client: &http.Client{},
	}

	if s3.bucket == "" {
		b.Skip("`terraform output s3_bucket` not available")
	}

	dir, size, testfn := testdir(0, b)
	tarbuf := bytes.NewBuffer(nil)
	b.Run("tarring", func(b *testing.B) {
		b.SetBytes(size)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := tardir(dir, tarbuf)
			if err != nil {
				b.Errorf("failed to tar directory: %v", err)
			}
		}
	})

	//tar only reads, but for good measure
	testfn(dir, b)

	outdir, err := ioutil.TempDir("", "s3sync_")
	if err != nil {
		b.Fatalf("failed to create tempdir: %v", err)
	}

	b.Run("untarring", func(b *testing.B) {
		b.SetBytes(size)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := untardir(outdir, tarbuf)
			if err != nil {
				b.Errorf("failed to tar directory: %v", err)
			}
		}
	})

	//outtar should be equal
	testfn(outdir, b)
}

func BenchmarkUpDownFromMemory(b *testing.B) {
	s3 := &S3{
		scheme: "http",
		host:   fmt.Sprintf("s3-%s.amazonaws.com", os.Getenv("AWS_REGION")),
		bucket: bucket(b),
		client: &http.Client{},
	}

	if s3.bucket == "" {
		b.Skip("`terraform output s3_bucket` not available")
	}

	var krw = KeyReadWriter()
	var input = randb(12*1024*1024, 0)
	var output = bytes.NewBuffer(nil)

	b.Run("upload", func(b *testing.B) {
		benchmarkUpload(b, krw, input, s3)
	})

	b.Run("upload-same-input", func(b *testing.B) {
		benchmarkUpload(b, krw, input, s3)
	})

	b.Run("download", func(b *testing.B) {
		benchmarkDownload(b, krw, input, output, s3)
	})

	if !bytes.Equal(output.Bytes(), input) {
		b.Error("downloaded data should be equal to input")
	}
}

func benchmarkUpload(b *testing.B, krw *keys, data []byte, s3 *S3) {
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := bytes.NewReader(data)
		cr := chunker.New(r, chunker.Pol(0x3DA3358B4DC173))
		err := upload(cr, krw, 64, s3)
		if err != nil {
			b.Error(err)
		}

		if len(krw.L) < 1 {
			b.Error("expected at least some keys")
		}
	}
}

func benchmarkDownload(b *testing.B, krw *keys, data []byte, output io.Writer, s3 *S3) {
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := download(krw, output, 64, s3)
		if err != nil {
			b.Error(err)
		}
	}
}
