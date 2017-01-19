package main_test

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dchest/safefile"
	"github.com/nerdalize/s3sync/s3sync"
	"github.com/restic/chunker"
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

type skipper interface {
	Skip(...interface{})
}

func root(t skipper) string {
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

func s3(t skipper) (s3 *s3sync.S3) {
	s3 = &s3sync.S3{
		Scheme: "http",
		Host:   fmt.Sprintf("s3-%s.amazonaws.com", os.Getenv("AWS_REGION")),
		Root:   root(t),
		Client: &http.Client{},
	}

	if s3.Root == "" {
		t.Skip("`terraform output s3_bucket` not available")
	}

	return s3
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

type keys struct {
	*sync.Mutex
	pos int
	M   map[s3sync.K]struct{}
	L   []s3sync.K
}

func KeyReadWriter() *keys {
	return &keys{Mutex: &sync.Mutex{}, M: map[s3sync.K]struct{}{}}
}

func (kw *keys) Write(k s3sync.K) error {
	kw.Lock()
	defer kw.Unlock()
	if _, ok := kw.M[k]; ok {
		return nil
	}

	kw.M[k] = struct{}{}
	kw.L = append(kw.L, k)
	return nil
}

func (kw *keys) Read() (k s3sync.K, err error) {
	kw.Lock()
	defer kw.Unlock()
	if kw.pos == len(kw.L) {
		return s3sync.ZeroKey, io.EOF
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

func BenchmarkTarUntarDirectory(b *testing.B) {
	// s3 := s3(b)
	dir, size, testfn := testdir(0, b)
	tarbuf := bytes.NewBuffer(nil)
	b.Run("tarring", func(b *testing.B) {
		b.SetBytes(size)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := s3sync.Tar(dir, tarbuf)
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

	//outtar results should be equal
	testfn(outdir, b)
}

func BenchmarkUpDownFromMemory(b *testing.B) {
	s3 := s3(b)
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

func benchmarkUpload(b *testing.B, krw *keys, data []byte, s3 *s3sync.S3) {
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := bytes.NewReader(data)
		cr := chunker.New(r, chunker.Pol(0x3DA3358B4DC173))
		err := s3sync.Upload(cr, krw, 64, s3)
		if err != nil {
			b.Error(err)
		}

		if len(krw.L) < 1 {
			b.Error("expected at least some keys")
		}
	}
}

func benchmarkDownload(b *testing.B, krw *keys, data []byte, output io.Writer, s3 *s3sync.S3) {
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := s3sync.Download(krw, output, 64, s3)
		if err != nil {
			b.Error(err)
		}
	}
}
