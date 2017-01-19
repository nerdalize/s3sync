package s3sync

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"

	"github.com/smartystreets/go-aws-auth"
)

//PrefixContent is the prefix used in S3 to store uploaded content. URL structure
//for chunks is <scheme>://<host>/<root>/content/<key>
const PrefixContent = "content"

//PrefixMetadata is the prefix used in S3 to store metadata for content.
//Metadata here is an index file containing references to the content chunks
//that together form a TAR.
const PrefixMetadata = "metadata"

//S3 is A boring s3 client
type S3 struct {
	Scheme string
	Host   string
	Root   string
	Client *http.Client
	Creds  awsauth.Credentials
}

//KeyURL returns the url to a key based on s3 config
func (s3 *S3) KeyURL(prefix string, k string) string {
	url := &url.URL{
		Scheme: s3.Scheme,
		Host:   s3.Host,
		Path:   path.Join(s3.Root, prefix, k),
	}
	return url.String()
}

//Has attempts to download header info for an S3 k
func (s3 *S3) Has(prefix string, k string) (has bool, err error) {
	raw := s3.KeyURL(prefix, k)
	loc, err := url.Parse(raw)
	if err != nil {
		return false, fmt.Errorf("failed to parse '%s' as url: %v", raw, err)
	}

	req, err := http.NewRequest("HEAD", loc.String(), nil)
	if err != nil {
		return false, fmt.Errorf("failed to create HEAD request: %v", err)
	}

	if s3.Creds.AccessKeyID != "" {
		awsauth.Sign4(req, s3.Creds)
	}

	resp, err := s3.Client.Do(req)
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
func (s3 *S3) Get(prefix string, k string) (resp *http.Response, err error) {
	raw := s3.KeyURL(prefix, k)
	loc, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("failed to parse '%s' as url: %v", raw, err)
	}

	req, err := http.NewRequest("GET", loc.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create GET request: %v", err)
	}

	if s3.Creds.AccessKeyID != "" {
		awsauth.Sign4(req, s3.Creds)
	}

	resp, err = s3.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to perform GET request: %v", err)
	}

	return resp, nil
}

//Put uploads a chunk to an S3 object store under the provided key 'k'
func (s3 *S3) Put(prefix string, k string, body io.Reader) error {
	raw := s3.KeyURL(prefix, k)
	loc, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("failed to parse '%s' as url: %v", raw, err)
	}

	req, err := http.NewRequest("PUT", loc.String(), body)
	if err != nil {
		return fmt.Errorf("failed to create PUT request: %v", err)
	}

	if s3.Creds.AccessKeyID != "" {
		awsauth.Sign4(req, s3.Creds)
	}

	resp, err := s3.Client.Do(req)
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
