package command

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/nerdalize/s3sync/s3sync"
	"github.com/smartystreets/go-aws-auth"
)

//S3Opts configur how to interact with S3
type S3Opts struct {
	S3Scheme       string `long:"s3-scheme" default:"https" value-name:"https" description:"..."`
	S3Host         string `long:"s3-host" default:"s3.amazonaws.com" value-name:"s3.amazonaws.com" description:"..."`
	S3Prefix       string `long:"s3-prefix" description:"..."`
	S3AccessKey    string `long:"s3-access-key" value-name:"AWS_ACCESS_KEY_ID" description:"..."`
	S3SecretKey    string `long:"s3-secret-key" value-name:"AWS_SECRET_ACCESS_KEY" description:"..."`
	S3SessionToken string `long:"s3-session-token" value-name:"AWS_SESSION_TOKEN" description:"..."`
}

//CreateS3Client uses command line options to create an s3 client
func (opts *S3Opts) CreateS3Client(ep string) (s3 *s3sync.S3, err error) {
	loc, err := url.Parse(ep)
	if err != nil {
		return nil, fmt.Errorf("failed to parse '%s' as url: %v", ep, err)
	}

	if os.Getenv("AWS_REGION") != "" {
		opts.S3Host = fmt.Sprintf("s3-%s.amazonaws.com", os.Getenv("AWS_REGION"))
	}

	if loc.Host != "" {
		opts.S3Host = loc.Host
	}

	if loc.Path != "" {
		opts.S3Prefix = strings.Trim(loc.Path, "/")
	}

	if opts.S3AccessKey == "" {
		opts.S3AccessKey = os.Getenv("AWS_ACCESS_KEY_ID")
	}

	if opts.S3SecretKey == "" {
		opts.S3SecretKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
	}

	if opts.S3SessionToken == "" {
		opts.S3SessionToken = os.Getenv("AWS_SESSION_TOKEN")
	}

	if loc.Scheme != "" {
		opts.S3Scheme = loc.Scheme
	}

	s3 = &s3sync.S3{
		Scheme: opts.S3Scheme,
		Host:   opts.S3Host,
		Prefix: opts.S3Prefix,
		Client: &http.Client{},
		Creds: awsauth.Credentials{
			AccessKeyID:     opts.S3AccessKey,
			SecretAccessKey: opts.S3SecretKey,
			SecurityToken:   opts.S3SessionToken,
		},
	}

	return s3, nil
}
