package profile

import (
	"bytes"
	"io"

	"github.com/profefe/profefe/internal/pprof/profile"
	"golang.org/x/xerrors"
)

type Reader interface {
	Next() bool
	Profile() *profile.Profile
	Bytes() []byte
	Err() error
	Close() error
}

type SingleProfileReader struct {
	pp  *profile.Profile
	r   io.Reader
	buf *bytes.Buffer
	err error
}

func NewSingleProfileReader(pp *profile.Profile) *SingleProfileReader {
	buf := new(bytes.Buffer)
	return &SingleProfileReader{
		pp:  pp,
		buf: buf,
		err: pp.Write(buf),
	}
}

func NewReaderFrom(r io.Reader) Reader {
	buf := new(bytes.Buffer)
	return &SingleProfileReader{
		r:   io.TeeReader(r, buf),
		buf: buf,
	}
}

func (pr *SingleProfileReader) Next() bool {
	if pr.err == nil && pr.pp == nil {
		pr.err = pr.parse()
	}

	if pr.err != nil {
		return false
	}

	// set EOF on first (successful) Next call
	pr.err = io.EOF

	return true
}

func (pr *SingleProfileReader) Profile() *profile.Profile {
	return pr.pp
}

func (pr *SingleProfileReader) Bytes() []byte {
	return pr.buf.Bytes()
}

func (pr *SingleProfileReader) parse() (err error) {
	if pr.pp != nil {
		return nil
	}

	// TODO(narqo): check if profile.Profile.Compact make any sense here
	pr.pp, err = profile.Parse(pr.r)
	if err != nil {
		err = xerrors.Errorf("could not parse profile: %w", err)
	}
	return err
}

func (pr *SingleProfileReader) Err() error {
	// XXX(narqo): ignore EOF set in Next
	if pr.err != io.EOF {
		return pr.err
	}
	return nil
}

func (pr *SingleProfileReader) Close() error {
	return nil
}
