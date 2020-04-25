package pprofutil

import (
	"bytes"
	"io"
	"io/ioutil"

	pprofProfile "github.com/profefe/profefe/internal/pprof/profile"
)

type ProfileParser struct {
	r    *bytes.Reader
	prof *pprofProfile.Profile
}

func NewProfileParser(data []byte) *ProfileParser {
	return &ProfileParser{r: bytes.NewReader(data)}
}

func (pr *ProfileParser) Read(p []byte) (n int, err error) {
	return pr.r.Read(p)
}

func (pr *ProfileParser) WriteTo(w io.Writer) (n int64, err error) {
	return pr.r.WriteTo(w)
}

func (pr *ProfileParser) Seek(offset int64, whence int) (int64, error) {
	return pr.r.Seek(offset, whence)
}

func (pr *ProfileParser) ParseProfile() (prof *pprofProfile.Profile, err error) {
	if pr.prof == nil {
		pr.prof, err = pprofProfile.Parse(pr.r)
	}
	return pr.prof, err
}

func ParseProfileFrom(r io.Reader) (*pprofProfile.Profile, error) {
	if pr, _ := r.(*ProfileParser); pr != nil {
		return pr.ParseProfile()
	}
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return NewProfileParser(data).ParseProfile()
}
