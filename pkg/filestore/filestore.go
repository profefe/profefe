package filestore

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/profefe/profefe/pkg/profile"
)

type FileStore struct {
	dataRoot string
}

func New(dataRoot string) (*FileStore, error) {
	err := os.MkdirAll(dataRoot, 0755)
	if err != nil {
		return nil, fmt.Errorf("could not create data root %q: %v", dataRoot, err)
	}
	fs := &FileStore{
		dataRoot: dataRoot,
	}
	return fs, nil
}

func (fs *FileStore) Get(ctx context.Context, dgst profile.Digest) (io.ReadCloser, error) {
	uri := fs.resolvePath(dgst)
	return os.Open(uri)
}

func (fs *FileStore) Save(ctx context.Context, r io.Reader) (dgst profile.Digest, size int64, data []byte, err error) {
	data, err = ioutil.ReadAll(r)
	if err != nil {
		return "", 0, nil, err
	}

	var uri string
	dgst, uri, err = fs.getDataDescriptor(data)
	if err != nil {
		return "", 0, nil, fmt.Errorf("could calculate descriptor for data: %v", err)
	}

	dataDir := filepath.Dir(uri)
	if err := os.MkdirAll(dataDir, 0740); err != nil {
		return "", 0, nil, fmt.Errorf("could not create data dir %q: %v", dataDir, err)
	}

	f, err := os.Create(uri)
	if err != nil {
		return "", 0, nil, fmt.Errorf("could not create data file %q: %v", uri, err)
	}
	defer func() {
		if err != nil {
			return
		}
		if err = f.Close(); err != nil {
			err = fmt.Errorf("could not close file %q: %v", uri, err)
		}
	}()

	n, err := f.Write(data)
	if err != nil {
		return "", 0, nil, fmt.Errorf("could not write data to file %q: %v", uri, err)
	}

	if err := f.Sync(); err != nil {
		return "", 0, nil, fmt.Errorf("could not flush file %q: %v", uri, err)
	}

	log.Printf("DEBUG put: dgst %s, uri %s, size %d\n", dgst, uri, size)

	return dgst, int64(n), data, nil
}

func (fs *FileStore) Delete(ctx context.Context, dgst profile.Digest) error {
	uri := fs.resolvePath(dgst)
	return os.Remove(uri)
}

func (fs *FileStore) getDataDescriptor(data []byte) (dgst profile.Digest, uri string, err error) {
	h := sha1.New()
	if _, err := h.Write(data); err != nil {
		return "", "", err
	}
	dgstStr := hex.EncodeToString(h.Sum(nil))
	dgst = profile.Digest(dgstStr)
	uri = fs.resolvePath(dgst)
	return dgst, uri, nil
}

func (fs *FileStore) resolvePath(dgst profile.Digest) string {
	var group = "0000"
	if len(dgst) > 4 {
		group = string(dgst)[:4]
	}
	return filepath.Join(fs.dataRoot, group[:2], group[2:], string(dgst))
}
