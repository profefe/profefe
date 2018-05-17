package store

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
)

type fsBlobStore struct {
	dataRoot string
}

func newFsBlobStore(dataRoot string) (*fsBlobStore, error) {
	err := os.MkdirAll(dataRoot, 0755)
	if err != nil {
		return nil, fmt.Errorf("could not create data root %q: %v", dataRoot, err)
	}
	fs := &fsBlobStore{
		dataRoot: dataRoot,
	}
	return fs, nil
}

func (fs *fsBlobStore) Get(ctx context.Context, dgst string) (io.ReadCloser, error) {
	uri := fs.resolvePath(dgst)
	return os.Open(uri)
}

func (fs *fsBlobStore) Put(ctx context.Context, data []byte) (dgst string, size int64, err error) {
	var uri string
	dgst, uri, err = fs.blobDescriptor(data)
	if err != nil {
		return "", 0, fmt.Errorf("could calculate descriptor for data: %v", err)
	}

	dataDir := filepath.Dir(uri)
	if err := os.MkdirAll(dataDir, 0740); err != nil {
		return "", 0, fmt.Errorf("could not create data dir %q: %v", dataDir, err)
	}

	f, err := os.Create(uri)
	if err != nil {
		return "", 0, fmt.Errorf("could not create data file %q: %v", uri, err)
	}
	defer func() {
		if err != nil {
			return
		}
		if err = f.Close(); err != nil {
			err = fmt.Errorf("could not close file %q: %v", uri, err)
		}
	}()

	size, err = io.Copy(f, bytes.NewReader(data))
	if err != nil {
		return "", 0, fmt.Errorf("could not write data to file %q: %v", uri, err)
	}

	if err := f.Sync(); err != nil {
		return "", 0, fmt.Errorf("could not flush file %q: %v", uri, err)
	}

	log.Printf("DEBUG put: dgst %s, uri %s, size %d\n", dgst, uri, size)

	return dgst, size, nil
}

func (fs *fsBlobStore) blobDescriptor(data []byte) (dgst string, uri string, err error) {
	h := sha1.New()
	if _, err := h.Write(data); err != nil {
		return "", "", err
	}
	dgst = hex.EncodeToString(h.Sum(nil))
	uri = fs.resolvePath(dgst)
	return dgst, uri, nil
}

func (fs *fsBlobStore) resolvePath(dgst string) string {
	var group = "0000"
	if len(dgst) > 4 {
		group = dgst[:4]
	}
	return filepath.Join(fs.dataRoot, group[:2], group[2:], dgst)
}
