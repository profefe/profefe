package storage

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/profefe/profefe/pkg/profile"
	"github.com/stretchr/testify/require"
)

func TestMultiWriter_WriteProfile(t *testing.T) {
	data := "the data"

	sw := &StubWriter{
		WriteProfileFunc: func(ctx context.Context, _ *WriteProfileParams, r io.Reader) (profile.Meta, error) {
			d, _ := ioutil.ReadAll(r)
			require.Equal(t, data, string(d))
			return profile.Meta{}, nil
		},
	}

	mw := NewMultiWriter(sw, sw)
	_, err := mw.WriteProfile(context.Background(), &WriteProfileParams{}, strings.NewReader(data))
	require.NoError(t, err)
}

func TestMultiWriter_WriteProfile_firstError(t *testing.T) {
	theErr := errors.New("the error")

	sw1 := &StubWriter{
		WriteProfileFunc: func(ctx context.Context, _ *WriteProfileParams, _ io.Reader) (profile.Meta, error) {
			return profile.Meta{}, theErr
		},
	}
	sw2 := &StubWriter{
		WriteProfileFunc: func(ctx context.Context, _ *WriteProfileParams, _ io.Reader) (profile.Meta, error) {
			return profile.Meta{}, nil
		},
	}

	mw := NewMultiWriter(sw1, sw2)
	_, err := mw.WriteProfile(context.Background(), &WriteProfileParams{}, strings.NewReader("test data"))
	require.Equal(t, theErr, err)
}
