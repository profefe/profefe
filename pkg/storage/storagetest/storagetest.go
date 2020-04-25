package storagetest

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/storage"
	"github.com/stretchr/testify/require"
)

func WriteProfile(t *testing.T, sw storage.Writer, params *storage.WriteProfileParams, fileName string) (profile.Meta, []byte) {
	data, err := ioutil.ReadFile(fileName)
	require.NoError(t, err)

	meta, err := sw.WriteProfile(context.Background(), params, bytes.NewReader(data))
	require.NoError(t, err)
	require.NotEmpty(t, meta.ProfileID)
	require.Equal(t, params.Service, meta.Service)
	require.Equal(t, params.Type, meta.Type)
	require.False(t, meta.CreatedAt.IsZero())

	return meta, data
}

func genServiceName() string {
	return fmt.Sprintf("test-service-%x", time.Now().Nanosecond())
}
