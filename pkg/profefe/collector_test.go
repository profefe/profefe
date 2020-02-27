package profefe

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/profefe/profefe/pkg/log"
	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestCollector_WriteProfile(t *testing.T) {
	pprofData, err := ioutil.ReadFile("../../testdata/collector_cpu_1.prof")
	require.NoError(t, err)

	traceData, err := ioutil.ReadFile("../../testdata/collector_trace_1.out")
	require.NoError(t, err)

	cases := []struct {
		params *storage.WriteProfileParams
		data   []byte
	}{
		{
			&storage.WriteProfileParams{
				Service: "service1",
				Type:    profile.TypeCPU,
			},
			pprofData,
		},
		{

			&storage.WriteProfileParams{
				Service: "service1",
				Type:    profile.TypeTrace,
			},
			traceData,
		},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("type=%s", tc.params.Type), func(t *testing.T) {
			sw := &storage.StubWriter{
				WriteProfileFunc: func(ctx context.Context, params *storage.WriteProfileParams, r io.Reader) (profile.Meta, error) {
					assert.False(t, params.CreatedAt.IsZero(), "params.CreatedAt must be set")

					data, err := ioutil.ReadAll(r)
					require.NoError(t, err)
					require.Equal(t, tc.data, data)

					meta := profile.Meta{
						ProfileID: profile.TestID,
						Service:   params.Service,
						Type:      params.Type,
					}
					return meta, nil
				},
			}

			testLogger := log.New(zaptest.NewLogger(t))
			collector := NewCollector(testLogger, sw)

			profModel, err := collector.WriteProfile(context.Background(), tc.params, bytes.NewReader(tc.data))
			require.NoError(t, err)

			assert.Equal(t, profile.TestID, profModel.ProfileID)
			assert.Equal(t, tc.params.Service, profModel.Service)
			assert.Equal(t, tc.params.Type.String(), profModel.Type)
		})
	}
}

func TestCollector_WriteProfile_MalformedPprofData(t *testing.T) {
	sw := &storage.StubWriter{}
	testLogger := log.New(zaptest.NewLogger(t))
	collector := NewCollector(testLogger, sw)

	params := &storage.WriteProfileParams{
		Service: "service1",
		Type:    profile.TypeCPU,
	}
	_, err := collector.WriteProfile(context.Background(), params, strings.NewReader("not a pprof"))
	require.Error(t, err)
}
