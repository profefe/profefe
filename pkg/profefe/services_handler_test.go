package profefe

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/profefe/profefe/pkg/log"
	"github.com/profefe/profefe/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"golang.org/x/xerrors"
)

func TestServicesHandler_success(t *testing.T) {
	services := []string{"service1", "service2"}
	sr := &storage.StubReader{
		ListServicesFunc: func(ctx context.Context) ([]string, error) {
			return services, nil
		},
	}

	testLogger := log.New(zaptest.NewLogger(t))
	h := NewServicesHandler(testLogger, NewQuerier(testLogger, sr))

	rec := httptest.NewRecorder()
	req, err := http.NewRequest(http.MethodGet, "/api/0/services", nil)
	require.NoError(t, err)

	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

	var resp jsonResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Empty(t, resp.Error)
	assert.ElementsMatch(t, services, resp.Body.([]interface{}))
}

func TestServicesHandler_nothingFound(t *testing.T) {
	sr := &storage.StubReader{
		ListServicesFunc: func(ctx context.Context) ([]string, error) {
			return nil, storage.ErrNotFound
		},
	}

	testLogger := log.New(zaptest.NewLogger(t))
	h := NewServicesHandler(testLogger, NewQuerier(testLogger, sr))

	rec := httptest.NewRecorder()
	req, err := http.NewRequest(http.MethodGet, "/api/0/services", nil)
	require.NoError(t, err)

	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNotFound, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
}

func TestServicesHandler_storageFailure(t *testing.T) {
	sr := &storage.StubReader{
		ListServicesFunc: func(ctx context.Context) ([]string, error) {
			return nil, xerrors.New("unexpected storage error")
		},
	}

	testLogger := log.New(zaptest.NewLogger(t))
	h := NewServicesHandler(testLogger, NewQuerier(testLogger, sr))

	rec := httptest.NewRecorder()
	req, err := http.NewRequest(http.MethodGet, "/api/0/services", nil)
	require.NoError(t, err)

	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
}
