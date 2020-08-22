package profefe

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplyJSON(t *testing.T) {
	w := httptest.NewRecorder()
	body := map[string]interface{}{"foo": "bar"}

	ReplyJSON(w, body)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("content-type"))

	var resp map[string]interface{}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))

	assert.EqualValues(t, http.StatusOK, resp["code"])
	assert.Equal(t, body, resp["body"])
}

// malformedJSON always returns an error when marshaling itself into JSON
type malformedJSON string

func (t malformedJSON) MarshalJSON() ([]byte, error) {
	return nil, errors.New(`unexpected """, '"', and {}`)
}

func TestReplyJSON_FailMarshalResponse(t *testing.T) {
	w := httptest.NewRecorder()
	body := malformedJSON("foo")

	ReplyJSON(w, body)

	var resp map[string]interface{}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))

	assert.EqualValues(t, http.StatusInternalServerError, resp["code"])
	assert.NotEmpty(t, resp["error"])
}

func TestReplyError(t *testing.T) {
	testRawErr := errors.New("unexpected error")

	cases := []struct {
		err       error
		wantCode  int
		wantError string
	}{
		{
			StatusError(http.StatusBadRequest, "bad request", nil),
			http.StatusBadRequest,
			"bad request",
		},
		{
			StatusError(http.StatusBadRequest, "bad request", testRawErr),
			http.StatusBadRequest,
			"bad request",
		},
		{
			fmt.Errorf("unexpected error: %w", StatusError(http.StatusBadRequest, "bad request", nil)),
			http.StatusBadRequest,
			"bad request",
		},
		{
			fmt.Errorf("unexpected error: %w", testRawErr),
			http.StatusInternalServerError,
			"internal server error",
		},
		{
			fmt.Errorf("unexpted error: %v", testRawErr),
			http.StatusInternalServerError,
			"internal server error",
		},
		{
			ErrNoResults,
			http.StatusNoContent,
			"no results",
		},
		{
			ErrNotFound,
			http.StatusNotFound,
			"nothing found",
		},
		{
			nil,
			http.StatusInternalServerError,
			"internal server error",
		},
	}

	for n, tc := range cases {
		t.Run(fmt.Sprintf("case=%d", n), func(t *testing.T) {
			w := httptest.NewRecorder()
			ReplyError(w, tc.err)

			require.Equal(t, tc.wantCode, w.Code)
			assert.Equal(t, "application/json", w.Header().Get("content-type"))

			var resp map[string]interface{}
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))

			assert.EqualValues(t, tc.wantCode, resp["code"])
			assert.Equal(t, tc.wantError, resp["error"])
		})
	}
}
