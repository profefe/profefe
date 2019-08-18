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
	"golang.org/x/xerrors"
)

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
			xerrors.Errorf("unexpted error: %w", StatusError(http.StatusBadRequest, "bad request", nil)),
			http.StatusBadRequest,
			"bad request",
		},
		{
			xerrors.Errorf("unexpted error: %w", testRawErr),
			http.StatusInternalServerError,
			"internal error",
		},
		{
			fmt.Errorf("unexpted error: %v", testRawErr),
			http.StatusInternalServerError,
			"internal error",
		},
		{
			nil,
			http.StatusInternalServerError,
			"internal error",
		},
	}

	for n, tc := range cases {
		t.Run(fmt.Sprintf("case %d", n), func(t *testing.T) {
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
