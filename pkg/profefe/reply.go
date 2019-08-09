package profefe

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"github.com/profefe/profefe/pkg/log"
	"golang.org/x/xerrors"
)

func ReplyOK(w http.ResponseWriter) {
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")

	io.WriteString(w, `{"code":200}`)
}

func ReplyJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")

	json.NewEncoder(w).Encode(v)
}

func ReplyError(w http.ResponseWriter, err error) {
	var (
		statusCode int
		errMsg     string
	)

	switch err := err.(type) {
	case *statusError:
		statusCode = err.code
		errMsg = err.Error()
	default:
		statusCode = http.StatusInternalServerError
		errMsg = "internal error"
	}

	w.WriteHeader(statusCode)
	w.Header().Set("Content-Type", "application/json")

	resp := struct {
		Code  int    `json:"code"`
		Error string `json:"error"`
	}{
		Code:  statusCode,
		Error: errMsg,
	}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		io.WriteString(w, `{"code":`+strconv.Itoa(http.StatusInternalServerError)+`,"error":"`+err.Error()+`"}`)
	}
}

func HandleErrorHTTP(logger *log.Logger, err error, w http.ResponseWriter, r *http.Request) {
	if err == nil {
		return
	}

	ReplyError(w, err)

	if origErr := xerrors.Unwrap(err); origErr != nil {
		err = origErr
	}
	if err != nil {
		logger.Errorw("request failed", "url", r.URL.String(), "err", err)
	}
}

type statusError struct {
	code   int
	status string
	cause  error
}

func (s *statusError) Error() string {
	return s.status
}

func (s *statusError) Unwrap() error {
	return s.cause
}

func StatusError(code int, msg string, cause error) *statusError {
	return &statusError{
		code:   code,
		status: msg,
		cause:  cause,
	}
}
