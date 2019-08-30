package profefe

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"github.com/profefe/profefe/pkg/log"
	"golang.org/x/xerrors"
)

var ErrNotFound = StatusError(http.StatusNotFound, "not found", nil)

func ReplyOK(w http.ResponseWriter) {
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")

	io.WriteString(w, `{"code":200}`)
}

func ReplyJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")

	err := json.NewEncoder(w).Encode(v)
	if err != nil {
		io.WriteString(w, `{"code":`+strconv.Itoa(http.StatusInternalServerError)+`,"error":"`+err.Error()+`"}`)
	}
}

func ReplyError(w http.ResponseWriter, err error) {
	var (
		statusErr  *statusError
		statusCode int
		errMsg     string
	)

	if xerrors.As(err, &statusErr) {
		statusCode = statusErr.code
		errMsg = statusErr.Error()
	} else {
		statusCode = http.StatusInternalServerError
		errMsg = "internal error"
	}

	w.WriteHeader(statusCode)

	resp := struct {
		Code  int    `json:"code"`
		Error string `json:"error"`
	}{
		Code:  statusCode,
		Error: errMsg,
	}
	ReplyJSON(w, resp)
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
	code    int
	message string
	cause   error
}

func StatusError(code int, msg string, cause error) *statusError {
	return &statusError{
		code:    code,
		message: msg,
		cause:   cause,
	}
}

func (s statusError) Error() string {
	return s.message
}

func (s statusError) Unwrap() error {
	return s.cause
}
