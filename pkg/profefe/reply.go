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

type jsonResponse struct {
	Code  int         `json:"code"`
	Body  interface{} `json:"body,omitempty"`
	Error string      `json:"error,omitempty"`
}

func ReplyOK(w http.ResponseWriter) {
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")

	io.WriteString(w, `{"code":200}`)
}

func ReplyJSON(w http.ResponseWriter, v interface{}) {
	w.WriteHeader(http.StatusOK)

	resp := jsonResponse{
		Code: http.StatusOK,
		Body: v,
	}
	replyJSON(w, resp)
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

	resp := jsonResponse{
		Code:  statusCode,
		Error: errMsg,
	}
	replyJSON(w, resp)
}

func replyJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")

	err := json.NewEncoder(w).Encode(v)
	if err != nil {
		// TODO(narqo): escape quotes in the failure response JSON message
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
