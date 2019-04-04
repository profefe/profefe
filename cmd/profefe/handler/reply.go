package handler

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
)

type causer interface {
	Cause() error
}

type statusError struct {
	code   int
	status error
	cause  error
}

func (s *statusError) Error() string {
	return s.status.Error()
}

func (s *statusError) Cause() error {
	return s.cause
}

func StatusError(code int, msg string, cause error) *statusError {
	return &statusError{
		code:   code,
		status: errors.New(msg),
		cause:  cause,
	}
}

func ReplyOK(w http.ResponseWriter) {
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")

	io.WriteString(w, `{"code":200}`)
}

func ReplyError(w http.ResponseWriter, err error) {
	var statusCode int

	switch err := err.(type) {
	case *statusError:
		statusCode = err.code
	default:
		statusCode = http.StatusInternalServerError
	}

	w.WriteHeader(statusCode)
	w.Header().Set("Content-Type", "application/json")

	resp := struct {
		Code  int    `json:"code"`
		Error string `json:"error"`
	}{
		Code:  statusCode,
		Error: err.Error(),
	}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		io.WriteString(w, `{"code":`+strconv.Itoa(http.StatusInternalServerError)+`,"error":"`+err.Error()+`"}`)
	}
}
