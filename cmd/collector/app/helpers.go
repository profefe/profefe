package app

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"

	"github.com/profefe/profefe/pkg/profile"
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

	io.WriteString(w, `{"code":"200"}`)
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

	if origErr, _ := err.(causer); origErr != nil {
		err = origErr.Cause()
	}
	log.Printf("request failed: %v\n", err)
}

func readLabels(s string) (labels profile.Labels, err error) {
	var chunk string
	for s != "" {
		chunk, s = split2(s, ',')
		key, val := split2(chunk, '=')

		key, err = url.QueryUnescape(key)
		if err != nil {
			return nil, err
		}
		val, err = url.QueryUnescape(val)
		if err != nil {
			return nil, err
		}
		labels = append(labels, profile.Label{key, val})
	}

	sort.Sort(labels)

	return labels, nil
}

func split2(s string, ch byte) (s1, s2 string) {
	for i := 0; i < len(s); i++ {
		if s[i] == ch {
			return s[:i], s[i+1:]
		}
	}
	return s, ""
}
