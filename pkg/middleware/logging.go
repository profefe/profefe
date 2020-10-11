package middleware

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

const headerRequestID = "X-Request-Id"

func LoggingHandler(out io.Writer, handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ts := time.Now().UTC()

		resp := &responseWriter{w, http.StatusOK}

		id := r.Header.Get(headerRequestID)
		if id == "" {
			id = nextRequestID()
			r = r.WithContext(ContextWithRequestID(r.Context(), id))
			r.Header.Set(headerRequestID, id)
		}

		handler.ServeHTTP(resp, r)

		rtime := time.Now().UTC().Sub(ts)

		host, _, err := net.SplitHostPort(r.Host)
		if err != nil {
			host = r.Host
		}

		remoteAddr, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			remoteAddr = r.RemoteAddr
		}

		fmt.Fprintf(
			out,
			"rid=%s ts=%s method=%s uri=%s code=%v host=%s ip=%s rtime=%s\n",
			id,
			ts.Format(time.RFC3339),
			r.Method,
			r.RequestURI,
			resp.statusCode,
			host,
			remoteAddr,
			rtime,
		)
	})
}

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (r *responseWriter) WriteHeader(statusCode int) {
	r.ResponseWriter.WriteHeader(statusCode)
	r.statusCode = statusCode
}
