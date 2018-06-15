package middleware

import (
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

const headerRequestID = "X-Request-Id"

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (r *responseWriter) WriteHeader(statusCode int) {
	r.ResponseWriter.WriteHeader(statusCode)
	r.statusCode = statusCode
}

func LoggingHandler(out io.Writer, next http.Handler) http.Handler {
	h := func(w http.ResponseWriter, r *http.Request) {
		ts := time.Now().UTC()

		resp := &responseWriter{w, http.StatusOK}

		rid := r.Header.Get(headerRequestID)
		if rid == "" {
			rid = newRequestID()
		}

		next.ServeHTTP(resp, r)

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
			"rid=%s ts=%s method=%s uri=%s status=%v host=%s ip=%s rtime=%s\n",
			rid,
			ts.Format(time.RFC3339),
			r.Method,
			r.RequestURI,
			resp.statusCode,
			host,
			remoteAddr,
			rtime,
		)
	}
	return http.HandlerFunc(h)
}

func newRequestID() string {
	return fmt.Sprintf("%08x%08x", rand.Uint32(), rand.Uint32())
}
