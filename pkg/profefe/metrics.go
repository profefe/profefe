package profefe

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func metricsHandler(registry prometheus.Registerer, next http.Handler) http.Handler {
	var reqTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "profefe",
		Name:      "api_requests_total",
	}, []string{"method", "path", "code"})

	var reqDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "profefe",
		Name:      "api_request_duration_seconds",
	}, []string{"method", "path", "code"})

	var reqSize = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace: "profefe",
		Name:      "api_request_size_bytes",
	}, []string{"method", "path"})

	var respSize = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace: "profefe",
		Name:      "api_response_size_bytes",
	}, []string{"method", "path"})

	registry.MustRegister(
		reqTotal,
		reqDuration,
		reqSize,
		respSize,
	)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		respw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		next.ServeHTTP(respw, r)

		apiPath := fixAPIPathLabel(r.URL.Path)
		labels := []string{
			r.Method,
			apiPath,
			strconv.Itoa(respw.statusCode),
		}
		reqTotal.WithLabelValues(labels...).Inc()
		reqDuration.WithLabelValues(labels...).Observe(time.Since(start).Seconds())

		reqSize.WithLabelValues(r.Method, apiPath).Observe(float64(calcReqSize(r)))
		respSize.WithLabelValues(r.Method, apiPath).Observe(float64(respw.written))
	})
}

type responseWriter struct {
	http.ResponseWriter
	statusCode  int
	wroteHeader bool
	written     int64
}

func (r *responseWriter) WriteHeader(statusCode int) {
	r.ResponseWriter.WriteHeader(statusCode)
	r.statusCode = statusCode
	r.wroteHeader = true
}

func (r *responseWriter) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	n, err := r.ResponseWriter.Write(b)
	r.written += int64(n)
	return n, err
}

// refer to https://github.com/banzaicloud/go-gin-prometheus/blob/df9373ad532734d669123fdb23a78817b6de408a/middleware.go
func calcReqSize(r *http.Request) int {
	s := 0
	if r.URL != nil {
		s = len(r.URL.String())
	}

	s += len(r.Method)
	s += len(r.Proto)
	for name, values := range r.Header {
		s += len(name)
		for _, value := range values {
			s += len(value)
		}
	}
	s += len(r.Host)

	// N.B. r.Form and r.MultipartForm are assumed to be included in r.URL.

	if r.ContentLength != -1 {
		s += int(r.ContentLength)
	}
	return s
}

func fixAPIPathLabel(p string) string {
	p = strings.TrimSuffix(p, "/")
	// fix ID-based API path making it suitable to use in metrics' labels
	if strings.HasPrefix(p, apiProfilesPath) && p != apiProfilesMergePath {
		p = apiProfilesPath + "/__pid__"
	}
	return p
}
