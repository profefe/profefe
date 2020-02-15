package profefe

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var apiRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "profefe",
	Name:      "api_request_duration_seconds",
	Help:      "API request duration in seconds",
	Buckets:   prometheus.DefBuckets,
}, []string{"method", "path", "code"})

func metricsHandler(obs prometheus.ObserverVec, next http.Handler) http.Handler {
	prometheus.MustRegister(obs)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		respw := &responseWriter{w, http.StatusOK}

		defer func(start time.Time) {
			labels := prometheus.Labels{
				"method": r.Method,
				"path":   r.URL.Path,
				"code":   strconv.Itoa(respw.statusCode),
			}
			obs.With(labels).Observe(time.Since(start).Seconds())
		}(time.Now())

		next.ServeHTTP(respw, r)
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
