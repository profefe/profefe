package middleware

import "net/http"

type recoveryHandler struct {
	handler http.Handler
	log     func(_ ...interface{}) // TODO(narqo) add logging
}

func RecoveryHandler(next http.Handler) http.Handler {
	return &recoveryHandler{handler: next}
}

func (h *recoveryHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			h.log(err)
		}
	}()

	h.handler.ServeHTTP(w, req)
}
