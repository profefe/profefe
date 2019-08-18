package profefe

import (
	"net/http"

	"github.com/profefe/profefe/pkg/log"
)

func NewServicesHandler(logger *log.Logger, querier *Querier) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		services, err := querier.GetServices(r.Context())
		if err != nil {
			HandleErrorHTTP(logger, err, w, r)
			return
		}
		ReplyJSON(w, services)
	}
}
