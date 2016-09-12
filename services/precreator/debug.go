// +build debug

package precreator // import "github.com/influxdata/influxdb/services/precreator"

import (
	"net/http"

	"github.com/bmizerany/pat"
	"github.com/influxdata/influxdb/debug"
)

var _ debug.DebugService = &Service{}

// AddDebugHandler adds an endpoint to immediately precreate any necessary shards.
func (s *Service) AddDebugHandler(pmux *pat.PatternServeMux) {
	pmux.Post("/debug/precreate", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.forcePrecreate <- struct{}{}
	}))
}
