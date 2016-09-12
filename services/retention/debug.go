// +build debug

package retention // import "github.com/influxdata/influxdb/services/retention"

import (
	"net/http"

	"github.com/bmizerany/pat"
	"github.com/influxdata/influxdb/debug"
)

var _ debug.DebugService = &Service{}

// AddDebugHandler adds an endpoint to immediately enforce the retention policies.
func (s *Service) AddDebugHandler(pmux *pat.PatternServeMux) {
	pmux.Post("/debug/enforce_retention", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.forceDeleteShardGroups <- struct{}{}
		s.forceDeleteShards <- struct{}{}
	}))
}
