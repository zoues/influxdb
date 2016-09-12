package debug // import "github.com/influxdata/influxdb/debug"

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/pprof"
	"sort"

	"github.com/bmizerany/pat"
	"github.com/influxdata/influxdb/monitor"
)

// Ensure Mux implements the http.Handler interface.
var _ http.Handler = &Mux{}

// Monitor is the dependency to gather statistics for the /debug/vars endpoint.
type Monitor interface {
	Statistics(tags map[string]string) ([]*monitor.Statistic, error)
}

// DebugService provides a method to attach extra debug handlers to the underlying debug mux.
// Most of the time, a service that defines AddDebugHandler will do so in a separate file
// that depends on the debug build tag being set.
type DebugService interface {
	AddDebugHandler(pmux *pat.PatternServeMux)
}

// Mux contains all the handlers for any /debug endpoint.
type Mux struct {
	pmux    *pat.PatternServeMux
	monitor Monitor
}

// NewMux returns a new Mux that serves stats from the given Monitor.
func NewMux(m Monitor) *Mux {
	pmux := pat.New()
	mux := &Mux{
		pmux:    pmux,
		monitor: m,
	}

	// TODO: add pprof enabled flag?
	pmux.Get("/debug/pprof", http.HandlerFunc(pprof.Index))
	pmux.Get("/debug/pprof/:type", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		typ := r.URL.Query().Get(":type")
		switch typ {
		case "cmdline":
			pprof.Cmdline(w, r)
		case "profile":
			pprof.Profile(w, r)
		case "symbol":
			pprof.Symbol(w, r)
		default:
			pprof.Index(w, r)
		}
	}))
	pmux.Get("/debug/vars", http.HandlerFunc(mux.serveExpvar))

	return mux
}

// ServeHTTP implements the http.Handler interface.
func (m *Mux) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.pmux.ServeHTTP(w, r)
}

// AddDebugService adds the debug handler for the given DebugService.
func (m *Mux) AddDebugService(ds DebugService) {
	ds.AddDebugHandler(m.pmux)
}

// ServeError serves a JSON error.
func ServeError(w http.ResponseWriter, errMsg string, code int) {
	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(code)

	var response struct {
		Err string `json:"error"`
	}
	response.Err = errMsg
	b, _ := json.Marshal(response)
	w.Write(b)
}

func (m *Mux) serveExpvar(w http.ResponseWriter, r *http.Request) {
	// Retrieve statistics from the monitor.
	stats, err := m.monitor.Statistics(nil)
	if err != nil {
		ServeError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	statMap := make(map[string]*monitor.Statistic)
	for _, s := range stats {
		// Very hackily create a unique key.
		buf := bytes.NewBufferString(s.Name)
		if path, ok := s.Tags["path"]; ok {
			fmt.Fprintf(buf, ":%s", path)
			if id, ok := s.Tags["id"]; ok {
				fmt.Fprintf(buf, ":%s", id)
			}
		} else if bind, ok := s.Tags["bind"]; ok {
			if proto, ok := s.Tags["proto"]; ok {
				fmt.Fprintf(buf, ":%s", proto)
			}
			fmt.Fprintf(buf, ":%s", bind)
		} else if database, ok := s.Tags["database"]; ok {
			fmt.Fprintf(buf, ":%s", database)
			if rp, ok := s.Tags["retention_policy"]; ok {
				fmt.Fprintf(buf, ":%s", rp)
				if name, ok := s.Tags["name"]; ok {
					fmt.Fprintf(buf, ":%s", name)
				}
				if dest, ok := s.Tags["destination"]; ok {
					fmt.Fprintf(buf, ":%s", dest)
				}
			}
		}
		key := buf.String()

		statMap[key] = s
	}

	// Sort the keys to simulate /debug/vars output.
	keys := make([]string, 0, len(statMap))
	for k := range statMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	fmt.Fprintln(w, "{")
	for idx, key := range keys {
		// Marshal this statistic to JSON.
		out, err := json.Marshal(statMap[key])
		if err != nil {
			continue
		}

		if idx != 0 {
			fmt.Fprintln(w, ",")
		}
		fmt.Fprintf(w, "%q: ", key)
		w.Write(bytes.TrimSpace(out))
	}
	fmt.Fprintln(w, "\n}")
}
