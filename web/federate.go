// Copyright 2015 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package web

import (
	"fmt"
	"net/http"
	"sort"

	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

var (
	federationErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_web_federation_errors_total",
		Help: "Total number of errors that occurred while sending federation responses.",
	})
	federationWarnings = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_web_federation_warnings_total",
		Help: "Total number of warnings that occurred while sending federation responses.",
	})
)

func registerFederationMetrics(r prometheus.Registerer) {
	r.MustRegister(federationWarnings, federationErrors)
}

func (h *Handler) federation(w http.ResponseWriter, req *http.Request) {
	h.mtx.RLock()
	defer h.mtx.RUnlock()

	if err := req.ParseForm(); err != nil {
		http.Error(w, fmt.Sprintf("error parsing form values: %v", err), http.StatusBadRequest)
		return
	}

	var matcherSets [][]*labels.Matcher
	for _, s := range req.Form["match[]"] {
		matchers, err := parser.ParseMetricSelector(s)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		matcherSets = append(matcherSets, matchers)
	}

	var (
		mint   = timestamp.FromTime(h.now().Time().Add(-h.lookbackDelta))
		maxt   = timestamp.FromTime(h.now().Time())
		format = expfmt.Negotiate(req.Header)
		enc    = expfmt.NewEncoder(w, format)
	)
	w.Header().Set("Content-Type", string(format))

	q, err := h.localStorage.Querier(req.Context(), mint, maxt)
	if err != nil {
		federationErrors.Inc()
		if errors.Cause(err) == tsdb.ErrNotReady {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer q.Close()

	vec := make(promql.Vector, 0, 8000)

	hints := &storage.SelectHints{Start: mint, End: maxt}

	var sets []storage.SeriesSet
	for _, mset := range matcherSets {
		s := q.Select(true, hints, mset...)
		sets = append(sets, s)
	}

	set := storage.NewMergeSeriesSet(sets, storage.ChainedSeriesMerge)
	it := storage.NewBuffer(int64(h.lookbackDelta / 1e6))
	var chkIter chunkenc.Iterator
	for set.Next() {
		s := set.At()

		// TODO(fabxc): allow fast path for most recent sample either
		// in the storage itself or caching layer in Prometheus.
		chkIter = s.Iterator(chkIter)
		it.Reset(chkIter)

		var t int64
		var v float64
		var ok bool

		valueType := it.Seek(maxt)
		if valueType == chunkenc.ValFloat {
			t, v = it.At()
		} else {
			// TODO(beorn7): Handle histograms.
			t, v, _, ok = it.PeekBack(1)
			if !ok {
				continue
			}
		}
		// The exposition formats do not support stale markers, so drop them. This
		// is good enough for staleness handling of federated data, as the
		// interval-based limits on staleness will do the right thing for supported
		// use cases (which is to say federating aggregated time series).
		if value.IsStaleNaN(v) {
			continue
		}

		vec = append(vec, promql.Sample{
			Metric: s.Labels(),
			Point:  promql.Point{T: t, V: v},
		})
	}
	if ws := set.Warnings(); len(ws) > 0 {
		level.Debug(h.logger).Log("msg", "Federation select returned warnings", "warnings", ws)
		federationWarnings.Add(float64(len(ws)))
	}
	if set.Err() != nil {
		federationErrors.Inc()
		http.Error(w, set.Err().Error(), http.StatusInternalServerError)
		return
	}

	sort.Sort(byName(vec))

	externalLabels := h.config.GlobalConfig.ExternalLabels.Map()
	if _, ok := externalLabels[model.InstanceLabel]; !ok {
		externalLabels[model.InstanceLabel] = ""
	}
	externalLabelNames := make([]string, 0, len(externalLabels))
	for ln := range externalLabels {
		externalLabelNames = append(externalLabelNames, ln)
	}
	sort.Strings(externalLabelNames)

	var (
		lastMetricName string
		protMetricFam  *dto.MetricFamily
	)
	for _, s := range vec {
		nameSeen := false
		globalUsed := map[string]struct{}{}
		protMetric := &dto.Metric{
			Untyped: &dto.Untyped{},
		}

		err := s.Metric.Validate(func(l labels.Label) error {
			if l.Value == "" {
				// No value means unset. Never consider those labels.
				// This is also important to protect against nameless metrics.
				return nil
			}
			if l.Name == labels.MetricName {
				nameSeen = true
				if l.Value == lastMetricName {
					// We already have the name in the current MetricFamily,
					// and we ignore nameless metrics.
					return nil
				}
				// Need to start a new MetricFamily. Ship off the old one (if any) before
				// creating the new one.
				if protMetricFam != nil {
					if err := enc.Encode(protMetricFam); err != nil {
						return err
					}
				}
				protMetricFam = &dto.MetricFamily{
					Type: dto.MetricType_UNTYPED.Enum(),
					Name: proto.String(l.Value),
				}
				lastMetricName = l.Value
				return nil
			}
			protMetric.Label = append(protMetric.Label, &dto.LabelPair{
				Name:  proto.String(l.Name),
				Value: proto.String(l.Value),
			})
			if _, ok := externalLabels[l.Name]; ok {
				globalUsed[l.Name] = struct{}{}
			}
			return nil
		})
		if err != nil {
			federationErrors.Inc()
			level.Error(h.logger).Log("msg", "federation failed", "err", err)
			return
		}
		if !nameSeen {
			level.Warn(h.logger).Log("msg", "Ignoring nameless metric during federation", "metric", s.Metric)
			continue
		}
		// Attach global labels if they do not exist yet.
		for _, ln := range externalLabelNames {
			lv := externalLabels[ln]
			if _, ok := globalUsed[ln]; !ok {
				protMetric.Label = append(protMetric.Label, &dto.LabelPair{
					Name:  proto.String(ln),
					Value: proto.String(lv),
				})
			}
		}

		protMetric.TimestampMs = proto.Int64(s.T)
		protMetric.Untyped.Value = proto.Float64(s.V)
		// TODO(beorn7): Handle histograms.

		protMetricFam.Metric = append(protMetricFam.Metric, protMetric)
	}
	// Still have to ship off the last MetricFamily, if any.
	if protMetricFam != nil {
		if err := enc.Encode(protMetricFam); err != nil {
			federationErrors.Inc()
			level.Error(h.logger).Log("msg", "federation failed", "err", err)
		}
	}
}

// byName makes a model.Vector sortable by metric name.
type byName promql.Vector

func (vec byName) Len() int      { return len(vec) }
func (vec byName) Swap(i, j int) { vec[i], vec[j] = vec[j], vec[i] }

func (vec byName) Less(i, j int) bool {
	ni := vec[i].Metric.Get(labels.MetricName)
	nj := vec[j].Metric.Get(labels.MetricName)
	return ni < nj
}
