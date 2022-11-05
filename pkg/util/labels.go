// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/labels.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package util

import (
	"strings"

	"github.com/cespare/xxhash"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
)

// LabelsToMetric converts a Labels to Metric
// Don't do this on any performance sensitive paths.
func LabelsToMetric(ls labels.Labels) model.Metric {
	m := make(model.Metric, ls.Len())
	ls.Range(func(l labels.Label) {
		m[model.LabelName(l.Name)] = model.LabelValue(l.Value)
	})
	return m
}

// LabelMatchersToString returns a string representing the input label matchers.
func LabelMatchersToString(matchers []*labels.Matcher) string {
	out := strings.Builder{}
	out.WriteRune('{')

	for idx, m := range matchers {
		if idx > 0 {
			out.WriteRune(',')
		}

		out.WriteString(m.String())
	}

	out.WriteRune('}')
	return out.String()
}

// replicate historical hash computation from Prometheus circa 2020
func LabelsXXHash(b []byte, ls labels.Labels) (uint64, []byte) {
	b = b[:0]
	ls.Range(func(l labels.Label) {
		b = append(b, l.Name...)
		b = append(b, '\xff')
		b = append(b, l.Value...)
		b = append(b, '\xff')
	})
	return xxhash.Sum64(b), b
}
