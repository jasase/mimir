// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/cortexpb/compat.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package mimirpb

import (
	stdjson "encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/textparse"
	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/util"
)

// ToWriteRequest converts matched slices of Labels, Samples, Exemplars, and Metadata into a WriteRequest
// proto. It gets timeseries from the pool, so ReuseSlice() should be called when done. Note that this
// method implies that only a single sample and optionally exemplar can be set for each series.
func ToWriteRequest(lbls []labels.Labels, samples []Sample, exemplars []*Exemplar, metadata []*MetricMetadata, source WriteRequest_SourceEnum) *WriteRequest {
	req := &WriteRequest{
		Timeseries: PreallocTimeseriesSliceFromPool(),
		Metadata:   metadata,
		Source:     source,
	}

	for i, s := range samples {
		ts := TimeseriesFromPool()
		ts.Labels = append(ts.Labels, FromLabelsToLabelAdapters(lbls[i])...)
		ts.Samples = append(ts.Samples, s)

		if exemplars != nil {
			// If provided, we expect a matched entry for exemplars (like labels and samples) but the
			// entry may be nil since not every timeseries is guaranteed to have an exemplar.
			if e := exemplars[i]; e != nil {
				ts.Exemplars = append(ts.Exemplars, *e)
			}
		}

		req.Timeseries = append(req.Timeseries, PreallocTimeseries{TimeSeries: ts})
	}

	return req
}

// FromLabelAdaptersToLabels casts []LabelAdapter to labels.Labels.
// For now it's doing an expensive conversion: TODO figure out a faster way, maybe via PreallocTimeSeries.
func FromLabelAdaptersToLabels(ls []LabelAdapter) labels.Labels {
	builder := labels.SimpleBuilder{}
	for _, v := range ls {
		builder.Add(v.Name, v.Value)
	}
	return builder.Labels()
}

// FromLabelAdaptersToLabelsWithCopy converts []LabelAdapter to labels.Labels.
// Do NOT use unsafe to convert between data types because this function may
// get in input labels whose data structure is reused.
func FromLabelAdaptersToLabelsWithCopy(input []LabelAdapter) labels.Labels {
	return FromLabelAdaptersToLabels(input).Copy()
}

// FromLabelsToLabelAdapters casts labels.Labels to []LabelAdapter.
// It uses unsafe, but as LabelAdapter == labels.Label this should be safe.
// This allows us to use labels.Labels directly in protos.
func FromLabelsToLabelAdapters(ls labels.Labels) []LabelAdapter {
	return *(*[]LabelAdapter)(unsafe.Pointer(&ls))
}

// FromLabelAdaptersToMetric converts []LabelAdapter to a model.Metric.
// Don't do this on any performance sensitive paths.
func FromLabelAdaptersToMetric(ls []LabelAdapter) model.Metric {
	return util.LabelsToMetric(FromLabelAdaptersToLabels(ls))
}

// FromMetricsToLabelAdapters converts model.Metric to []LabelAdapter.
// Don't do this on any performance sensitive paths.
// The result is sorted.
func FromMetricsToLabelAdapters(metric model.Metric) []LabelAdapter {
	result := make([]LabelAdapter, 0, len(metric))
	for k, v := range metric {
		result = append(result, LabelAdapter{
			Name:  string(k),
			Value: string(v),
		})
	}
	sort.Sort(byLabel(result)) // The labels should be sorted upon initialisation.
	return result
}

func FromExemplarsToExemplarProtos(es []exemplar.Exemplar) []Exemplar {
	result := make([]Exemplar, 0, len(es))
	for _, e := range es {
		result = append(result, Exemplar{
			Labels:      FromLabelsToLabelAdapters(e.Labels),
			Value:       e.Value,
			TimestampMs: e.Ts,
		})
	}
	return result
}

func FromExemplarProtosToExemplars(es []Exemplar) []exemplar.Exemplar {
	result := make([]exemplar.Exemplar, 0, len(es))
	for _, e := range es {
		result = append(result, exemplar.Exemplar{
			Labels: FromLabelAdaptersToLabels(e.Labels),
			Value:  e.Value,
			Ts:     e.TimestampMs,
		})
	}
	return result
}

// FromPointsToSamples casts []promql.Point to []Sample. It uses unsafe.
func FromPointsToSamples(points []promql.Point) []Sample {
	return *(*[]Sample)(unsafe.Pointer(&points))
}

type byLabel []LabelAdapter

func (s byLabel) Len() int           { return len(s) }
func (s byLabel) Less(i, j int) bool { return s[i].Name < s[j].Name }
func (s byLabel) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// MetricMetadataMetricTypeToMetricType converts a metric type from our internal client
// to a Prometheus one.
func MetricMetadataMetricTypeToMetricType(mt MetricMetadata_MetricType) textparse.MetricType {
	switch mt {
	case UNKNOWN:
		return textparse.MetricTypeUnknown
	case COUNTER:
		return textparse.MetricTypeCounter
	case GAUGE:
		return textparse.MetricTypeGauge
	case HISTOGRAM:
		return textparse.MetricTypeHistogram
	case GAUGEHISTOGRAM:
		return textparse.MetricTypeGaugeHistogram
	case SUMMARY:
		return textparse.MetricTypeSummary
	case INFO:
		return textparse.MetricTypeInfo
	case STATESET:
		return textparse.MetricTypeStateset
	default:
		return textparse.MetricTypeUnknown
	}
}

// isTesting is only set from tests to get special behaviour to verify that custom sample encode and decode is used,
// both when using jsonitor or standard json package.
var isTesting = false

// MarshalJSON implements json.Marshaler.
func (s Sample) MarshalJSON() ([]byte, error) {
	if isTesting && math.IsNaN(s.Value) {
		return nil, fmt.Errorf("test sample")
	}

	t, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(model.Time(s.TimestampMs))
	if err != nil {
		return nil, err
	}
	v, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(model.SampleValue(s.Value))
	if err != nil {
		return nil, err
	}
	return []byte(fmt.Sprintf("[%s,%s]", t, v)), nil
}

// UnmarshalJSON implements json.Unmarshaler.
func (s *Sample) UnmarshalJSON(b []byte) error {
	var t model.Time
	var v model.SampleValue
	vs := [...]stdjson.Unmarshaler{&t, &v}
	if err := jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal(b, &vs); err != nil {
		return err
	}
	s.TimestampMs = int64(t)
	s.Value = float64(v)

	if isTesting && math.IsNaN(float64(v)) {
		return fmt.Errorf("test sample")
	}
	return nil
}

func SampleJsoniterEncode(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	sample := (*Sample)(ptr)

	if isTesting && math.IsNaN(sample.Value) {
		stream.Error = fmt.Errorf("test sample")
		return
	}

	stream.WriteArrayStart()
	marshalTimestamp(sample.TimestampMs, stream)
	stream.WriteMore()
	marshalValue(sample.Value, stream)
	stream.WriteArrayEnd()
}

func SampleJsoniterDecode(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	if !iter.ReadArray() {
		iter.ReportError("mimirpb.Sample", "expected [")
		return
	}

	t := model.Time(iter.ReadFloat64() * float64(time.Second/time.Millisecond))

	if !iter.ReadArray() {
		iter.ReportError("mimirpb.Sample", "expected ,")
		return
	}

	bs := iter.ReadStringAsSlice()
	ss := *(*string)(unsafe.Pointer(&bs))
	v, err := strconv.ParseFloat(ss, 64)
	if err != nil {
		iter.ReportError("mimirpb.Sample", err.Error())
		return
	}

	if isTesting && math.IsNaN(v) {
		iter.Error = fmt.Errorf("test sample")
		return
	}

	if iter.ReadArray() {
		iter.ReportError("mimirpb.Sample", "expected ]")
	}

	*(*Sample)(ptr) = Sample{
		TimestampMs: int64(t),
		Value:       v,
	}
}

func marshalTimestamp(t int64, stm *jsoniter.Stream) {
	// Write out the timestamp as a float divided by 1000.
	// This is ~3x faster than converting to a float.
	if t < 0 {
		stm.WriteRaw(`-`)
		t = -t
	}
	stm.WriteInt64(t / 1000)
	fraction := t % 1000
	if fraction != 0 {
		stm.WriteRaw(`.`)
		if fraction < 100 {
			stm.WriteRaw(`0`)
		}
		if fraction < 10 {
			stm.WriteRaw(`0`)
		}
		stm.WriteInt64(fraction)
	}
}

func marshalValue(v float64, stm *jsoniter.Stream) {
	stm.WriteRaw(`"`)
	// Taken from https://github.com/json-iterator/go/blob/master/stream_float.go#L71 as a workaround
	// to https://github.com/json-iterator/go/issues/365 (jsoniter, to follow json standard, doesn't allow inf/nan).
	buf := stm.Buffer()
	abs := math.Abs(v)
	format := byte('f')
	// Note: Must use float32 comparisons for underlying float32 value to get precise cutoffs right.
	if abs != 0 {
		if abs < 1e-6 || abs >= 1e21 {
			format = 'e'
		}
	}
	buf = strconv.AppendFloat(buf, v, format, -1, 64)
	stm.SetBuffer(buf)
	stm.WriteRaw(`"`)
}

func init() {
	jsoniter.RegisterTypeEncoderFunc("mimirpb.Sample", SampleJsoniterEncode, func(unsafe.Pointer) bool { return false })
	jsoniter.RegisterTypeDecoderFunc("mimirpb.Sample", SampleJsoniterDecode)
}
