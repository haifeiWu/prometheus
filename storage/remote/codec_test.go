// Copyright 2017 The Prometheus Authors
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

package remote

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/textparse"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

var testHistogram = histogram.Histogram{
	Schema:          2,
	ZeroThreshold:   1e-128,
	ZeroCount:       0,
	Count:           0,
	Sum:             20,
	PositiveSpans:   []histogram.Span{{Offset: 0, Length: 1}},
	PositiveBuckets: []int64{1},
	NegativeSpans:   []histogram.Span{{Offset: 0, Length: 1}},
	NegativeBuckets: []int64{-1},
}

var writeRequestFixture = &prompb.WriteRequest{
	Timeseries: []prompb.TimeSeries{
		{
			Labels: []prompb.Label{
				{Name: "__name__", Value: "test_metric1"},
				{Name: "b", Value: "c"},
				{Name: "baz", Value: "qux"},
				{Name: "d", Value: "e"},
				{Name: "foo", Value: "bar"},
			},
			Samples:    []prompb.Sample{{Value: 1, Timestamp: 0}},
			Exemplars:  []prompb.Exemplar{{Labels: []prompb.Label{{Name: "f", Value: "g"}}, Value: 1, Timestamp: 0}},
			Histograms: []prompb.Histogram{HistogramToHistogramProto(0, &testHistogram)},
		},
		{
			Labels: []prompb.Label{
				{Name: "__name__", Value: "test_metric1"},
				{Name: "b", Value: "c"},
				{Name: "baz", Value: "qux"},
				{Name: "d", Value: "e"},
				{Name: "foo", Value: "bar"},
			},
			Samples:    []prompb.Sample{{Value: 2, Timestamp: 1}},
			Exemplars:  []prompb.Exemplar{{Labels: []prompb.Label{{Name: "h", Value: "i"}}, Value: 2, Timestamp: 1}},
			Histograms: []prompb.Histogram{HistogramToHistogramProto(1, &testHistogram)},
		},
	},
}

func TestValidateLabelsAndMetricName(t *testing.T) {
	tests := []struct {
		input       []prompb.Label
		expectedErr string
		description string
	}{
		{
			input: []prompb.Label{
				{Name: "__name__", Value: "name"},
				{Name: "labelName", Value: "labelValue"},
			},
			expectedErr: "",
			description: "regular labels",
		},
		{
			input: []prompb.Label{
				{Name: "__name__", Value: "name"},
				{Name: "_labelName", Value: "labelValue"},
			},
			expectedErr: "",
			description: "label name with _",
		},
		{
			input: []prompb.Label{
				{Name: "__name__", Value: "name"},
				{Name: "@labelName", Value: "labelValue"},
			},
			expectedErr: "invalid label name: @labelName",
			description: "label name with @",
		},
		{
			input: []prompb.Label{
				{Name: "__name__", Value: "name"},
				{Name: "123labelName", Value: "labelValue"},
			},
			expectedErr: "invalid label name: 123labelName",
			description: "label name starts with numbers",
		},
		{
			input: []prompb.Label{
				{Name: "__name__", Value: "name"},
				{Name: "", Value: "labelValue"},
			},
			expectedErr: "invalid label name: ",
			description: "label name is empty string",
		},
		{
			input: []prompb.Label{
				{Name: "__name__", Value: "name"},
				{Name: "labelName", Value: string([]byte{0xff})},
			},
			expectedErr: "invalid label value: " + string([]byte{0xff}),
			description: "label value is an invalid UTF-8 value",
		},
		{
			input: []prompb.Label{
				{Name: "__name__", Value: "@invalid_name"},
			},
			expectedErr: "invalid metric name: @invalid_name",
			description: "metric name starts with @",
		},
		{
			input: []prompb.Label{
				{Name: "__name__", Value: "name1"},
				{Name: "__name__", Value: "name2"},
			},
			expectedErr: "duplicate label with name: __name__",
			description: "duplicate label names",
		},
		{
			input: []prompb.Label{
				{Name: "label1", Value: "name"},
				{Name: "label2", Value: "name"},
			},
			expectedErr: "",
			description: "duplicate label values",
		},
		{
			input: []prompb.Label{
				{Name: "", Value: "name"},
				{Name: "label2", Value: "name"},
			},
			expectedErr: "invalid label name: ",
			description: "don't report as duplicate label name",
		},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			err := validateLabelsAndMetricName(test.input)
			if test.expectedErr != "" {
				require.Error(t, err)
				require.Equal(t, test.expectedErr, err.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestConcreteSeriesSet(t *testing.T) {
	series1 := &concreteSeries{
		labels:  labels.FromStrings("foo", "bar"),
		samples: []prompb.Sample{{Value: 1, Timestamp: 2}},
	}
	series2 := &concreteSeries{
		labels:  labels.FromStrings("foo", "baz"),
		samples: []prompb.Sample{{Value: 3, Timestamp: 4}},
	}
	c := &concreteSeriesSet{
		series: []storage.Series{series1, series2},
	}
	require.True(t, c.Next(), "Expected Next() to be true.")
	require.Equal(t, series1, c.At(), "Unexpected series returned.")
	require.True(t, c.Next(), "Expected Next() to be true.")
	require.Equal(t, series2, c.At(), "Unexpected series returned.")
	require.False(t, c.Next(), "Expected Next() to be false.")
}

func TestConcreteSeriesClonesLabels(t *testing.T) {
	lbls := labels.FromStrings("a", "b", "c", "d")
	cs := concreteSeries{
		labels: lbls,
	}

	gotLabels := cs.Labels()
	require.Equal(t, lbls, gotLabels)

	gotLabels.CopyFrom(labels.FromStrings("a", "foo", "c", "foo"))

	gotLabels = cs.Labels()
	require.Equal(t, lbls, gotLabels)
}

func TestConcreteSeriesIterator(t *testing.T) {
	series := &concreteSeries{
		labels: labels.FromStrings("foo", "bar"),
		samples: []prompb.Sample{
			{Value: 1, Timestamp: 1},
			{Value: 1.5, Timestamp: 1},
			{Value: 2, Timestamp: 2},
			{Value: 3, Timestamp: 3},
			{Value: 4, Timestamp: 4},
		},
	}
	it := series.Iterator(nil)

	// Seek to the first sample with ts=1.
	require.Equal(t, chunkenc.ValFloat, it.Seek(1))
	ts, v := it.At()
	require.Equal(t, int64(1), ts)
	require.Equal(t, 1., v)

	// Seek one further, next sample still has ts=1.
	require.Equal(t, chunkenc.ValFloat, it.Next())
	ts, v = it.At()
	require.Equal(t, int64(1), ts)
	require.Equal(t, 1.5, v)

	// Seek again to 1 and make sure we stay where we are.
	require.Equal(t, chunkenc.ValFloat, it.Seek(1))
	ts, v = it.At()
	require.Equal(t, int64(1), ts)
	require.Equal(t, 1.5, v)

	// Another seek.
	require.Equal(t, chunkenc.ValFloat, it.Seek(3))
	ts, v = it.At()
	require.Equal(t, int64(3), ts)
	require.Equal(t, 3., v)

	// And we don't go back.
	require.Equal(t, chunkenc.ValFloat, it.Seek(2))
	ts, v = it.At()
	require.Equal(t, int64(3), ts)
	require.Equal(t, 3., v)

	// Seek beyond the end.
	require.Equal(t, chunkenc.ValNone, it.Seek(5))
	// And we don't go back. (This exposes issue #10027.)
	require.Equal(t, chunkenc.ValNone, it.Seek(2))
}

func TestFromQueryResultWithDuplicates(t *testing.T) {
	ts1 := prompb.TimeSeries{
		Labels: []prompb.Label{
			{Name: "foo", Value: "bar"},
			{Name: "foo", Value: "def"},
		},
		Samples: []prompb.Sample{
			{Value: 0.0, Timestamp: 0},
		},
	}

	res := prompb.QueryResult{
		Timeseries: []*prompb.TimeSeries{
			&ts1,
		},
	}

	series := FromQueryResult(false, &res)

	errSeries, isErrSeriesSet := series.(errSeriesSet)

	require.True(t, isErrSeriesSet, "Expected resulting series to be an errSeriesSet")
	errMessage := errSeries.Err().Error()
	require.Equal(t, "duplicate label with name: foo", errMessage, fmt.Sprintf("Expected error to be from duplicate label, but got: %s", errMessage))
}

func TestNegotiateResponseType(t *testing.T) {
	r, err := NegotiateResponseType([]prompb.ReadRequest_ResponseType{
		prompb.ReadRequest_STREAMED_XOR_CHUNKS,
		prompb.ReadRequest_SAMPLES,
	})
	require.NoError(t, err)
	require.Equal(t, prompb.ReadRequest_STREAMED_XOR_CHUNKS, r)

	r2, err := NegotiateResponseType([]prompb.ReadRequest_ResponseType{
		prompb.ReadRequest_SAMPLES,
		prompb.ReadRequest_STREAMED_XOR_CHUNKS,
	})
	require.NoError(t, err)
	require.Equal(t, prompb.ReadRequest_SAMPLES, r2)

	r3, err := NegotiateResponseType([]prompb.ReadRequest_ResponseType{})
	require.NoError(t, err)
	require.Equal(t, prompb.ReadRequest_SAMPLES, r3)

	_, err = NegotiateResponseType([]prompb.ReadRequest_ResponseType{20})
	require.Error(t, err, "expected error due to not supported requested response types")
	require.Equal(t, "server does not support any of the requested response types: [20]; supported: map[SAMPLES:{} STREAMED_XOR_CHUNKS:{}]", err.Error())
}

func TestMergeLabels(t *testing.T) {
	for _, tc := range []struct {
		primary, secondary, expected []prompb.Label
	}{
		{
			primary:   []prompb.Label{{Name: "aaa", Value: "foo"}, {Name: "bbb", Value: "foo"}, {Name: "ddd", Value: "foo"}},
			secondary: []prompb.Label{{Name: "bbb", Value: "bar"}, {Name: "ccc", Value: "bar"}},
			expected:  []prompb.Label{{Name: "aaa", Value: "foo"}, {Name: "bbb", Value: "foo"}, {Name: "ccc", Value: "bar"}, {Name: "ddd", Value: "foo"}},
		},
		{
			primary:   []prompb.Label{{Name: "bbb", Value: "bar"}, {Name: "ccc", Value: "bar"}},
			secondary: []prompb.Label{{Name: "aaa", Value: "foo"}, {Name: "bbb", Value: "foo"}, {Name: "ddd", Value: "foo"}},
			expected:  []prompb.Label{{Name: "aaa", Value: "foo"}, {Name: "bbb", Value: "bar"}, {Name: "ccc", Value: "bar"}, {Name: "ddd", Value: "foo"}},
		},
	} {
		require.Equal(t, tc.expected, MergeLabels(tc.primary, tc.secondary))
	}
}

func TestMetricTypeToMetricTypeProto(t *testing.T) {
	tc := []struct {
		desc     string
		input    textparse.MetricType
		expected prompb.MetricMetadata_MetricType
	}{
		{
			desc:     "with a single-word metric",
			input:    textparse.MetricTypeCounter,
			expected: prompb.MetricMetadata_COUNTER,
		},
		{
			desc:     "with a two-word metric",
			input:    textparse.MetricTypeStateset,
			expected: prompb.MetricMetadata_STATESET,
		},
		{
			desc:     "with an unknown metric",
			input:    "not-known",
			expected: prompb.MetricMetadata_UNKNOWN,
		},
	}

	for _, tt := range tc {
		t.Run(tt.desc, func(t *testing.T) {
			m := metricTypeToMetricTypeProto(tt.input)
			require.Equal(t, tt.expected, m)
		})
	}
}

func TestDecodeWriteRequest(t *testing.T) {
	buf, _, err := buildWriteRequest(writeRequestFixture.Timeseries, nil, nil, nil)
	require.NoError(t, err)

	actual, err := DecodeWriteRequest(bytes.NewReader(buf))
	require.NoError(t, err)
	require.Equal(t, writeRequestFixture, actual)
}

func TestNilHistogramProto(t *testing.T) {
	// This function will panic if it impromperly handles nil
	// values, causing the test to fail.
	HistogramProtoToHistogram(prompb.Histogram{})
}

func TestStreamResponse(t *testing.T) {
	lbs1 := labelsToLabelsProto(labels.FromStrings("instance", "localhost1", "job", "demo1"), nil)
	lbs2 := labelsToLabelsProto(labels.FromStrings("instance", "localhost2", "job", "demo2"), nil)
	chunk := prompb.Chunk{
		Type: prompb.Chunk_XOR,
		Data: make([]byte, 100),
	}
	lbSize, chunkSize := 0, chunk.Size()
	for _, lb := range lbs1 {
		lbSize += lb.Size()
	}
	maxBytesInFrame := lbSize + chunkSize*2
	testData := []*prompb.ChunkedSeries{{
		Labels: lbs1,
		Chunks: []prompb.Chunk{chunk, chunk, chunk, chunk},
	}, {
		Labels: lbs2,
		Chunks: []prompb.Chunk{chunk, chunk, chunk, chunk},
	}}
	css := newMockChunkSeriesSet(testData)
	writer := mockWriter{}
	warning, err := StreamChunkedReadResponses(&writer, 0,
		css,
		nil,
		maxBytesInFrame,
		&sync.Pool{})
	require.Nil(t, warning)
	require.Nil(t, err)
	expectData := []*prompb.ChunkedSeries{{
		Labels: lbs1,
		Chunks: []prompb.Chunk{chunk, chunk},
	}, {
		Labels: lbs1,
		Chunks: []prompb.Chunk{chunk, chunk},
	}, {
		Labels: lbs2,
		Chunks: []prompb.Chunk{chunk, chunk},
	}, {
		Labels: lbs2,
		Chunks: []prompb.Chunk{chunk, chunk},
	}}
	require.Equal(t, expectData, writer.actual)
}

type mockWriter struct {
	actual []*prompb.ChunkedSeries
}

func (m *mockWriter) Write(p []byte) (n int, err error) {
	cr := &prompb.ChunkedReadResponse{}
	if err := proto.Unmarshal(p, cr); err != nil {
		return 0, fmt.Errorf("unmarshaling: %w", err)
	}
	m.actual = append(m.actual, cr.ChunkedSeries...)
	return len(p), nil
}

type mockChunkSeriesSet struct {
	chunkedSeries []*prompb.ChunkedSeries
	index         int
}

func newMockChunkSeriesSet(ss []*prompb.ChunkedSeries) storage.ChunkSeriesSet {
	return &mockChunkSeriesSet{chunkedSeries: ss, index: -1}
}

func (c *mockChunkSeriesSet) Next() bool {
	c.index++
	return c.index < len(c.chunkedSeries)
}

func (c *mockChunkSeriesSet) At() storage.ChunkSeries {
	return &storage.ChunkSeriesEntry{
		Lset: labelProtosToLabels(c.chunkedSeries[c.index].Labels),
		ChunkIteratorFn: func(chunks.Iterator) chunks.Iterator {
			return &mockChunkIterator{
				chunks: c.chunkedSeries[c.index].Chunks,
				index:  -1,
			}
		},
	}
}

func (c *mockChunkSeriesSet) Warnings() storage.Warnings { return nil }

func (c *mockChunkSeriesSet) Err() error {
	return nil
}

type mockChunkIterator struct {
	chunks []prompb.Chunk
	index  int
}

func (c *mockChunkIterator) At() chunks.Meta {
	one := c.chunks[c.index]
	chunk, err := chunkenc.FromData(chunkenc.Encoding(one.Type), one.Data)
	if err != nil {
		panic(err)
	}
	return chunks.Meta{
		Chunk:   chunk,
		MinTime: one.MinTimeMs,
		MaxTime: one.MaxTimeMs,
	}
}

func (c *mockChunkIterator) Next() bool {
	c.index++
	return c.index < len(c.chunks)
}

func (c *mockChunkIterator) Err() error {
	return nil
}
