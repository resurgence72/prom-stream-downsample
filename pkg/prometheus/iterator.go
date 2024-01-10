package prometheus

import (
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/sirupsen/logrus"

	"prom-stream-downsample/pkg/pb"
)

type Iterator interface {
	Next() bool
	At() pb.TimeSeries
}

type StreamIterator struct {
	css     storage.ChunkSeriesSet
	samples int64
}

func (s *StreamIterator) Next() bool {
	return s.css.Next()
}

func (s *StreamIterator) At() pb.TimeSeries {
	series := s.css.At()
	lbs := series.Labels()

	var it chunks.Iterator
	it = series.Iterator(it)

	timeseries := pb.TimeSeries{Labels: make([]pb.Label, 0, len(lbs))}
	for _, lb := range lbs {
		timeseries.Labels = append(timeseries.Labels, pb.Label{
			Name:  lb.Name,
			Value: lb.Value,
		})
	}

	for it.Next() {
		chk := it.At().Chunk
		if chk.Encoding().String() != "XOR" {
			break
		}

		var itt chunkenc.Iterator
		itt = chk.Iterator(itt)
		for itt.Next() != chunkenc.ValNone {
			ts, val := itt.At()
			timeseries.Points = append(timeseries.Points, pb.Point{
				Timestamp: ts,
				Value:     val,
			})

			s.samples++
		}
	}
	if it.Err() != nil {
		logrus.Errorln("StreamIterator.At error", it.Err())
		return pb.TimeSeries{}
	}

	return timeseries
}

type SampleIterator struct {
	ss      storage.SeriesSet
	samples int64
}

func (s *SampleIterator) Next() bool {
	return s.ss.Next()
}

func (s *SampleIterator) At() pb.TimeSeries {
	series := s.ss.At()
	lbs := series.Labels()

	var it chunkenc.Iterator
	it = series.Iterator(it)

	timeseries := pb.TimeSeries{Labels: make([]pb.Label, 0, len(lbs))}
	for _, lb := range lbs {
		timeseries.Labels = append(timeseries.Labels, pb.Label{
			Name:  lb.Name,
			Value: lb.Value,
		})
	}

	for it.Next() == chunkenc.ValFloat {
		ts, val := it.At()
		timeseries.Points = append(timeseries.Points, pb.Point{
			Timestamp: ts,
			Value:     val,
		})

		s.samples++
	}

	if it.Err() != nil {
		logrus.Errorln("StreamIterator.At error", it.Err())
		return pb.TimeSeries{}
	}

	return timeseries
}
