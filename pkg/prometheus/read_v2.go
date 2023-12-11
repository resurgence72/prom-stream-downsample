package prometheus

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"prom-stream-downsample/pkg/pb"
)

func (p *Prometheus) remoteReadV2(
	span *pb.DurationSpan,
	interval time.Duration,
	matchers ...pb.Matcher,
) ([]pb.TimeSeries, int64, error) {
	end := time.Now()
	start := end.Add(-interval)

	var mtcs []*labels.Matcher
	for _, matcher := range matchers {
		var matchType labels.MatchType
		switch matcher.Type {
		case pb.LabelMatcher_EQ:
			matchType = labels.MatchEqual
		case pb.LabelMatcher_NEQ:
			matchType = labels.MatchNotEqual
		case pb.LabelMatcher_RE:
			matchType = labels.MatchRegexp
		case pb.LabelMatcher_NRE:
			matchType = labels.MatchNotRegexp
		}

		mtcs = append(mtcs,
			labels.MustNewMatcher(
				matchType,
				matcher.Name,
				matcher.Value,
			))
	}

	if p.enabledStream {
		return p.streamRemoteReadV2(span, start, end, mtcs)
		//return p.sampleRemoteReadV2(span, start, end, mtcs)
	} else {
		return p.sampleRemoteReadV2(span, start, end, mtcs)
	}
}

func (p *Prometheus) streamRemoteReadV2(span *pb.DurationSpan, start time.Time, end time.Time, mtcs []*labels.Matcher) ([]pb.TimeSeries, int64, error) {
	ctx, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
	defer cancel()

	queriers := make([]storage.ChunkQuerier, 0, len(p.queryables))
	for _, queryable := range p.queryables {
		q, err := queryable.ChunkQuerier(ctx, start.UnixMilli(), end.UnixMilli())
		if err != nil {
			return nil, 0, err
		}
		queriers = append(queriers, q)
	}

	ss := storage.NewMergeChunkQuerier(nil, queriers, storage.NewConcatenatingChunkSeriesMerger()).Select(
		false,
		nil,
		mtcs...,
	)

	var (
		tsSet     []pb.TimeSeries
		sampleCnt int64
	)

	for ss.Next() {
		series := ss.At()
		lbs := series.Labels()
		it := series.Iterator(nil)

		var timeseries pb.TimeSeries
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

			itt := chk.Iterator(nil)
			for itt.Next() != chunkenc.ValNone {
				ts, val := itt.At()
				timeseries.Points = append(timeseries.Points, pb.Point{
					Timestamp: ts,
					Value:     val,
				})

				sampleCnt++
			}
		}

		if it.Err() != nil {
			return nil, 0, it.Err()
		}

		tsSet = append(tsSet, timeseries)
	}

	return tsSet, sampleCnt, nil
}

func (p *Prometheus) sampleRemoteReadV2(span *pb.DurationSpan, start, end time.Time, matchers []*labels.Matcher) ([]pb.TimeSeries, int64, error) {
	ctx, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
	defer cancel()

	queriers := make([]storage.Querier, 0, len(p.queryables))
	for _, queryable := range p.queryables {
		q, err := queryable.Querier(ctx, start.UnixMilli(), end.UnixMilli())
		if err != nil {
			return nil, 0, err
		}
		queriers = append(queriers, q)
	}

	ss := storage.NewMergeQuerier(nil, queriers, storage.ChainedSeriesMerge).Select(
		false,
		nil,
		matchers...,
	)

	var (
		tsSet     []pb.TimeSeries
		sampleCnt int64
	)

	for ss.Next() {
		series := ss.At()
		lbs := series.Labels()
		it := series.Iterator(nil)

		var timeseries pb.TimeSeries
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

			sampleCnt++
		}

		if it.Err() != nil {
			return nil, 0, it.Err()
		}

		tsSet = append(tsSet, timeseries)
	}

	return tsSet, sampleCnt, nil
}
