package prometheus

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

func TestRemoteRead(t *testing.T) {
	//p := Prometheus{
	//	remoteReadGroup: "http://172.18.12.38:9090/api/v1/read",
	//	enabledStream: false,
	//}
	//
	//date, cnt, err := p.RemoteRead(&pb.DurationSpan{}, 1*time.Minute, pb.Matcher{
	//	Name:  "__name__",
	//	Type:  "=~",
	//	Value: "up",
	//})
	//
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//t.Log(cnt, date)

	u, err := url.Parse("http://172.18.12.38:9090/api/v1/read")
	if err != nil {
		t.Fatal(err)
	}

	rc, err := remote.NewReadClient("read-0", &remote.ClientConfig{
		URL:     &config.URL{u},
		Timeout: model.Duration(10 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}

	queryables := make([]storage.SampleAndChunkQueryable, 0)
	queryables = append(queryables, remote.NewSampleAndChunkQueryableClient(
		rc,
		nil,
		nil,
		true,
		nil,
	))

	ctx := context.Background()
	maxt := time.Now()
	mint := maxt.Add(-1 * time.Minute)

	// sample query
	queriers := make([]storage.Querier, 0, len(queryables))
	for _, queryable := range queryables {
		q, err := queryable.Querier(ctx, mint.UnixMilli(), maxt.UnixMilli())
		if err != nil {
			t.Fatal(err)
		}
		queriers = append(queriers, q)
	}

	ss := storage.NewMergeQuerier(nil, queriers, storage.ChainedSeriesMerge).Select(
		false,
		nil,
		labels.MustNewMatcher(labels.MatchEqual, "__name__", "up"),
		labels.MustNewMatcher(labels.MatchEqual, "pod", "prometheus-server-97cfb8984-4vljm"),
	)

	for ss.Next() {
		series := ss.At()
		lbs := series.Labels()
		it := series.Iterator(nil)
		for it.Next() == chunkenc.ValFloat {
			ts, val := it.At()
			fmt.Printf("%s %g %d\n", lbs, val, ts)
		}
		if it.Err() != nil {
			t.Fatal(err)
		}
	}

	// stream query
	//queriers := make([]storage.ChunkQuerier, 0, len(queryables))
	//for _, queryable := range queryables {
	//	q, err := queryable.ChunkQuerier(ctx, mint.UnixMilli(), maxt.UnixMilli())
	//	if err != nil {
	//		t.Fatal(err)
	//	}
	//	queriers = append(queriers, q)
	//}
	//ss := storage.NewMergeChunkQuerier(nil, queriers, storage.NewConcatenatingChunkSeriesMerger()).Select(
	//	false,
	//	nil,
	//	labels.MustNewMatcher(labels.MatchEqual, "__name__", "up"),
	//	labels.MustNewMatcher(labels.MatchEqual, "pod", "prometheus-server-97cfb8984-4vljm"),
	//)
	//
	//for ss.Next() {
	//	series := ss.At()
	//	lbs := series.Labels()
	//	it := series.Iterator(nil)
	//
	//	for it.Next() {
	//		chk := it.At().Chunk
	//
	//		if chk.Encoding().String() != "XOR" {
	//			return
	//		} else {
	//			it := chk.Iterator(nil)
	//			for it.Next() != chunkenc.ValNone {
	//				ts, val := it.At()
	//				fmt.Printf("%s %g %d\n", lbs, val, ts)
	//			}
	//		}
	//
	//	}
	//
	//	if it.Err() != nil {
	//		t.Fatal(err)
	//	}
	//}
}
