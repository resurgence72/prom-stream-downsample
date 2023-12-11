package prometheus

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"prom-stream-downsample/pkg/pb"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/sirupsen/logrus"
)

func (p *Prometheus) remoteReadV1(
	span *pb.DurationSpan,
	interval time.Duration,
	matchers ...pb.Matcher,
) ([]pb.TimeSeries, int64, error) {
	var labelMatchers []*prompb.LabelMatcher
	for _, matcher := range matchers {
		labelMatchers = append(labelMatchers, &prompb.LabelMatcher{
			Type:  labelMatcherSet[matcher.Type],
			Name:  matcher.Name,
			Value: matcher.Value,
		})
	}
	end := time.Now()
	start := end.Add(-interval)

	// 创建一个请求
	req := &prompb.ReadRequest{
		AcceptedResponseTypes: []prompb.ReadRequest_ResponseType{prompb.ReadRequest_SAMPLES},
		Queries: []*prompb.Query{
			{
				StartTimestampMs: start.UnixMilli(), // 时间范围的开始
				EndTimestampMs:   end.UnixMilli(),   // 时间范围的结束
				Matchers:         labelMatchers,
			},
		},
	}

	if p.enabledStream {
		// append流式请求协议支持
		req.AcceptedResponseTypes = append(req.AcceptedResponseTypes, prompb.ReadRequest_STREAMED_XOR_CHUNKS)
	}

	// 将请求编码成Snappy压缩的protobuf格式
	data, err := req.Marshal()
	if err != nil {
		logrus.Error(err)
		return nil, 0, err
	}

	// 发送请求
	httpReq, err := http.NewRequestWithContext(context.Background(), "POST", p.remoteReadGroup[0], bytes.NewReader(snappy.Encode(nil, data)))
	if err != nil {
		logrus.Error(err)
		return nil, 0, err
	}
	httpReq.Header.Set("Content-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-stream-protobuf")
	httpReq.Header.Set("X-Prometheus-Remote-Read-Version", "0.1.0")

	queryStart := time.Now()
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		logrus.Error(err)
		return nil, 0, err
	}
	defer func() {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()
	// 记录查询耗时
	span.QueryDuration = time.Since(queryStart).Seconds()

	contentType := resp.Header.Get("Content-Type")
	if strings.HasPrefix(contentType, "application/x-protobuf") {
		logrus.Info("handleSampledPrometheusResponse")
		return p.sampleRemoteReadV1(resp)
	} else if strings.HasPrefix(contentType, "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse") {
		logrus.Info("handleStreamedPrometheusResponse")
		return p.streamRemoteReadV1(resp)
	}
	return nil, 0, fmt.Errorf("unknown response type: %s", contentType)
}

func (p *Prometheus) sampleRemoteReadV1(resp *http.Response) ([]pb.TimeSeries, int64, error) {
	bs, err := io.ReadAll(resp.Body)
	if err != nil {
		logrus.Error(err)
		return nil, 0, err
	}

	dst := make([]byte, initialBufSize)
	decomp, err := snappy.Decode(dst, bs)
	if err != nil {
		logrus.Error(err)
		return nil, 0, err
	}

	var data prompb.ReadResponse
	err = proto.Unmarshal(decomp, &data)
	if err != nil {
		logrus.Error(err)
		return nil, 0, err
	}

	var (
		tsSet     []pb.TimeSeries
		sampleCnt int64
	)
	for _, ts := range data.Results[0].Timeseries {
		if len(ts.Samples) == 0 {
			continue
		}

		var timeseries pb.TimeSeries
		for _, label := range ts.GetLabels() {
			timeseries.Labels = append(timeseries.Labels, pb.Label{
				Name:  label.GetName(),
				Value: label.GetValue(),
			})
		}

		sampleCnt += int64(len(ts.GetSamples()))
		for _, sample := range ts.GetSamples() {
			timeseries.Points = append(timeseries.Points, pb.Point{
				Timestamp: sample.GetTimestamp(),
				Value:     sample.GetValue(),
			})
		}

		tsSet = append(tsSet, timeseries)
	}
	return tsSet, sampleCnt, nil
}

func (p *Prometheus) streamRemoteReadV1(resp *http.Response) ([]pb.TimeSeries, int64, error) {
	// 解码响应
	bs := make([]byte, initialBufSize)
	stream := remote.NewChunkedReader(resp.Body, remote.DefaultChunkedReadLimit, bs)

	var (
		tsSet     []pb.TimeSeries
		sampleCnt int64
	)
	for {
		frame := &prompb.ChunkedReadResponse{}
		err := stream.NextProto(frame)
		if err == io.EOF {
			break
		}
		if err != nil {
			logrus.Error(err)
			return nil, 0, err
		}

		for _, ts := range frame.GetChunkedSeries() {
			var timeseries pb.TimeSeries
			for _, label := range ts.GetLabels() {
				timeseries.Labels = append(timeseries.Labels, pb.Label{
					Name:  label.GetName(),
					Value: label.GetValue(),
				})
			}

			for _, chunk := range ts.GetChunks() {
				if chunk.Type != prompb.Chunk_XOR {
					continue
				}
				c, err := chunkenc.FromData(chunkenc.EncXOR, chunk.Data)
				if err != nil {
					logrus.Error(err)
					return nil, 0, err
				}
				it := c.Iterator(nil)
				for it.Next().ChunkEncoding() != chunkenc.EncNone {
					t, v := it.At()
					timeseries.Points = append(timeseries.Points, pb.Point{
						Timestamp: t,
						Value:     v,
					})
					sampleCnt++
				}
			}

			tsSet = append(tsSet, timeseries)
		}
	}
	return tsSet, sampleCnt, nil
}
