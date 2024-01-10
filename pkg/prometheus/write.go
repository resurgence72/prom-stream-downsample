package prometheus

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"runtime"
	"sync"
	"time"

	"prom-stream-downsample/pkg/pb"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/sirupsen/logrus"
)

func (p *Prometheus) putBuffer(buf []prompb.TimeSeries) {
	buf = buf[:0]
	pb.TimeSeriesPool.Put(buf)
}

func (p *Prometheus) getMaxConcurrent() int {
	c := runtime.GOMAXPROCS(0)
	if c < 2 {
		c = 4
	} else if c > 8 {
		c = 8
	}
	return c
}

func (p *Prometheus) StartRemoteWrite(ctx context.Context) {
	ticker := time.NewTicker(time.Second * 5)

	var wg sync.WaitGroup
	wg.Add(p.getMaxConcurrent())
	for i := 0; i < p.getMaxConcurrent(); i++ {
		go func() {
			defer wg.Done()

			for batch := range p.writeCh {
				select {
				case <-ctx.Done():
					return
				default:
				}

				if len(batch) > 0 {
					p.send(batch)
					p.putBuffer(batch)
				}
			}
		}()
	}

	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			wg.Wait()
			return
		case <-ticker.C:
			select {
			case batch := <-p.writeCh:
				if len(batch) > 0 {
					p.writeCh <- batch
				}
			default:
			}
		case batch := <-p.writeCh:
			if len(batch) > 0 {
				p.writeCh <- batch
			}
		}
	}
}

func (p *Prometheus) send(batch []prompb.TimeSeries) {
	marshal, err := proto.Marshal(&prompb.WriteRequest{Timeseries: batch})
	if err != nil {
		logrus.Errorln("send series proto marshal failed", err)
		return
	}

	httpReq, err := http.NewRequest("POST", p.remoteWriteURL, bytes.NewReader(snappy.Encode(nil, marshal)))
	if err != nil {
		return
	}

	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("User-Agent", "prom-remote-write-shard")
	httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		logrus.Errorln("api do failed", err)
		return
	}
	defer func() {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode >= 400 {
		all, _ := io.ReadAll(resp.Body)
		logrus.Errorln("api do status code >= 400", resp.StatusCode, string(all))
		return
	}

	logrus.Warnln("remote write series success", len(batch))
	return
}
