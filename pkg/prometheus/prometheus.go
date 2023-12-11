package prometheus

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"

	"prom-stream-downsample/pkg/pb"

	"github.com/prometheus/prometheus/prompb"
)

const initialBufSize = 32 * 1024

type Prometheus struct {
	remoteReadGroup []string
	remoteWriteURL  string
	enabledStream   bool
	remoteReadType  string

	queryables []storage.SampleAndChunkQueryable

	writeCh chan []prompb.TimeSeries
}

var labelMatcherSet = map[string]prompb.LabelMatcher_Type{
	"=":  prompb.LabelMatcher_EQ,
	"!=": prompb.LabelMatcher_NEQ,
	"=~": prompb.LabelMatcher_RE,
	"!~": prompb.LabelMatcher_NRE,
}

func (p Prometheus) RemoteReadType() string {
	return p.remoteReadType
}

func NewPrometheus(
	rrg []string,
	rw string,
	enabled bool,
	writeCh chan []prompb.TimeSeries,
) (*Prometheus, error) {
	p8s := &Prometheus{
		remoteReadGroup: rrg,
		remoteWriteURL:  rw,
		enabledStream:   enabled,
		writeCh:         writeCh,
	}
	supported, err := p8s.versionSupportStreamRemoteRead()
	if err != nil {
		return nil, err
	}

	// 如果当前指定的是 sample 传输，但版本支持流式传输，则强制开启流式传输功能
	if !p8s.enabledStream && supported {
		p8s.enabledStream = true
	}

	p8s.remoteReadType = pb.RemoteReadType[p8s.enabledStream]

	var queryables []storage.SampleAndChunkQueryable
	for i, rr := range rrg {
		u, _ := url.Parse(rr)
		rc, err := remote.NewReadClient(fmt.Sprintf("remote-%d", i), &remote.ClientConfig{
			URL:     &config.URL{u},
			Timeout: model.Duration(30 * time.Second),
		})
		if err != nil {
			return nil, err
		}

		queryables = append(queryables, remote.NewSampleAndChunkQueryableClient(
			rc,
			nil,
			nil,
			true,
			nil,
		))
	}

	p8s.queryables = queryables
	return p8s, nil
}

func (p *Prometheus) versionSupportStreamRemoteRead() (bool, error) {
	for _, rr := range p.remoteReadGroup {
		u, err := url.Parse(rr)
		if err != nil {
			return false, err
		}

		info, err := NewPrometheusMetaInfo(u.Scheme + "://" + u.Host + "/")
		if err != nil {
			return false, err
		}

		// 如果当前版本号大于2.13.0，则开启自动流式传输
		if strings.Compare(info.Version, "2.13.0") <= 0 {
			return false, nil
		}
	}

	return true, nil
}
