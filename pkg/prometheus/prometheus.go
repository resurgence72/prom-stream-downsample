package prometheus

import (
	"net/url"

	"prom-stream-downsample/pkg/pb"

	"github.com/prometheus/prometheus/prompb"
	"golang.org/x/mod/semver"
)

const initialBufSize = 32 * 1024

type Prometheus struct {
	remoteReadURL  string
	remoteWriteURL string
	enabledStream  bool
	remoteReadType string

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
	rr string,
	rw string,
	enabled bool,
	writeCh chan []prompb.TimeSeries,
) (*Prometheus, error) {
	p8s := &Prometheus{
		remoteReadURL:  rr,
		remoteWriteURL: rw,
		enabledStream:  enabled,
		writeCh:        writeCh,
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
	return p8s, nil
}

func (p *Prometheus) versionSupportStreamRemoteRead() (bool, error) {
	u, _ := url.Parse(p.remoteReadURL)
	info, err := NewPrometheusMetaInfo(u.Scheme + "://" + u.Host + "/")
	if err != nil {
		return false, err
	}

	// 如果当前版本号大于2.13.0，则开启自动流式传输
	if semver.Compare(semver.Build(info.Version), "2.13.0") >= 0 {
		return true, nil
	}
	return false, nil
}
