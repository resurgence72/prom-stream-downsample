package prometheus

import (
	"time"

	"prom-stream-downsample/pkg/pb"
)

func (p *Prometheus) RemoteRead(
	span *pb.DurationSpan,
	interval time.Duration,
	matchers ...pb.Matcher,
) (Iterator, error) {
	return p.remoteReadV2(span, interval, matchers...)
}
