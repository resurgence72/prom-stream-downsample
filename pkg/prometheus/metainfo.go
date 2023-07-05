package prometheus

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
)

type PrometheusMetaInfo struct {
	Version            string
	QueryLookBackDelta time.Duration
}

func NewPrometheusMetaInfo(addr string) (*PrometheusMetaInfo, error) {
	client, err := api.NewClient(api.Config{Address: addr})
	if err != nil {
		logrus.Errorln("create prometheus client error: ", err)
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*5)
	defer cancel()

	api := v1.NewAPI(client)
	buildinfo, err := api.Buildinfo(ctx)
	if err != nil {
		logrus.Errorln("get prometheus buildinfo error: ", err)
		return nil, err
	}

	flags, err := api.Flags(ctx)
	if err != nil {
		logrus.Errorln("get prometheus flags error: ", err)
		return nil, err
	}

	duration, _ := time.ParseDuration(flags["query.lookback-delta"])
	p := &PrometheusMetaInfo{
		Version:            buildinfo.Version,
		QueryLookBackDelta: duration,
	}
	return p, nil
}
