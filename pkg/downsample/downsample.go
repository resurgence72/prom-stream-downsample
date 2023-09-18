package downsample

import (
	"context"
	"fmt"
	"strings"
	"time"

	"prom-stream-downsample/pkg/config"
	"prom-stream-downsample/pkg/downsample/agg"
	"prom-stream-downsample/pkg/pb"
	"prom-stream-downsample/pkg/prometheus"
	"prom-stream-downsample/pkg/util"

	"github.com/prometheus/prometheus/prompb"
	"github.com/sirupsen/logrus"
)

type DownSampleMgr struct {
	DownSamples []*DownSample

	writeCh chan []prompb.TimeSeries
	ctx     context.Context
	quit    chan struct{}
}

func configMatcher2pbMatcher(ms []config.Matcher) []pb.Matcher {
	matchers := make([]pb.Matcher, 0, len(ms))
	for _, m := range ms {
		matchers = append(matchers, pb.Matcher{
			Type:  m.MatcherType,
			Name:  m.LabelName,
			Value: m.LabelValue,
		})
	}
	return matchers
}

func NewDownSampleMgr(ctx context.Context, ch chan []prompb.TimeSeries, p8s *prometheus.Prometheus, resolutions pb.Intervals) *DownSampleMgr {
	// 声明一个channel,用于控制downsample的退出
	quit := make(chan struct{})

	mgr := &DownSampleMgr{ctx: ctx, writeCh: ch, quit: quit}
	dss := config.Get().DownSampleConfig
	for _, ds := range dss {
		var aggs []agg.Agg
		for _, a := range ds.Aggregations {
			ag, err := agg.NewAgg(a)
			if err != nil {
				logrus.Errorln("new agg error:", err)
				continue
			}
			aggs = append(aggs, ag)
		}

		// matchers 中不支持填写包括 :downsample 的 label value
		// 希望只对row metric 做 downsample, 不对 downsample metric 做 downsample
		needIgnoreDownsample := false
		for _, match := range ds.Matchers {
			if strings.Contains(match.LabelValue, ":downsample") {
				needIgnoreDownsample = true
				break
			}
		}

		if needIgnoreDownsample {
			logrus.Warnln("label value contains :downsample, job will be ignored")
			continue
		}

		mgr.DownSamples = append(mgr.DownSamples, &DownSample{
			matchers:    configMatcher2pbMatcher(ds.Matchers),
			Aggs:        aggs,
			prometheus:  p8s,
			writeCh:     ch,
			resolutions: resolutions,
			buffer:      pb.TimeSeriesPool.Get().([]prompb.TimeSeries),
			quit:        quit,
			metricReuse: config.Get().GlobalConfig.EnabledMetricReuse,
		})
	}

	return mgr
}

func (dsm *DownSampleMgr) Start() {
	for _, ds := range dsm.DownSamples {
		go ds.Start(dsm.ctx)
	}
}

func (dsm *DownSampleMgr) Stop() {
	close(dsm.quit)
}

type DownSample struct {
	matchers []pb.Matcher

	prometheus *prometheus.Prometheus
	writeCh    chan []prompb.TimeSeries
	quit       chan struct{}
	buffer     []prompb.TimeSeries

	resolutions pb.Intervals
	Aggs        []agg.Agg

	metricReuse bool
}

func (ds *DownSample) Start(ctx context.Context) {
	// 每个 downsample 的每个 resolution 都起一个 goroutine
	for intervalIdx := range ds.resolutions {
		il := time.Duration(ds.resolutions[intervalIdx].IntervalValue)

		go util.Wait(ctx, il, func(idx int) func() {
			return func() { ds.downsample(idx) }
		}(intervalIdx))
	}
}

func (ds *DownSample) submit() {
	select {
	case <-ds.quit:
		return
	case ds.writeCh <- ds.buffer:
		ds.buffer = pb.TimeSeriesPool.Get().([]prompb.TimeSeries)
	default:
	}
}

func (ds *DownSample) append(ts prompb.TimeSeries) {
	ds.buffer = append(ds.buffer, ts)
	if len(ds.buffer) >= cap(ds.buffer) {
		ds.submit()
	}
}

func (ds *DownSample) downsample(idx int) {
	// 具体的downsample逻辑
	// 1. 根据downsample的配置，从prometheus中获取数据
	select {
	case <-ds.quit:
		return
	default:
	}

	interval := ds.resolutions[idx]
	now := time.Now().UnixMilli()
	span := &pb.DurationSpan{}

	/*
		四个case下不适用metric复用：
		1. metricReuse=false 时，说明未配置metric复用
		2. metricReuse=true 时，说明配置了metric复用，但是当前是第一次downsample，需要从prometheus中获取原始数据
		3. 当前的matcher是!=，且 name 为 __name__, 说明是不等于的匹配，需要从prometheus中获取原始数据
		4. 当前的matcher是!~，且 name 为 __name__,,说明是不匹配的匹配，需要从prometheus中获取原始数据

			case34: 凡是对__name__操作的取反都不能重用降采样数据；
					比如 {__name__!="abc"}->{__name__!="abc:downsample_xxx_xxx"},这样的替换并不能重用降采样的数据；
					因为降采样数据就是通过__name__标识的

		PS：case34已经在配置校验中过滤掉了，所以这里不需要再校验

			有意义的重用示例:
				1. {__name__=~"abc"}->{__name__=~"abc:downsample_xxx_xxx"}
				2. {app="game"} -> {app="game",__name__=~".+:downsample_xx_xx"}
	*/
	if !ds.metricReuse ||
		(ds.metricReuse && idx == 0) {
		// 如果不开启metric复用，那么只拉一次prometheus的原始数据，然后使用原始数据的agg算法进行downsample
		matchers := make([]pb.Matcher, 0, len(ds.matchers)+1)
		matchers = append(matchers, ds.matchers...)

		// 因为拉的是原始数据，所以需要在这一步排除所有的 downsample指标
		matchers = append(matchers, pb.Matcher{
			Name:  pb.MetricLabelName,
			Type:  pb.LabelMatcher_NRE,
			Value: ".+:downsample_.+",
		})

		data, sampleCnt, err := ds.prometheus.RemoteRead(
			span,
			time.Duration(interval.IntervalValue),
			matchers...,
		)
		if err != nil {
			logrus.WithError(err).Error("remote read error")
			return
		}
		if sampleCnt == 0 {
			return
		}

		// 指标打点
		ds.appendDot(float64(sampleCnt), now, interval.IntervalName, span, matchers...)
		for _, d := range data {
			select {
			case <-ds.quit:
				return
			default:
			}

			// 降采点的时间默认为原始点的中位
			ts := calculateTime(d)
			// 2. 根据downsample的 aggregations 配置，对数据进行聚合
			//var series []prompb.TimeSeries
			for _, agg := range ds.Aggs {
				if agg.Name() == "lttb" {
					ds.append(prompb.TimeSeries{
						Labels:  d.ToTimeSeriesPbLabel("", interval.IntervalName, agg.Name()),
						Samples: agg.Aggregate(d.Points).([]prompb.Sample),
					})
				} else {
					sample := prompb.Sample{
						Value:     agg.Aggregate(d.Points).(float64),
						Timestamp: ts,
					}
					ds.append(prompb.TimeSeries{
						Labels:  d.ToTimeSeriesPbLabel("", interval.IntervalName, agg.Name()),
						Samples: []prompb.Sample{sample},
					})
				}
			}
		}
	} else {
		// 如果开启了metric复用，那么需要根据resolutions和agg的配置，修改查询的指标，从prometheus中获取数据
		// 每次请求都会单独调用agg,请求次数会变多
		// 获取需要重用的 resolution; 比如当前是 20m 的聚合，这里就需要重用上一个 5m 的聚合
		resueRset := ds.resolutions[idx-1].IntervalName
		for _, agg := range ds.Aggs {
			select {
			case <-ds.quit:
				return
			default:
			}

			var (
				hasNameLabel, nameTypeISRe bool
				nameType, nameValue        string

				downsampleStr    = fmt.Sprintf(":downsample_%s_%s", resueRset, agg.Name())
				downsampleRegStr = ".*" + downsampleStr

				matchers      []pb.Matcher
				expandMatcher pb.Matcher // 用于替换__name__的matcher 或者 生成__name__的matcher
			)

			// 判断当前的matchers中是否有__name__ 标签
			for _, m := range ds.matchers {
				if m.Name == pb.MetricLabelName {
					hasNameLabel = true
					nameTypeISRe = m.Type == pb.LabelMatcher_RE
					nameType = m.Type
					nameValue = m.Value
				} else {
					// 除了 __name__ 标签，其余的标签都要提前append到matchers中
					// 因为后续会对__name__ matcher进行特殊处理 或 单独生成 __name__ 的matcher, 赋值给 expandMatcher
					matchers = append(matchers, m)
				}
			}

			if !hasNameLabel {
				// case 1. 如果当前所有的matchers都 不包含 __name__, 那么需要加一个 __name__ 的匹配条件
				// {app="game"} -> {app="game",__name__=~".+:downsample_xx_xx"}
				expandMatcher = pb.Matcher{
					Name:  pb.MetricLabelName,
					Value: downsampleRegStr,
					Type:  pb.LabelMatcher_RE,
				}
			} else {
				// case 2. 当前的 matcher 存在 __name__ 的matcher, 下面要对 __name__ 的 value 做适配处理
				if nameTypeISRe {
					// 为正则模式增加 .+ 适配
					downsampleStr = downsampleRegStr
				}

				// case 1.替换原 __name__ 为降采样的 __name__
				if splits := strings.Split(nameValue, "|"); len(splits) > 0 && nameTypeISRe {
					// {__name__=~"abc|def"}->{__name__=~"abc:downsample_xxx_xxx|def:downsample_xxx_xxx"}
					// 说明当前的ds.Value是多个值，需要分别替换
					for i := range splits {
						splits[i] += downsampleStr
					}
					nameValue = strings.Join(splits, "|")
				} else {
					// {__name__="abc"}->{__name__="abc:downsample_5m_xxx"}
					// 说明当前的ds.Value不是多个值，而是单个值
					nameValue = nameValue + downsampleStr
				}

				// 对 __name__ 进行特殊处理的matcher
				expandMatcher = pb.Matcher{
					Name:  pb.MetricLabelName,
					Value: nameValue,
					Type:  nameType,
				}
			}

			// append 将上述处理的 expandMatcher 添加到 matchers 中
			matchers = append(matchers, expandMatcher)
			data, sampleCnt, err := ds.prometheus.RemoteRead(
				span,
				time.Duration(interval.IntervalValue),
				matchers...,
			)
			if err != nil {
				logrus.WithError(err).Error("remote read error")
				continue
			}

			if sampleCnt == 0 {
				continue
			}

			// 指标打点
			ds.appendDot(float64(sampleCnt), now, interval.IntervalName, span, matchers...)
			// 为每一个series都采用当前的agg进行聚合
			for _, d := range data {
				if agg.Name() == "lttb" {
					ds.append(prompb.TimeSeries{
						Labels:  d.ToTimeSeriesPbLabel(ds.resolutions[idx-1].IntervalName, interval.IntervalName, agg.Name()),
						Samples: agg.Aggregate(d.Points).([]prompb.Sample),
					})
				} else {
					sample := prompb.Sample{
						Value:     agg.Aggregate(d.Points).(float64),
						Timestamp: calculateTime(d),
					}
					ds.append(prompb.TimeSeries{
						Labels:  d.ToTimeSeriesPbLabel(ds.resolutions[idx-1].IntervalName, interval.IntervalName, agg.Name()),
						Samples: []prompb.Sample{sample},
					})
				}
			}
		}
	}

	// 3. 将聚合后的数据 remote write 写入prometheus
	ds.submit()
}

func calculateTime(series pb.TimeSeries) int64 {
	points := series.Points
	middle := len(points) / 2

	if len(points)%2 == 0 {
		return (points[middle-1].Timestamp + points[middle].Timestamp) / 2
	}
	return points[middle].Timestamp
}

func (ds *DownSample) appendDot(
	sampleCnt float64,
	timestamp int64,
	intervalName string,
	span *pb.DurationSpan,
	matchers ...pb.Matcher,
) {
	ms := make([]string, 0, len(matchers))
	for _, matcher := range matchers {
		ms = append(ms, matcher.Name+matcher.Type+matcher.Value)
	}

	m := strings.Join(ms, ",")

	// 指标打点
	ds.append(prompb.TimeSeries{
		Labels: []prompb.Label{
			{Name: pb.MetricLabelName, Value: "psd_remote_read_matcher_samples_count"},
			{Name: "remote_type", Value: ds.prometheus.RemoteReadType()},
			{Name: "matcher", Value: m},
		},
		Samples: []prompb.Sample{{
			Value:     sampleCnt,
			Timestamp: timestamp,
		}},
	})
	ds.append(prompb.TimeSeries{
		Labels: []prompb.Label{
			{Name: pb.MetricLabelName, Value: "psd_remote_read_query_time_seconds"},
			{Name: "remote_type", Value: ds.prometheus.RemoteReadType()},
			{Name: "matcher", Value: m},
			{Name: "query_range", Value: intervalName},
		},
		Samples: []prompb.Sample{{
			Value:     float64(span.QueryDuration),
			Timestamp: timestamp,
		}},
	})
}
