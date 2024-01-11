package pb

import (
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

const (
	DownSampleMetricExtendFormat = "%s:downsample_%s_%s" // up:downsample_5m_sum
	ExtrapolatedMultiple         = 4

	MetricLabelName      = "__name__"
	EmptyMetricLabelName = ""

	LabelMatcher_EQ  = "="
	LabelMatcher_NEQ = "!="
	LabelMatcher_RE  = "=~"
	LabelMatcher_NRE = "!~"
)

var (
	RemoteReadType = map[bool]string{
		false: "sample",
		true:  "stream",
	}

	TimeSeriesPool = sync.Pool{New: func() any { return make([]prompb.TimeSeries, 0, 5120) }}
)

type Resolutions struct {
	Rs []ResolutionSet
}

type ResolutionSet struct {
	SampleInterval model.Duration
	StringInterval string

	TimeRange model.Duration
}

type Interval struct {
	IntervalName  string
	IntervalValue model.Duration
}

// 为 Intervals 类型实现 sort 接口
func (is Intervals) Len() int {
	return len(is)
}
func (is Intervals) Less(i, j int) bool {
	return is[i].IntervalValue < is[j].IntervalValue
}
func (is Intervals) Swap(i, j int) {
	is[i], is[j] = is[j], is[i]
}

type Intervals []Interval

type MetricProxy struct {
	Metric   string
	MetricRe *regexp.Regexp
	Agg      string
}

type MetricProxySet map[*regexp.Regexp]MetricProxy

func (m MetricProxySet) Contains(s string) (MetricProxy, string, bool) {
	// 先基于map进行查找,因为大部分场景下 __name__ 都是一个
	// 少部分场景下会使用 __name__=~"abc|bcd" 等正则

	for reg, mp := range m {
		if reg.MatchString(s) {
			return mp, s, true
		}
	}

	return MetricProxy{}, s, false
}

type DurationSpan struct {
	QueryDuration float64
}

func (r *Resolutions) UnmarshalYAML(unmarshal func(any) error) error {
	var resolutions []string
	if err := unmarshal(&resolutions); err != nil {
		return err
	}

	result := Resolutions{Rs: make([]ResolutionSet, 0, len(resolutions))}
	for _, resolution := range resolutions {
		splits := strings.Split(resolution, ",")
		if len(splits) != 2 {
			return fmt.Errorf("invalid resolution format,must like 1m,1h")
		}

		si, err := model.ParseDuration(splits[0])
		if err != nil {
			return err
		}
		tr, err := model.ParseDuration(splits[1])
		if err != nil {
			return err
		}

		result.Rs = append(result.Rs, ResolutionSet{
			SampleInterval: si,
			TimeRange:      tr,
			StringInterval: splits[0],
		})
	}

	*r = result
	return nil
}

type Label struct {
	Name  string
	Value string
}

type Point struct {
	Timestamp int64
	Value     float64
}

type Matcher struct {
	Name  string
	Type  string
	Value string
}

type TimeSeries struct {
	Labels []Label
	Points []Point
}

func (t TimeSeries) ToTimeSeriesPbLabel(
	preInterval string,
	curInterval string,
	aggName string,
) []prompb.Label {
	labels := make([]prompb.Label, 0, len(t.Labels))
	for _, label := range t.Labels {
		if label.Name == MetricLabelName {
			var metricName string
			if len(preInterval) == 0 {
				// 如果 preInterval 为空，那么说明是原始数据，直接修改metricName为downsample指标即可
				metricName = fmt.Sprintf(
					DownSampleMetricExtendFormat,
					label.Value,
					curInterval,
					aggName,
				)
			} else {
				// 说明当前是指标重用，即只需要替换 interval 为下一级即可
				// 比如 abc:downsample_5m_sum -> abc:downsample_20m_sum
				format := "_%s_" + aggName
				metricName = strings.ReplaceAll(
					label.Value,
					fmt.Sprintf(format, preInterval),
					fmt.Sprintf(format, curInterval),
				)
			}

			labels = append(labels, prompb.Label{
				Name:  MetricLabelName,
				Value: metricName,
			})
		} else {
			labels = append(labels, prompb.Label{
				Name:  label.Name,
				Value: label.Value,
			})
		}
	}
	return labels
}
