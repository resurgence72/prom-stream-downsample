package proxy

import (
	"fmt"
	"time"

	"prom-stream-downsample/pkg/pb"

	reg "github.com/dlclark/regexp2"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/sirupsen/logrus"
)

func (p *Proxy) rangeQueryReplace(
	query string,
	start float64,
	end float64,
) *replaceResult {
	startTime, endTime := time.Unix(int64(start), 0), time.Unix(int64(end), 0)

	var replaced bool
	// 1. 判断当前的 start和end 跨度是否满足某个resolution的时间范围
	rset, resolutionFind := p.checkResolution(endTime.Sub(startTime), rangeQ)
	if !resolutionFind {
		return p.newDefaultReplaceResult(query)
	}

	expr, err := parser.ParseExpr(query)
	if err != nil {
		return p.newDefaultReplaceResult(query)
	}

	parser.Inspect(expr, func(node parser.Node, path []parser.Node) error {
		switch n := node.(type) {
		case *parser.MatrixSelector:
			vector := n.VectorSelector.(*parser.VectorSelector)
			_, metricFind := p.checkMetricName(vector)
			if !metricFind {
				return nil
			}

			replaced = true
			/*
				在 MatrixSelector 这一步不要替换metric
				ast 解析由外到内,即 MatrixSelector->VectorSelector
				如果在 MatrixSelector 阶段就进行替换，会导致后续的 VectorSelector 重复替换，导致指标出错
				会出现类似: net_ecs_cpuutilization:downsample_5m_avg:downsample_5m_avg 指标
			*/
			//p.injectReplacedMetric(vector, mp.Metric, mp.Agg, rset.StringInterval)

			// 替换 [range vector]
			// 判断当前range是否 > resolution * 4, 如果是，则不需要替换
			// 否则，要替换为 resolution * 4 的range vector
			if n.Range < time.Duration(rset.SampleInterval)*pb.ExtrapolatedMultiple {
				n.Range = time.Duration(rset.SampleInterval) * pb.ExtrapolatedMultiple
			}
		case *parser.VectorSelector:
			//  判断query中是否需要替换metric
			mp, metricFind := p.checkMetricName(n)
			if !metricFind {
				return nil
			}

			replaced = true
			// 替换metric
			p.injectReplacedMetric(n, mp.Metric, mp.Agg, rset.StringInterval)
		case *parser.SubqueryExpr:
		case *parser.Call:
		default:
		}
		return nil
	})

	rr := p.newDefaultReplaceResult(expr.String())
	if replaced {
		rr.needChangeLookBackDelta = true
		rr.lookBackDelta = time.Duration(rset.SampleInterval)

		// 打点
		p.proxyDownsampleTotalCounter.WithLabelValues(rangeQ).Inc()
		logrus.Warnln("before range query replace:", query)
		logrus.Warnln("after range query replace:", expr.String())
	}
	return rr
}

func (p *Proxy) instantQueryReplace(query string) string {
	// query := `up[1m] + uuuuuupuup[2m] + up{}[5m] + up[10m]`
	var replaced bool

	expr, err := parser.ParseExpr(query)
	if err != nil {
		return query
	}

	parser.Inspect(expr, func(node parser.Node, path []parser.Node) error {
		// 每个 node 都是一个子的promQL表达式，需要对每个node做相同判断逻辑
		switch n := node.(type) {
		case *parser.MatrixSelector:
			vector := n.VectorSelector.(*parser.VectorSelector)
			// 例如: up[1m] 获取 sum(rate(up[1m])) 中的 up[1m]，需要对up进行替换
			// 1. 首先要判断当前的 __name__ 是否需要替换
			mp, metricFind := p.checkMetricName(vector)

			// 2. 判断当前range vector 的窗口是否符合替换规则
			rset, resolutionFind := p.checkResolution(n.Range, instantQ)

			if !metricFind || !resolutionFind {
				return nil
			}

			replaced = true
			// 3. 进行指标替换
			p.injectReplacedMetric(vector, mp.Metric, mp.Agg, rset.StringInterval)
		case *parser.SubqueryExpr:
		case *parser.Call:
		default:
		}
		return nil
	})

	if replaced {
		p.proxyDownsampleTotalCounter.WithLabelValues(instantQ).Inc()
		logrus.Warnln("before instant query replace:", query)
		logrus.Warnln("after instant query replace:", expr.String())
	}
	return expr.String()
}

func (p *Proxy) checkMetricName(vector *parser.VectorSelector) (pb.MetricProxy, bool) {
	// 例如: up[1m] 获取 sum(rate(up[1m])) 中的 up[1m]，需要对up进行替换
	// 判断当前的 __name__ 是否需要替换
	for _, matcher := range vector.LabelMatchers {
		if matcher.Name == pb.MetricLabelName {
			return p.mps.Contains(matcher.Value)
		}
	}
	return pb.MetricProxy{}, false
}

func (p *Proxy) checkResolution(rge time.Duration, tp string) (*pb.ResolutionSet, bool) {
	for _, resolution := range p.resolutions {
		compare := time.Duration(resolution.TimeRange)
		if tp == instantQ {
			compare = time.Duration(resolution.SampleInterval)
		}

		if rge > compare {
			return &resolution, true
		}
	}
	return nil, false
}

func (p *Proxy) injectReplacedMetric(
	vector *parser.VectorSelector,
	originalMetric string,
	agg string,
	interval string,
) {
	downsampleMetric := fmt.Sprintf(
		pb.DownSampleMetricExtendFormat,
		originalMetric,
		interval,
		agg,
	)

	defer func() {
		// 这一步很重要,主要用来消除原始存在的metric, 统一转换为 {__name__="xxx"} 格式
		vector.Name = pb.EmptyMetricLabelName
	}()

	for _, matcher := range vector.LabelMatchers {
		if matcher.Name != pb.MetricLabelName {
			continue
		}

		/*
			early return 逻辑针对
			abc{} 这种格式的metric case.
			如果存在 xxx{} 或 xxx 这种格式的ql, 同时原指标已经和替换后的指标一致, 则不需要替换
		*/
		if len(vector.Name) > 0 && matcher.Value == downsampleMetric {
			return
		}

		/*
			下面逻辑针对
			{__name__="xxx"}, {__name__=~"xxx|yyy"}
			这种格式的metric case.
			为了保证不重复替换指标，这里只替换 不包含 :downsample_xxx_xxx 的指标
			TODO: 当使用proxy并请求downsample metric时，会因为检测到原Metric而导致重复替换 bugfixing
		*/
		suffix := fmt.Sprintf(`:downsample_%s_%s`, interval, agg)
		re := reg.MustCompile(
			fmt.Sprintf(
				`\b(%s)(?:%s)?\b`,
				originalMetric,
				suffix,
			),
			reg.None,
		)

		if res, err := re.Replace(matcher.Value, downsampleMetric, -1, -1); err == nil {
			matcher.Value = res
		}
		return
	}
}

func (p *Proxy) newDefaultReplaceResult(query string) *replaceResult {
	return &replaceResult{
		finalQuery:              query,
		lookBackDelta:           p.prometheusInfo.QueryLookBackDelta,
		defaultLookBackDelta:    p.prometheusInfo.QueryLookBackDelta,
		needChangeLookBackDelta: false,
	}
}

func (p *Proxy) queryReplace(
	query string,
	start float64,
	end float64,
	queryType string,
) *replaceResult {
	if len(p.mps) == 0 {
		return p.newDefaultReplaceResult(query)
	}

	switch queryType {
	case rangeQueryPath:
		// 只针对 range_query 的case下，才返回 replaceResult 对象
		return p.rangeQueryReplace(query, start, end)
	case instantQueryPath:
		// instant_query 不需要修改lookbackDelta
		// 只需要根据 query 构建 replaceResult 对象
		q := p.instantQueryReplace(query)
		return p.newDefaultReplaceResult(q)
	default:
		return p.newDefaultReplaceResult(query)
	}
}

type replaceResult struct {
	finalQuery string

	lookBackDelta        time.Duration
	defaultLookBackDelta time.Duration

	needChangeLookBackDelta bool
}

func (r *replaceResult) autoExpandLookBackDelta() string {
	// 如果不需要修改lookbackDelta，那么就将lookbackDelta设置为默认值
	if !r.needChangeLookBackDelta {
		return r.defaultLookBackDelta.String()
	}

	// 如果需要修改lookbackDelta，那么就将lookbackDelta设置为resolution的2倍，保证能回溯到上一个resolution的数据，避免数据丢失造成面板断点问题
	return (r.lookBackDelta * 2).String()
}
