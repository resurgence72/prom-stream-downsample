package agg

import (
	"math"
	"math/rand"
	"sort"
	"time"

	"prom-stream-downsample/pkg/pb"
)

// aggFnMap 需要支持如下聚合函数 min/max/avg/sum/count/first/last/median/stdev/sumsq/p50/p90/p95/p99
var aggFnMap = map[string]AggFn{
	"sum":    sum,
	"avg":    avg,
	"count":  count,
	"min":    min,
	"max":    max,
	"median": median,
	"stddev": stddev,
	"sumsq":  sumsq,
	"first":  first,
	"last":   last,
	"random": random,
	"mode":   mode,
	"rate":   rate,
	"p50":    p50,
	"p90":    p90,
	"p95":    p95,
	"p99":    p99,
	"p999":   p999,
}

func rate(points []pb.Point) float64 {
	if len(points) == 0 {
		return 0
	}

	// 计算当前 points 的速率
	// 不需要对 timestamp 排序，因为是按照时间顺序来的
	// 1. 计算速率
	return (points[len(points)-1].Value - points[0].Value) / float64(points[len(points)-1].Timestamp-points[0].Timestamp)
}

func mode(points []pb.Point) float64 {
	if len(points) == 0 {
		return 0
	}

	// 计算众数
	var (
		m     = make(map[float64]int)
		maxN  int
		modeN float64
	)
	for _, point := range points {
		m[point.Value]++
		if m[point.Value] > maxN {
			maxN = m[point.Value]
			modeN = point.Value
		}
	}
	return modeN
}

func random(points []pb.Point) float64 {
	if len(points) == 0 {
		return 0
	}
	rand.Seed(time.Now().UnixNano())
	// 随机返回一个点
	return points[rand.Intn(len(points))].Value
}

func p999(points []pb.Point) float64 {
	return quantile(points, .999)
}

func p99(points []pb.Point) float64 {
	return quantile(points, .99)
}

func p95(points []pb.Point) float64 {
	return quantile(points, .95)
}

func quantile(points []pb.Point, q float64) float64 {
	if len(points) == 0 {
		return 0
	}

	// 计算当前 points 的 q 分位数
	// 1. 先排序
	sort.Slice(points, func(i, j int) bool {
		return points[i].Value < points[j].Value
	})

	// 2. 找到 q% 的位置
	return points[int(float64(len(points))*q)].Value
}

func p90(points []pb.Point) float64 {
	return quantile(points, .9)
}

func p50(points []pb.Point) float64 {
	return quantile(points, .5)
}

func sumsq(points []pb.Point) float64 {
	if len(points) == 0 {
		return 0
	}

	// 计算平方和
	var res float64
	for _, point := range points {
		res += math.Pow(point.Value, 2)
	}
	return res
}

func stddev(points []pb.Point) float64 {
	if len(points) == 0 {
		return 0
	}

	// 计算标准差
	var s float64
	for _, point := range points {
		s += point.Value
	}
	mean := s / float64(len(points))

	var n float64
	for _, point := range points {
		n += math.Pow(point.Value-mean, 2)
	}
	return math.Sqrt(n / float64(len(points)))
}

func last(points []pb.Point) float64 {
	if len(points) == 0 {
		return 0
	}
	return points[len(points)-1].Value
}

func first(points []pb.Point) float64 {
	if len(points) == 0 {
		return 0
	}
	return points[0].Value
}

func median(points []pb.Point) float64 {
	if len(points) == 0 {
		return 0
	}

	// 找到 points 的中位数
	// 1. 先排序
	sort.Slice(points, func(i, j int) bool {
		return points[i].Value < points[j].Value
	})

	if len(points)%2 == 1 {
		// 2. 如果是奇数个，直接返回中间的数
		return points[len(points)/2].Value
	} else {
		// 3. 如果是偶数个，返回中间两个数的平均值
		return (points[len(points)/2-1].Value + points[len(points)/2].Value) / 2
	}
}

func max(points []pb.Point) float64 {
	if len(points) == 0 {
		return 0
	}

	// 找到当前 points 中的最大值
	res := math.Inf(-1)
	for _, p := range points {
		if p.Value > res {
			res = p.Value
		}
	}
	return res
}

func min(points []pb.Point) float64 {
	if len(points) == 0 {
		return 0
	}

	// 找到当前 points 中的最小值
	res := math.Inf(1)
	for _, p := range points {
		if p.Value < res {
			res = p.Value
		}
	}
	return res
}

func count(points []pb.Point) float64 {
	if len(points) == 0 {
		return 0
	}
	return float64(len(points))
}

func avg(points []pb.Point) float64 {
	if len(points) == 0 {
		return 0
	}

	return sum(points) / float64(len(points))
}

func sum(points []pb.Point) float64 {
	if len(points) == 0 {
		return 0
	}

	var res float64
	for _, p := range points {
		res += p.Value
	}
	return res
}
