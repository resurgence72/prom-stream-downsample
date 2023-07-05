package proxy

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/mod/semver"

	"prom-stream-downsample/pkg/pb"
	p8s "prom-stream-downsample/pkg/prometheus"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

const (
	rangeQueryPath   = "/api/v1/query_range"
	instantQueryPath = "/api/v1/query"

	instantQ = "instant"
	rangeQ   = "range"

	apiV1Prefix = "/api/v1"

	//regexExpr = `\b%s\b(?:\{[^}]*\})?\[(.+?)\]`
)

// 将 rangeQueryParams 和 instantQueryParams 统一成 QueryParams
type QueryParams struct {
	Query   string  `form:"query" json:"query"`
	Start   float64 `form:"start" json:"start"`
	End     float64 `form:"end" json:"end"`
	Step    int64   `form:"step" json:"step"`
	Time    float64 `form:"time" json:"time"`
	Timeout string  `form:"timeout" json:"timeout"`
}

type Proxy struct {
	resolutions []pb.ResolutionSet
	proxyPath   string
	flushProxy  func() pb.MetricProxySet
	mps         pb.MetricProxySet

	r      *gin.Engine
	lock   sync.Mutex
	reload chan chan error

	prometheusInfo                 *p8s.PrometheusMetaInfo
	prometheusSupportLookBackDelta bool

	proxyTotalCounter           prometheus.Counter
	proxyDownsampleTotalCounter prometheus.CounterVec
}

func NewProxy(
	r *gin.Engine,
	rs []pb.ResolutionSet,
	proxyPath string,
	fn func() pb.MetricProxySet,
	ch chan chan error,
) (*Proxy, error) {
	sort.Slice(rs, func(i, j int) bool {
		return rs[i].TimeRange > rs[j].TimeRange
	})
	pxy := &Proxy{
		r:           r,
		resolutions: rs,
		proxyPath:   proxyPath,
		flushProxy:  fn,
		mps:         fn(),
		reload:      ch,
		proxyTotalCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "psd_proxy_total",
			Help: "The total number of requests to proxy",
		}),
		proxyDownsampleTotalCounter: *prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "psd_proxy_downsample_total",
			Help: "The total number of requests downsample to proxy",
		}, []string{"query_type"}),
	}
	prometheus.MustRegister(pxy.proxyTotalCounter)
	prometheus.MustRegister(pxy.proxyDownsampleTotalCounter)

	// 获取prometheus的版本信息
	if err := pxy.metaInfo(); err != nil {
		return nil, err
	}
	return pxy, nil
}

func (p *Proxy) metaInfo() error {
	info, err := p8s.NewPrometheusMetaInfo(p.proxyPath)
	if err != nil {
		return err
	}

	// 如果当前版本号大于2.43.0，则开启自动lookbackDelta
	if semver.Compare(semver.Build(info.Version), "2.43.0") >= 0 {
		p.prometheusSupportLookBackDelta = true
	}

	logrus.Warnf("proxy prometheus [%s] version: [%s], LookBackDelta: [%s]\n", p.proxyPath, info.Version, info.QueryLookBackDelta)
	p.prometheusInfo = info
	return nil
}

func (p *Proxy) promHandler(handler http.Handler) gin.HandlerFunc {
	return func(c *gin.Context) {
		handler.ServeHTTP(c.Writer, c.Request)
	}
}

func (p *Proxy) setRequest(
	req *http.Request,
	method string,
	rowData string,
) {
	switch method {
	case http.MethodGet:
		req.URL.RawQuery = rowData
	case http.MethodPost:
		// 将修改后的参数设置为请求体
		req.Body = io.NopCloser(strings.NewReader(rowData))
		req.ContentLength = int64(len(rowData))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
}

func (p *Proxy) injectOtherRouter() {
	p.r.GET("metrics", p.promHandler(promhttp.Handler()))
	p.r.POST("/-/reload", func(c *gin.Context) {
		ch := make(chan error)
		p.reload <- ch
		if err := <-ch; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"status": fmt.Sprintf("reload failed: %v", err)})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "reload success"})
	})
}

func (p *Proxy) StartProxy() {
	proxyUrl, _ := url.Parse(p.proxyPath)

	p.injectOtherRouter()
	apiV1 := p.r.Group(apiV1Prefix)
	apiV1.Any("*name", func(c *gin.Context) {
		p.proxyTotalCounter.Inc()

		// 如果匹配到非 query_range/query path, 则直接转发
		if c.Request.URL.Path != instantQueryPath &&
			c.Request.URL.Path != rangeQueryPath {
			c.Request.URL.Path = apiV1Prefix + c.Param("name")
			httputil.NewSingleHostReverseProxy(proxyUrl).ServeHTTP(c.Writer, c.Request)
			return
		}

		// 解析通用查询参数结构体
		q := &QueryParams{}
		if err := c.ShouldBind(q); err != nil {
			logrus.WithFields(logrus.Fields{
				"path": c.Request.URL.Path,
				"err":  err,
			}).Errorf("bind query params error: %v", err)
			c.AbortWithStatus(http.StatusInternalServerError)
			return
		}

		v := url.Values{}
		// 对instant 和 range query 的共同参数进行解析
		replaceR := p.queryReplace(q.Query, q.Start, q.End, c.Request.URL.Path)
		v.Add("query", replaceR.finalQuery) // 对query进行替换
		v.Add("timeout", q.Timeout)

		if c.Request.URL.Path == instantQueryPath {
			// 解析 instant query 特有参数 time, 表示当前查询的时间戳
			v.Add("time", p.changeTime(q.Time))
		} else if c.Request.URL.Path == rangeQueryPath {
			// 匹配到 query_range, 则将query参数中的原metric 根据 start和end 替换为对应的downsample metric
			// 解析 range query 特有参数 start, end, step
			step := time.Duration(q.Step) * time.Second
			v.Add("start", p.changeTime(q.Start))
			v.Add("end", p.changeTime(q.End))
			v.Add("step", step.String())

			// TODO 继续观察： 当前只在query_range中使用lookbackDelta参数调整功能
			if replaceR.needChangeLookBackDelta && p.prometheusSupportLookBackDelta {
				// 如果 在queryReplace中判定需要调整回溯窗口 且 当前p8s版本支持动态LookBackDelta功能 -> 则设置lookbackDelta参数
				expandLookBackDelta := replaceR.autoExpandLookBackDelta()
				v.Add("lookback_delta", expandLookBackDelta)
				logrus.Warnf(
					"query [%s] auto expand lookback_delta, from [%s] to [%s]\n",
					replaceR.finalQuery,
					replaceR.defaultLookBackDelta,
					expandLookBackDelta,
				)
			}
		}

		// 设置请求体
		p.setRequest(c.Request, c.Request.Method, v.Encode())
		// 转发请求
		httputil.NewSingleHostReverseProxy(proxyUrl).ServeHTTP(c.Writer, c.Request)
	})
}

func (p *Proxy) changeTime(t float64) string {
	return time.Unix(int64(t), 0).UTC().Format("2006-01-02T15:04:05Z")
}

func (p *Proxy) Reload() error {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.mps = p.flushProxy()
	logrus.Warnln("reload proxy success", p.mps)
	return nil
}
