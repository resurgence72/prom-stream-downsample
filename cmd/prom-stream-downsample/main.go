package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"prom-stream-downsample/pkg/config"
	"prom-stream-downsample/pkg/downsample"
	"prom-stream-downsample/pkg/pb"
	"prom-stream-downsample/pkg/prometheus"
	"prom-stream-downsample/pkg/proxy"
	"prom-stream-downsample/pkg/version"

	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/prometheus/prompb"
	"github.com/sirupsen/logrus"
)

var (
	confFile string
	h        bool
	v        bool
)

func initArgs() {
	flag.StringVar(&confFile, "config", "./prom-stream-downsample.yaml", "config path")
	flag.BoolVar(&v, "v", false, "版本信息")
	flag.BoolVar(&h, "h", false, "帮助信息")
	flag.Parse()

	if v {
		logrus.WithField("version", version.Version).Println("version")
		os.Exit(0)
	}

	if h {
		flag.Usage()
		os.Exit(0)
	}
}

func init() {
	initLog()
	initArgs()
}

func initLog() {
	// 设置日志记录级别
	logrus.SetLevel(logrus.DebugLevel)
	// 输出日志中添加文件名和方法信息
	logrus.SetReportCaller(true)
	// 设置日志格式
	logrus.SetFormatter(&logrus.TextFormatter{TimestampFormat: "2006-01-02 15:04:05"})
}

type reloader struct {
	name     string
	reloader func() error
}

func main() {
	if err := config.InitConfig(confFile); err != nil {
		panic(err)
	}
	ctx, cancel := context.WithCancel(context.Background())

	reloadCh := make(chan chan error)
	reloaders := []reloader{
		{
			name:     "config",
			reloader: config.Reload,
		},
	}

	global := config.Get().GlobalConfig

	if global.EnabledDownSample {
		writeCh := make(chan []prompb.TimeSeries, 1024)
		p8s, err := prometheus.NewPrometheus(
			global.Prometheus.RemoteReadUrl,
			global.Prometheus.RemoteWriteUrl,
			global.EnabledStream,
			writeCh,
		)
		if err != nil {
			cancel()
			logrus.WithField("error", err).Fatalln("init prometheus failed")
		}

		go p8s.StartRemoteWrite(ctx)

		ds := downsample.NewDownSampleMgr(
			ctx,
			writeCh,
			p8s,
			func() pb.Intervals {
				var res pb.Intervals
				for _, r := range config.Get().GlobalConfig.Resolutions.Rs {
					res = append(res, pb.Interval{
						IntervalName:  r.StringInterval,
						IntervalValue: r.SampleInterval,
					})
				}
				sort.Sort(res)
				return res
			}())
		ds.Start()

		defer func() {
			ds.Stop()
			close(writeCh)
		}()
	}

	if global.EnabledProxy {
		gin.SetMode(gin.ReleaseMode)
		r := gin.Default()
		pprof.Register(r)
		// 开启http代理，端口号为 listen_addr
		pxyCfg := config.Get().ProxyConfig
		pxy, err := proxy.NewProxy(
			r,
			global.Resolutions.Rs,
			pxyCfg.PrometheusAddr,
			func() pb.MetricProxySet {
				mps := make(pb.MetricProxySet, len(pxyCfg.ProxyMetrics))
				for _, pm := range config.Get().ProxyConfig.ProxyMetrics {
					mps[pm.MetricName] = pb.MetricProxy{
						Metric: pm.MetricName,
						Agg:    pm.Aggregation,
					}
				}
				return mps
			},
			reloadCh,
		)
		if err != nil {
			cancel()
			logrus.WithField("error", err).Fatalln("init proxy failed")
		}
		pxy.StartProxy()

		reloaders = append(reloaders, reloader{
			name:     "proxy",
			reloader: pxy.Reload,
		})

		server := &http.Server{
			Addr:           pxyCfg.ListenAddr,
			Handler:        r,
			ReadTimeout:    30 * time.Second,
			WriteTimeout:   30 * time.Second,
			MaxHeaderBytes: 1 << 20,
		}
		go server.ListenAndServe()
		defer server.Shutdown(ctx)
	}

	// start reload watch
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case errCh := <-reloadCh:
				errCh <- reloadConfig(reloaders)
			}
		}
	}()

	term := make(chan os.Signal, 1)
	signal.Notify(term, os.Interrupt, syscall.SIGTERM)
	<-term

	cancel()
	logrus.Warnln("quit...")
}

func reloadConfig(reloaders []reloader) error {
	logrus.Warnln("reloaders reload start")

	var err error
	for _, rld := range reloaders {
		if err = rld.reloader(); err != nil {
			logrus.WithFields(logrus.Fields{
				"status":   "failed",
				"reloader": rld.name,
				"error":    err,
			}).Errorln("reloader failed")
		}
	}

	logrus.Warnln("reloaders reload done")
	return err
}
