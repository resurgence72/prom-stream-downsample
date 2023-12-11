package config

import (
	"errors"
	"os"
	"path/filepath"
	"sync"

	"prom-stream-downsample/pkg/pb"

	"gopkg.in/yaml.v3"
)

var (
	config *PromStreamDownSampleConfig
	lock   sync.RWMutex
	fp     string
)

func InitConfig(filePath string) error {
	absPath, err := filepath.Abs(filePath)
	if err != nil {
		return err
	}

	fp = absPath
	cfg, err := loadFile(absPath)
	if err != nil {
		return err
	}

	config = cfg
	return nil
}

func Get() *PromStreamDownSampleConfig {
	lock.RLock()
	defer lock.RUnlock()
	return config
}

func Reload() error {
	lock.Lock()
	defer lock.Unlock()

	cfg, err := loadFile(fp)
	if err != nil {
		return err
	}

	config = cfg
	return nil
}

func loadFile(fileName string) (*PromStreamDownSampleConfig, error) {
	bytes, err := os.ReadFile(fileName)
	if err != nil {
		return nil, err
	}

	return load(bytes)
}

func load(bytes []byte) (*PromStreamDownSampleConfig, error) {
	cfg := &PromStreamDownSampleConfig{}
	err := yaml.Unmarshal(bytes, cfg)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

type Metric struct {
	MetricName  string `yaml:"metric_name"`
	Aggregation string `yaml:"aggregation"`
}

type ProxyConfig struct {
	ListenAddr     string   `yaml:"listen_addr"`
	PrometheusAddr string   `yaml:"prometheus_addr"`
	ProxyMetrics   []Metric `yaml:"proxy_metrics"`
}

type PromStreamDownSampleConfig struct {
	GlobalConfig     GlobalConfig       `yaml:"global_config"`
	DownSampleConfig []DownSampleConfig `yaml:"downsample_config"`
	ProxyConfig      ProxyConfig        `yaml:"proxy_config"`
}

type GlobalConfig struct {
	EnabledStream      bool           `yaml:"enabled_stream"`
	EnabledProxy       bool           `yaml:"enabled_proxy"`
	EnabledDownSample  bool           `yaml:"enabled_downsample"`
	EnabledMetricReuse bool           `yaml:"enabled_metric_reuse"`
	Prometheus         Prometheus     `yaml:"prometheus"`
	Resolutions        pb.Resolutions `yaml:"resolutions"`
}

type DownSampleConfig struct {
	JobName      string    `yaml:"job_name"`
	Matchers     []Matcher `yaml:"matchers"`
	Aggregations []string  `yaml:"aggregations"`
}

type Matcher struct {
	MatcherType string `yaml:"matcher_type"`
	LabelName   string `yaml:"label_name"`
	LabelValue  string `yaml:"label_value"`
}

func (m *Matcher) UnmarshalYAML(unmarshal func(any) error) error {
	mc := &Matcher{}
	type plain Matcher

	if err := unmarshal((*plain)(mc)); err != nil {
		return err
	}

	if len(mc.LabelName) == 0 {
		mc.LabelName = pb.MetricLabelName
	}

	if len(mc.MatcherType) == 0 {
		mc.MatcherType = pb.LabelMatcher_EQ
	}

	*m = *mc
	return nil
}

func (d *DownSampleConfig) UnmarshalYAML(unmarshal func(any) error) error {
	dsc := &DownSampleConfig{}
	type plain DownSampleConfig

	if err := unmarshal((*plain)(dsc)); err != nil {
		return err
	}

	if len(dsc.Matchers) == 0 {
		return errors.New("matchers can not be empty")
	}

	// 不支持 matcher_type 全部为 != 或 !~ 的情况 (prometheus 查询会报错)
	// 同样不支持 __name__ 的否定匹配条件
	allFalse := true
	for _, matcher := range dsc.Matchers {
		if matcher.LabelName == pb.MetricLabelName && (matcher.MatcherType == pb.LabelMatcher_NEQ || matcher.MatcherType == pb.LabelMatcher_NRE) {
			return errors.New("can not match __name__ with != or !~")
		}

		if matcher.MatcherType == pb.LabelMatcher_EQ || matcher.MatcherType == pb.LabelMatcher_RE {
			allFalse = false
		}
	}
	if allFalse {
		return errors.New("matchers can not be all false")
	}

	if len(dsc.Aggregations) == 0 {
		dsc.Aggregations = []string{"avg"}
	}

	*d = *dsc
	return nil
}

type Prometheus struct {
	RemoteReadGroup []string `yaml:"remote_read_group"`
	RemoteWriteUrl  string   `yaml:"remote_write_url"`
}
